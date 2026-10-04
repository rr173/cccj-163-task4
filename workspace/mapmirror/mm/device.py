"""路侧计算盒设备代理。

本地布局（--device-root <dir>）:
  agent.db             本地状态：tasks/blocks/attempts（重启可辨别一切）
  blobs/<sha>          已确认压缩块缓存（内容寻址，跨清单/跨任务共享去重）
  staging/<task>/      某清单暂存区：解包对象 + 镜像视图 + MANIFEST.json
  images/<manifest>/   已验真镜像（root_sha256 校验通过）
  current -> images/<manifest>   当前运行镜像（原子 symlink）

处理规则:
  * 每个清单的任务在仓库端有确定 task_id；同一清单重试返回第一次任务与切换结果；
  * 拉取逐块校验块散列+对象散列；已确认块缓存复用，断网后续传不重复计数；
  * 所有对象齐了才做整包根散列校验，之后才有资格进入"待切换"槽；
  * 一个盒子同时最多一个待切换镜像：较老代次拒绝（OLDER_GENERATION），
    已暂存较新代次后到的旧代次拒绝（PENDING_SLOT_CONFLICT）；
  * 当前镜像永不被较老代次或坏对象覆盖；
  * 换指针：fsync 后 rename 原子替换 current，任何失败 current 原样保留；
  * 每次 attempt 的结果（含拒绝理由）写本地 attempts 并上报仓库，网页与 CLI 同源可见。
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.parse

from .common import (VerifyError, atomic_symlink, atomic_write_bytes,
                     atomic_write_json, canonical, hardlink_or_copy, root_hash,
                     sha_bytes, sha_file, unpack_block)
from .registry import Registry
from .transport import TransportError

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
  task_id        TEXT PRIMARY KEY,
  manifest_id    TEXT NOT NULL,
  generation     INTEGER NOT NULL,
  status         TEXT NOT NULL,
  reason_code    TEXT DEFAULT '',
  reason         TEXT DEFAULT '',
  total_blocks   INTEGER NOT NULL,
  attempt        INTEGER NOT NULL DEFAULT 0,
  created_at     TEXT NOT NULL,
  updated_at     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS blocks (
  task_id     TEXT NOT NULL,
  block_index INTEGER NOT NULL,
  sha256      TEXT NOT NULL,
  size        INTEGER NOT NULL,
  confirmed   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (task_id, block_index)
);
CREATE TABLE IF NOT EXISTS attempts (
  attempt_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id      TEXT NOT NULL,
  attempt      INTEGER NOT NULL,
  phase        TEXT NOT NULL,
  result       TEXT NOT NULL,
  code         TEXT DEFAULT '',
  message      TEXT DEFAULT '',
  ts           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_task ON attempts(task_id, attempt_id);
"""

# 重启后可辨别的四态（+ 坏对象失败态）
PULLING = "PULLING"
STAGED = "STAGED"
SWITCHED = "SWITCHED"
REJECTED = "REJECTED"
FAILED = "FAILED"
TERMINAL = {SWITCHED, REJECTED, FAILED}


class AgentError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


class DeviceAgent:
    def __init__(self, device_id: str, root: str, registry_url: str,
                 transport=None, clock=time.time):
        self.device_id = device_id
        self.root = root
        self.url = registry_url
        self.tr = transport
        self.clock = clock
        for sub in ("blobs", "staging", "images"):
            os.makedirs(os.path.join(root, sub), exist_ok=True)
        self.db = os.path.join(root, "agent.db")
        self.conn = sqlite3.connect(self.db)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ------------------------------------------------------------ helpers --
    def _ts(self) -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%S+00:00",
                             time.gmtime(self.clock()))

    def _blob_cache(self, digest: str) -> str:
        return os.path.join(self.root, "blobs",
                            digest.removeprefix("sha256:")[:2],
                            digest.removeprefix("sha256:"))

    def _staging(self, task_id: str) -> str:
        return os.path.join(self.root, "staging", task_id)

    def _image(self, manifest_id: str) -> str:
        return os.path.join(self.root, "images", manifest_id)

    @property
    def current_link(self) -> str:
        return os.path.join(self.root, "current")

    def current(self) -> dict | None:
        """当前运行镜像（重启后从 symlink + CURRENT.json 恢复辨认）。"""
        link = self.current_link
        if not os.path.islink(link):
            return None
        info = os.path.join(link, "CURRENT.json")
        if os.path.exists(info):
            with open(info, encoding="utf-8") as f:
                return json.load(f)
        return None

    def _local_task(self, task_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM tasks WHERE task_id=?",
                                 (task_id,)).fetchone()

    def _save_attempt(self, task_id: str, attempt: int, phase: str,
                      result: str, code: str = "", message: str = "") -> None:
        self.conn.execute(
            "INSERT INTO attempts(task_id,attempt,phase,result,code,message,ts)"
            " VALUES(?,?,?,?,?,?,?)",
            (task_id, attempt, phase, result, code, message, self._ts()))
        self.conn.commit()

    def _report(self, task_id: str, event: str, *, attempt=None,
                confirmed_blocks=None, code="", message="",
                details=None) -> dict:
        """上报仓库；网络断时不致命（本地已留痕，下次再报）。"""
        uid = f"{task_id}:{event}:{attempt or 0}:{code}:{confirmed_blocks or 0}"
        body = {"event_uid": uid, "event": event, "ts": self._ts(),
                "attempt": attempt, "confirmed_blocks": confirmed_blocks,
                "code": code, "message": message, "details": details or {}}
        try:
            resp, _status = self.tr.post_json(
                f"/api/tasks/{urllib.parse.quote(task_id)}/events", body)
            return resp
        except TransportError as e:
            self._save_attempt(task_id, attempt or 0, "report", "unsent",
                               e.code, e.message)
            return {"task": {}, "idempotent": True, "unsent": True}

    # ------------------------------------------------------------ fetching --
    def fetch_task(self, manifest_id: str) -> tuple[str, dict, dict]:
        """从仓库取清单并定位本设备的任务；同时返回 (task_id, manifest, server_task)。"""
        manifest = self.tr.get_json(f"/api/manifests/{urllib.parse.quote(manifest_id)}")
        task_id = Registry.task_id(manifest_id, self.device_id)
        server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
        return task_id, manifest, server

    def process(self, manifest_id: str) -> dict:
        """主入口：把一个清单处理到可切换（或终态）。

        重试同一清单：终态直接返回第一次结果（仓库+本地双来源一致），
        绝不重复下载/计数，也不会再次换指针。
        """
        task_id, manifest, server_task = self.fetch_task(manifest_id)
        local = self._local_task(task_id)
        if local and local["status"] in TERMINAL:
            # 重试同一清单返回第一次任务与切换结果
            server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
            return {"task_id": task_id, "manifest_id": manifest_id,
                    "status": local["status"], "idempotent": True,
                    "reason_code": local["reason_code"],
                    "reason": local["reason"], "server": server,
                    "local_only": False}
        if local and local["status"] == STAGED:
            # 已验真待切换：不重复拉取/解包/计数；竞争状态在 activate 前复核
            server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
            return {"task_id": task_id, "manifest_id": manifest_id,
                    "status": STAGED, "idempotent": True,
                    "root_sha256": manifest["root_sha256"],
                    "confirmed_blocks": len(manifest["blocks"]),
                    "total_blocks": len(manifest["blocks"]), "server": server}

        if local is None:
            self._init_local(task_id, manifest, server_task)

        # 竞争仲裁（基于本地已知状态 + 仓库当前/待切换事实）
        cur = self.current()
        if cur and cur.get("manifest_id") == manifest_id:
            # 运行中的就是这份清单（例如本地库重建）：幂等，不重复处理
            server = self.tr.get_json(
                f"/api/tasks/{urllib.parse.quote(task_id)}")
            return {"task_id": task_id, "manifest_id": manifest_id,
                    "status": SWITCHED, "idempotent": True,
                    "generation": cur.get("generation", 0),
                    "root_sha256": cur.get("root_sha256", ""),
                    "server": server}
        verdict = self._admission(task_id, manifest, server_task, local)
        if verdict:
            return verdict

        attempt = (local["attempt"] if local else 0) + 1
        self.conn.execute("UPDATE tasks SET attempt=?, status='PULLING', "
                          "updated_at=? WHERE task_id=?",
                          (attempt, self._ts(), task_id))
        self.conn.commit()
        self._report(task_id, "pull_started", attempt=attempt)

        outcome = self._pull(task_id, manifest, attempt)
        if outcome is not None:
            return outcome  # 断网暂停（可续）或坏对象失败
        return self._stage(task_id, manifest, attempt)

    def _init_local(self, task_id: str, manifest: dict,
                    server_task: dict) -> None:
        ts = self._ts()
        self.conn.execute(
            "INSERT INTO tasks(task_id,manifest_id,generation,status,reason_code,"
            "reason,total_blocks,attempt,created_at,updated_at) "
            "VALUES(?,?,?, 'NEW','','',?,0,?,?)",
            (task_id, manifest["manifest_id"], server_task.get("generation", 0),
             len(manifest["blocks"]), ts, ts))
        for b in manifest["blocks"]:
            self.conn.execute(
                "INSERT INTO blocks(task_id,block_index,sha256,size,confirmed)"
                " VALUES(?,?,?,?,0)",
                (task_id, b["index"], b["sha256"], b["size"]))
        self.conn.commit()

    def _admission(self, task_id: str, manifest: dict, server_task: dict,
                   local: sqlite3.Row | None) -> dict | None:
        """返回拒绝结果 dict 表示拒绝；None 表示放行。"""
        gen = server_task.get("generation", 0)
        # 1) 较老代次不能覆盖当前镜像
        cur = self.current()
        if cur and gen <= cur.get("generation", 0):
            return self._reject(
                task_id, "OLDER_GENERATION", local,
                f"清单代次 {gen} 不新于当前镜像代次 {cur['generation']}"
                f"（当前 {cur.get('manifest_id')}），拒绝覆盖")
        # 2) 一个盒子只允许一个待切换镜像
        pend = self._pending_task(exclude=task_id)
        if pend:
            p_manifest = self.tr.get_json(
                f"/api/manifests/{urllib.parse.quote(pend['manifest_id'])}")
            if pend["generation"] >= gen:
                return self._reject(
                    task_id, "PENDING_SLOT_CONFLICT", local,
                    f"待切换槽已被 {pend['manifest_id']}（代次 {pend['generation']}"
                    f"）占用；本清单代次 {gen} 不更新，拒绝排队")
            # 到的是更新的代次：旧待切换让出槽位（标记拒绝，不能再切换）
            self._reject(
                pend["task_id"], "SUPERSEDED_BY_NEWER_GENERATION",
                self._local_task(pend["task_id"]),
                f"代次 {gen} 的新清单到达，代次 {pend['generation']} 的待切换让位")
        return None

    def _pending_task(self, exclude: str = "") -> sqlite3.Row | None:
        rows = self.conn.execute(
            "SELECT * FROM tasks WHERE status='STAGED' AND task_id!=? "
            "ORDER BY generation DESC", (exclude,)).fetchall()
        return rows[0] if rows else None

    def _reject(self, task_id: str, code: str,
                local: sqlite3.Row | None, message: str) -> dict:
        attempt = (local["attempt"] if local else 0) + 1
        self.conn.execute(
            "UPDATE tasks SET status='REJECTED', reason_code=?, reason=?,"
            " attempt=?, updated_at=? WHERE task_id=?",
            (code, message, attempt, self._ts(), task_id))
        self.conn.commit()
        self._save_attempt(task_id, attempt, "admission", "rejected",
                           code, message)
        self._report(task_id, "rejected", attempt=attempt, code=code,
                     message=message)
        server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
        return {"task_id": task_id,
                "manifest_id": local["manifest_id"] if local else
                server.get("manifest_id", ""),
                "status": "REJECTED", "idempotent": False,
                "reason_code": code, "reason": message, "server": server}

    # ------------------------------------------------------------- pulling --
    def _pull(self, task_id: str, manifest: dict, attempt: int):
        """逐块拉取+验真。返回 None 表示全部确认；否则返回暂停/失败结果。"""
        expect_by_oid = {e["oid"]: e["sha256"] for e in manifest["objects"]}
        n = len(manifest["blocks"])
        interrupted = False
        for b in manifest["blocks"]:
            idx, want = b["index"], b["sha256"]
            row = self.conn.execute(
                "SELECT confirmed FROM blocks WHERE task_id=? AND block_index=?",
                (task_id, idx)).fetchone()
            if row and row["confirmed"]:
                continue  # 已确认块：续传时不重复下载、不重复计数
            cache = self._blob_cache(want)
            if os.path.exists(cache) and sha_file(cache) == want:
                self._confirm_block(task_id, idx, want, attempt, cached=True)
                continue
            try:
                blob = self.tr.get_block(want)
            except TransportError as e:
                # 网络中断：已确认块原样保留，下次 process 续传
                self._save_attempt(task_id, attempt, "pull", "paused",
                                   e.code, e.message)
                self._report(task_id, "pull_paused", attempt=attempt,
                             confirmed_blocks=self._confirmed_count(task_id),
                             code=e.code, message=e.message,
                             details={"block_index": idx})
                interrupted = True
                break
            # 块散列 + 逐对象散列（坏对象到此为止，绝不进入暂存）
            try:
                unpack_block(blob, expect_block_sha=want,
                             expect_objects=expect_by_oid)
            except VerifyError as e:
                return self._fail_bad_object(task_id, attempt, idx, e)
            os.makedirs(os.path.dirname(cache), exist_ok=True)
            atomic_write_bytes(cache, blob)
            if sha_file(cache) != want:
                return self._fail_bad_object(
                    task_id, attempt, idx,
                    VerifyError("BLOCK_HASH_MISMATCH", "落盘块散列复核失败"))
            self._confirm_block(task_id, idx, want, attempt)
        if interrupted:
            return {"task_id": task_id, "manifest_id": manifest["manifest_id"],
                    "status": "PULLING", "paused": True,
                    "confirmed_blocks": self._confirmed_count(task_id),
                    "total_blocks": n,
                    "message": "网络中断，已确认块保留；再次运行从断点续传"}
        if self._confirmed_count(task_id) != n:
            return {"task_id": task_id, "manifest_id": manifest["manifest_id"],
                    "status": "PULLING", "paused": True,
                    "confirmed_blocks": self._confirmed_count(task_id),
                    "total_blocks": n,
                    "message": "仍有未确认块，等待续传"}
        self._save_attempt(task_id, attempt, "pull", "completed",
                           message=f"{n}/{n} 块全部确认")
        return None

    def _confirmed_count(self, task_id: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) c FROM blocks WHERE task_id=? AND confirmed=1",
            (task_id,)).fetchone()["c"]

    def _confirm_block(self, task_id: str, idx: int, want: str,
                       attempt: int, cached: bool = False) -> None:
        # confirmed 只置 1 一次：已确认块不重复计数
        cur = self.conn.execute(
            "UPDATE blocks SET confirmed=1 WHERE task_id=? AND block_index=? "
            "AND confirmed=0", (task_id, idx))
        self.conn.commit()
        count = self._confirmed_count(task_id)
        total = self.conn.execute(
            "SELECT COUNT(*) c FROM blocks WHERE task_id=?",
            (task_id,)).fetchone()["c"]
        self._report(task_id, "pull_progress", attempt=attempt,
                     confirmed_blocks=count)
        self._save_attempt(task_id, attempt, "pull",
                           "block_cached" if cached else "block_confirmed",
                           message=f"块 {idx} 确认（{count}/{total}）")

    def _fail_bad_object(self, task_id: str, attempt: int, idx: int,
                         e: VerifyError) -> dict:
        self.conn.execute(
            "UPDATE tasks SET status='FAILED', reason_code=?, reason=?,"
            " updated_at=? WHERE task_id=?",
            (e.code, f"块 {idx} 验真失败：{e.message}", self._ts(), task_id))
        self.conn.commit()
        self._save_attempt(task_id, attempt, "pull", "failed", e.code, e.message)
        self._report(task_id, "failed", attempt=attempt, code=e.code,
                     message=f"块 {idx}：{e.message}",
                     details={"block_index": idx})
        server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
        return {"task_id": task_id,
                "manifest_id": server.get("manifest_id", ""),
                "status": "FAILED", "reason_code": e.code,
                "reason": f"块 {idx}：{e.message}", "server": server}

    # -------------------------------------------------------------- staging --
    def _stage(self, task_id: str, manifest: dict, attempt: int) -> dict:
        """解包全部块到暂存区 → 对象散列复核 → 整包根散列 → 建镜像目录。"""
        staging = self._staging(task_id)
        tmp_stage = staging + ".tmp"
        if os.path.exists(tmp_stage):
            import shutil
            shutil.rmtree(tmp_stage)
        os.makedirs(os.path.join(tmp_stage, "objects"), exist_ok=True)

        found: list[dict] = []
        for b in manifest["blocks"]:
            with open(self._blob_cache(b["sha256"]), "rb") as bf:
                blob = bf.read()
            items = unpack_block(blob, expect_block_sha=b["sha256"])
            for oid, obj, sha in items:
                # 对象散列必须与清单一致
                want = next(e for e in manifest["objects"] if e["oid"] == oid)
                if sha != want["sha256"]:
                    import shutil
                    shutil.rmtree(tmp_stage, ignore_errors=True)
                    return self._fail_bad_object(
                        task_id, attempt, b["index"],
                        VerifyError("OBJECT_HASH_MISMATCH",
                                    f"对象 {oid} 散列与清单不符"))
                safe = oid.replace("/", "__")
                # 以规范化字节落盘，使文件散列 == 清单对象散列
                atomic_write_bytes(
                    os.path.join(tmp_stage, "objects", safe), canonical(obj))
                found.append({"oid": oid, "sha256": sha,
                              "size": want["size"], "type": want["type"]})

        # 整包根散列（设备独立重算，不信任仓库给的任何中间值）
        got_root = root_hash(found)
        if got_root != manifest["root_sha256"]:
            import shutil
            shutil.rmtree(tmp_stage, ignore_errors=True)
            return self._fail_bad_object(
                task_id, attempt, -1,
                VerifyError("ROOT_HASH_MISMATCH",
                            f"整包根散列不符：清单 {manifest['root_sha256']}，"
                            f"重算 {got_root}"))

        atomic_write_json(os.path.join(tmp_stage, "MANIFEST.json"), manifest)
        # 暂存目录原子落位（失败/中断不留下半切换状态）
        if os.path.exists(staging):
            import shutil
            shutil.rmtree(staging)
        os.replace(tmp_stage, staging)

        # 建不可变镜像目录（硬链接对象去重）
        image_dir = self._image(manifest["manifest_id"])
        if not os.path.isdir(image_dir):
            img_tmp = image_dir + ".tmp"
            import shutil
            shutil.rmtree(img_tmp, ignore_errors=True)
            os.makedirs(os.path.join(img_tmp, "objects"))
            for name in os.listdir(os.path.join(staging, "objects")):
                hardlink_or_copy(os.path.join(staging, "objects", name),
                                 os.path.join(img_tmp, "objects", name))
            info = {"manifest_id": manifest["manifest_id"],
                    "root_sha256": manifest["root_sha256"],
                    "center_anchor": manifest["center_anchor"],
                    "snapshot_id": manifest["snapshot_id"]}
            atomic_write_json(os.path.join(img_tmp, "IMAGE.json"), info)
            os.replace(img_tmp, image_dir)

        self.conn.execute(
            "UPDATE tasks SET status='STAGED', updated_at=? WHERE task_id=?",
            (self._ts(), task_id))
        self.conn.commit()
        self._save_attempt(task_id, attempt, "stage", "completed",
                           message="根散列校验通过，镜像待切换")
        self._report(task_id, "stage_completed", attempt=attempt,
                     confirmed_blocks=len(manifest["blocks"]),
                     details={"root_sha256": manifest["root_sha256"]})

        # 占用待切换槽前再次检查竞争（拉取期间可能有更新代次已暂存）
        local = self._local_task(task_id)
        server_task = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
        verdict = self._admission(task_id, manifest, server_task, local)
        if verdict:
            return verdict
        server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
        return {"task_id": task_id, "manifest_id": manifest["manifest_id"],
                "status": "STAGED", "root_sha256": got_root,
                "confirmed_blocks": len(manifest["blocks"]),
                "total_blocks": len(manifest["blocks"]), "server": server}

    # ------------------------------------------------------------- activate --
    def activate(self, manifest_id: str) -> dict:
        """只有在镜像已全部验真（STAGED）后才执行原子换指针。"""
        task_id, manifest, _server = self.fetch_task(manifest_id)
        local = self._local_task(task_id)
        if local is None:
            raise AgentError("TASK_NOT_PROCESSED",
                            "请先 pull 处理该清单，再执行 activate")
        if local["status"] == SWITCHED:
            # 重试：返回第一次切换结果，不再换指针
            return self._idempotent_switch(task_id, manifest_id)
        if local["status"] == REJECTED:
            return {"task_id": task_id, "manifest_id": manifest_id,
                    "status": REJECTED, "idempotent": True,
                    "reason_code": local["reason_code"],
                    "reason": local["reason"]}
        if local["status"] == FAILED:
            return {"task_id": task_id, "manifest_id": manifest_id,
                    "status": FAILED, "idempotent": True,
                    "reason_code": local["reason_code"],
                    "reason": local["reason"]}
        if local["status"] != STAGED:
            raise AgentError("NOT_STAGED",
                            f"任务状态 {local['status']}，未完成全部验真，禁止切换")

        # 切换前最后闸门：
        #  1) 镜像目录与根散列仍在（防磁盘损坏/被替换）
        #  2) 较老代次不得覆盖当前
        #  3) 待切换槽仍属于自己（未被更新代次顶替）
        image_dir = self._image(manifest_id)
        self._verify_image(image_dir, manifest)
        cur = self.current()
        gen = local["generation"]
        if cur and gen <= cur.get("generation", 0):
            return self._reject_local(task_id, "OLDER_GENERATION",
                                      f"切换前复核：代次 {gen} 不新于当前 "
                                      f"{cur['generation']}")
        pend = self._pending_task(exclude=task_id)
        if pend and pend["generation"] > gen:
            return self._reject_local(task_id, "PENDING_SLOT_CONFLICT",
                                      f"切换前复核：更新代次 {pend['generation']} "
                                      f"已占用待切换槽")

        attempt = local["attempt"]
        info = {"manifest_id": manifest_id,
                "generation": gen,
                "root_sha256": manifest["root_sha256"],
                "center_anchor": manifest["center_anchor"],
                "snapshot_id": manifest["snapshot_id"],
                "switched_at": self._ts()}
        atomic_write_json(os.path.join(image_dir, "CURRENT.json"), info)
        target = os.path.relpath(image_dir, self.root)
        try:
            atomic_symlink(target, self.current_link)  # 原子换指针
        except OSError as e:
            # 切换失败：current 原样保留
            self._save_attempt(task_id, attempt, "switch", "failed",
                               "SWITCH_IO_ERROR", str(e))
            raise AgentError("SWITCH_IO_ERROR", f"原子换指针失败：{e}")

        self.conn.execute(
            "UPDATE tasks SET status='SWITCHED', reason_code='', reason='',"
            " updated_at=? WHERE task_id=?", (self._ts(), task_id))
        self.conn.commit()
        self._save_attempt(task_id, attempt, "switch", "switched",
                           message=f"{cur['manifest_id'] if cur else '∅'} "
                                   f"-> {manifest_id}")
        # 槽内其他 STAGED（同代或更老）不再可切换
        for other in self.conn.execute(
                "SELECT task_id FROM tasks WHERE status='STAGED' "
                "AND task_id!=?", (task_id,)).fetchall():
            self._reject(other["task_id"], "SUPERSEDED_BY_SWITCH",
                         self._local_task(other["task_id"]),
                         f"代次 {gen} 已切换，其余待切换镜像关闭")
        self._report(task_id, "switch_succeeded", attempt=attempt,
                     details={"root_sha256": manifest["root_sha256"],
                              "generation": gen,
                              "previous": cur.get("manifest_id") if cur else None,
                              "manifest_id": manifest_id})
        server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
        return {"task_id": task_id, "manifest_id": manifest_id,
                "status": SWITCHED, "generation": gen,
                "root_sha256": manifest["root_sha256"],
                "previous": cur.get("manifest_id") if cur else None,
                "server": server}

    def _verify_image(self, image_dir: str, manifest: dict) -> None:
        if not os.path.isdir(image_dir):
            raise AgentError("IMAGE_MISSING", "镜像目录不存在，拒绝切换")
        with open(os.path.join(image_dir, "IMAGE.json"), encoding="utf-8") as f:
            info = json.load(f)
        if info["root_sha256"] != manifest["root_sha256"]:
            raise AgentError("ROOT_HASH_MISMATCH",
                             "镜像根散列与清单不符，拒绝切换")
        # 抽查全部对象散列并重算根（切换前失败保持原状态）
        entries = []
        for e in manifest["objects"]:
            p = os.path.join(image_dir, "objects", e["oid"].replace("/", "__"))
            if not os.path.exists(p):
                raise AgentError("OBJECT_MISSING",
                                 f"镜像缺少对象 {e['oid']}")
            with open(p, "rb") as f:
                data = f.read()
            if sha_bytes(data) != e["sha256"]:
                raise AgentError("OBJECT_HASH_MISMATCH",
                                 f"镜像对象 {e['oid']} 散列不符")
            entries.append(e)
        if root_hash(entries) != manifest["root_sha256"]:
            raise AgentError("ROOT_HASH_MISMATCH", "镜像根散列重算不符")

    def _reject_local(self, task_id: str, code: str, message: str) -> dict:
        local = self._local_task(task_id)
        self.conn.execute(
            "UPDATE tasks SET status='REJECTED', reason_code=?, reason=?,"
            " updated_at=? WHERE task_id=?",
            (code, message, self._ts(), task_id))
        self.conn.commit()
        self._save_attempt(task_id, local["attempt"], "switch", "rejected",
                           code, message)
        self._report(task_id, "rejected", attempt=local["attempt"],
                     code=code, message=message)
        return {"task_id": task_id, "manifest_id": local["manifest_id"],
                "status": REJECTED, "reason_code": code, "reason": message}

    def _idempotent_switch(self, task_id: str, manifest_id: str) -> dict:
        local = self._local_task(task_id)
        server = self.tr.get_json(f"/api/tasks/{urllib.parse.quote(task_id)}")
        return {"task_id": task_id, "manifest_id": manifest_id,
                "status": SWITCHED, "idempotent": True,
                "generation": local["generation"],
                "server": server}

    # -------------------------------------------------------------- queries --
    def status(self) -> dict:
        """重启后辨别：当前镜像 + 各任务（拉取中/可切换/已切换/拒绝/失败）+ 每次结果。"""
        tasks = []
        for r in self.conn.execute(
                "SELECT t.*, (SELECT COUNT(*) FROM blocks b "
                "WHERE b.task_id=t.task_id AND b.confirmed=1) AS confirmed "
                "FROM tasks t ORDER BY t.generation").fetchall():
            d = dict(r)
            d["confirmed_blocks"] = d.pop("confirmed")
            d["attempts"] = [dict(a) for a in self.conn.execute(
                "SELECT attempt,phase,result,code,message,ts FROM attempts "
                "WHERE task_id=? ORDER BY attempt_id", (d["task_id"],))]
            tasks.append(d)
        return {"device_id": self.device_id, "current": self.current(),
                "pending": dict(self._pending_task()) if self._pending_task() else None,
                "tasks": tasks}
