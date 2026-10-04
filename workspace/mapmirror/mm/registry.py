"""镜像仓库（registry）：不可变清单库 + 压缩对象块库 + 任务/事件事实源。

存储（根目录 root）:
  registry.db          SQLite：manifests / blocks(去重) / tasks / task_events
  blobs/<ab>/<sha>     内容寻址的压缩块（sha256 即主键，天然去重）

关键不变量:
  * manifest 以内容散列为 id，重复创建返回同一记录，载荷永不改写；
  * 块按 sha256 去重存放；同散列不同内容 -> BLOCK_CONTENT_CONFLICT；
  * 任务 task_id = sha256(manifest_id|device_id)，重试同一清单永远返回第一次任务；
  * 每设备 generation 单调递增（发布顺序定代次）；
  * confirmed_blocks 只增不减（已确认块不重复计数）；
  * 终态 SWITCHED/REJECTED/FAILED 冻结，后续事件不改写第一次结果；
  * 网页与设备 CLI 都只读这里的 /api/tasks、/api/devices（同一数据源）。
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading

from .common import now_iso, sha_bytes, sha_file
from .manifest import verify_manifest

SCHEMA = """
CREATE TABLE IF NOT EXISTS manifests (
  manifest_id   TEXT PRIMARY KEY,
  snapshot_id   TEXT NOT NULL,
  center_anchor TEXT NOT NULL,
  root_sha256   TEXT NOT NULL,
  payload       TEXT NOT NULL,
  created_at    TEXT NOT NULL,
  created_by    TEXT DEFAULT '',
  note          TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS blocks (
  sha256 TEXT PRIMARY KEY,
  size   INTEGER NOT NULL,
  path   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
  task_id          TEXT PRIMARY KEY,
  manifest_id      TEXT NOT NULL,
  device_id        TEXT NOT NULL,
  generation       INTEGER NOT NULL,
  status           TEXT NOT NULL,
  reason_code      TEXT DEFAULT '',
  reason           TEXT DEFAULT '',
  confirmed_blocks INTEGER NOT NULL DEFAULT 0,
  total_blocks     INTEGER NOT NULL,
  attempt          INTEGER NOT NULL DEFAULT 0,
  last_error       TEXT DEFAULT '',
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL,
  switched_at      TEXT,
  idem_result      TEXT DEFAULT '',
  UNIQUE(manifest_id, device_id)
);
CREATE INDEX IF NOT EXISTS idx_tasks_device ON tasks(device_id, generation);
CREATE TABLE IF NOT EXISTS task_events (
  event_uid        TEXT PRIMARY KEY,
  task_id          TEXT NOT NULL,
  seq              INTEGER NOT NULL,
  ts               TEXT NOT NULL,
  event            TEXT NOT NULL,
  attempt          INTEGER,
  confirmed_blocks INTEGER,
  total_blocks     INTEGER,
  code             TEXT DEFAULT '',
  message          TEXT DEFAULT '',
  details          TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, seq);
CREATE TABLE IF NOT EXISTS devices (
  device_id           TEXT PRIMARY KEY,
  current_manifest_id TEXT DEFAULT '',
  current_generation  INTEGER NOT NULL DEFAULT 0,
  current_root        TEXT DEFAULT '',
  last_switched_at    TEXT,
  pending_task_id     TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

TERMINAL = {"SWITCHED", "REJECTED", "FAILED"}
# 允许的状态前向/保持迁移（pull_progress 等允许 PULLING->PULLING）
FORWARD = {
    "NEW": {"PULLING", "PULLED", "STAGED", "SWITCHED", "REJECTED", "FAILED"},
    "PULLING": {"PULLING", "PULLED", "STAGED", "SWITCHED", "REJECTED", "FAILED"},
    "PULLED": {"STAGED", "SWITCHED", "REJECTED", "FAILED"},
    "STAGED": {"STAGED", "SWITCHED", "REJECTED", "FAILED"},
}

EVENT_STATUS = {
    "created": "NEW",
    "pull_started": "PULLING",
    "pull_progress": "PULLING",
    "pull_paused": "PULLING",
    "pull_completed": "PULLED",
    "stage_completed": "STAGED",
    "switch_succeeded": "SWITCHED",
    "rejected": "REJECTED",
    "failed": "FAILED",
}


class RegistryError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def task_public(row: sqlite3.Row | dict) -> dict:
    d = dict(row)
    frozen = d.pop("idem_result", "") or ""
    d["frozen_result"] = json.loads(frozen) if frozen else None
    return d


class Registry:
    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)
        os.makedirs(os.path.join(root, "blobs"), exist_ok=True)
        self.db_path = os.path.join(root, "registry.db")
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self):
        with self.lock:
            self.conn.close()

    # ------------------------------------------------------------- helpers --
    def _blob_path(self, digest: str) -> str:
        h = digest.removeprefix("sha256:")
        return os.path.join(self.root, "blobs", h[:2], h)

    @staticmethod
    def task_id(manifest_id: str, device_id: str) -> str:
        return "T-" + hashlib.sha256(
            f"{manifest_id}|{device_id}".encode()).hexdigest()[:16]

    def _task(self, cur: sqlite3.Cursor, task_id: str) -> sqlite3.Row:
        row = cur.execute("SELECT * FROM tasks WHERE task_id=?",
                          (task_id,)).fetchone()
        if row is None:
            raise RegistryError("TASK_NOT_FOUND", f"任务 {task_id} 不存在", 404)
        return row

    # ------------------------------------------------------------- blocks ---
    def put_block(self, digest: str, data: bytes) -> dict:
        if not digest.startswith("sha256:"):
            raise RegistryError("BAD_DIGEST", "块散列必须以 sha256: 开头", 400)
        got = sha_bytes(data)
        if got != digest:
            raise RegistryError(
                "BLOCK_HASH_MISMATCH",
                f"上传块散列不符：声明 {digest}，实得 {got}", 422)
        path = self._blob_path(digest)
        with self.lock:
            row = self.conn.execute(
                "SELECT size FROM blocks WHERE sha256=?", (digest,)).fetchone()
            if row and os.path.exists(path) and sha_file(path) == digest:
                # 已注册且磁盘完好：幂等
                return {"sha256": digest, "size": row["size"],
                        "idempotent": True}
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".upload"
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            if sha_file(tmp) != digest:
                os.unlink(tmp)
                raise RegistryError("BLOCK_HASH_MISMATCH",
                                    "落盘复核块散列失败", 422)
            os.replace(tmp, path)
            if row is None:
                self.conn.execute(
                    "INSERT INTO blocks(sha256,size,path) VALUES(?,?,?)",
                    (digest, len(data), path))
            # row 已存在但磁盘损坏/缺失：已用正确字节修复，记录保持不变
            self.conn.commit()
        return {"sha256": digest, "size": len(data),
                "idempotent": bool(row)}

    def get_block(self, digest: str) -> bytes:
        with self.lock:
            row = self.conn.execute(
                "SELECT path FROM blocks WHERE sha256=?", (digest,)).fetchone()
        if row is None:
            raise RegistryError("BLOCK_NOT_FOUND", f"块 {digest} 不存在", 404)
        with open(row["path"], "rb") as f:
            data = f.read()
        if sha_bytes(data) != digest:
            # 仓库对象损坏：明确报错，绝不把坏字节交给设备
            raise RegistryError("BLOCK_CORRUPT",
                                f"仓库块 {digest} 已损坏（散列复核失败）", 500)
        return data

    def block_meta(self, digest: str) -> dict:
        with self.lock:
            row = self.conn.execute(
                "SELECT sha256,size FROM blocks WHERE sha256=?",
                (digest,)).fetchone()
        if row is None:
            raise RegistryError("BLOCK_NOT_FOUND", f"块 {digest} 不存在", 404)
        return dict(row)

    # ---------------------------------------------------------- manifests ---
    def put_manifest(self, manifest: dict, created_by: str = "") -> dict:
        try:
            verify_manifest(manifest)
        except Exception as e:
            code = getattr(e, "code", "MANIFEST_INVALID")
            raise RegistryError(code, str(getattr(e, "message", e)), 422)
        mid = manifest["manifest_id"]
        payload = json.dumps(manifest, ensure_ascii=False,
                             sort_keys=True, separators=(",", ":"))
        with self.lock:
            cur = self.conn.cursor()
            existing = cur.execute(
                "SELECT payload FROM manifests WHERE manifest_id=?",
                (mid,)).fetchone()
            if existing:
                if existing["payload"] != payload:
                    raise RegistryError(
                        "MANIFEST_IMMUTABLE",
                        f"清单 {mid} 已存在且内容不同；清单不可变", 409)
                tasks = [task_public(r) for r in cur.execute(
                    "SELECT * FROM tasks WHERE manifest_id=? ORDER BY device_id",
                    (mid,))]
                return {"manifest_id": mid, "idempotent": True, "tasks": tasks}

            for b in manifest["blocks"]:
                row = cur.execute(
                    "SELECT size FROM blocks WHERE sha256=?",
                    (b["sha256"],)).fetchone()
                if row is None:
                    raise RegistryError(
                        "BLOCK_NOT_UPLOADED",
                        f"清单块 {b['index']}（{b['sha256'][:18]}…）"
                        f"尚未上传到仓库", 412)
                if row["size"] != b["size"]:
                    raise RegistryError(
                        "BLOCK_CONTENT_CONFLICT",
                        f"块 {b['sha256'][:18]}… 大小与清单不符", 409)

            ts = now_iso()
            cur.execute(
                "INSERT INTO manifests(manifest_id,snapshot_id,center_anchor,"
                "root_sha256,payload,created_at,created_by,note) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (mid, manifest["snapshot_id"], manifest["center_anchor"],
                 manifest["root_sha256"], payload, ts,
                 created_by, manifest.get("note", "")))
            tasks = []
            for dev in manifest["devices"]:
                gen = self._next_generation(cur, dev)
                tid = self.task_id(mid, dev)
                cur.execute(
                    "INSERT INTO tasks(task_id,manifest_id,device_id,generation,"
                    "status,total_blocks,created_at,updated_at) "
                    "VALUES(?,?,?,?,'NEW',?,?,?)",
                    (tid, mid, dev, gen, len(manifest["blocks"]), ts, ts))
                cur.execute(
                    "INSERT INTO devices(device_id) VALUES(?) "
                    "ON CONFLICT(device_id) DO NOTHING", (dev,))
                tasks.append(task_public(cur.execute(
                    "SELECT * FROM tasks WHERE task_id=?", (tid,)).fetchone()))
            self.conn.commit()
        return {"manifest_id": mid, "idempotent": False, "tasks": tasks}

    @staticmethod
    def _next_generation(cur: sqlite3.Cursor, dev: str) -> int:
        key = f"gen:{dev}"
        row = cur.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        gen = (int(row["value"]) + 1) if row else 1
        cur.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (key, str(gen)))
        return gen

    def get_manifest(self, mid: str) -> dict:
        with self.lock:
            row = self.conn.execute(
                "SELECT payload FROM manifests WHERE manifest_id=?",
                (mid,)).fetchone()
        if row is None:
            raise RegistryError("MANIFEST_NOT_FOUND", f"清单 {mid} 不存在", 404)
        return json.loads(row["payload"])

    def list_manifests(self) -> list[dict]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT manifest_id,snapshot_id,center_anchor,root_sha256,"
                "created_at,created_by,note FROM manifests ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------- tasks ---
    def get_task(self, task_id: str) -> dict:
        with self.lock:
            return task_public(self._task(self.conn, task_id))

    def list_tasks(self, device: str | None = None,
                   manifest: str | None = None) -> list[dict]:
        sql = "SELECT * FROM tasks WHERE 1=1"
        args: list = []
        if device:
            sql += " AND device_id=?"
            args.append(device)
        if manifest:
            sql += " AND manifest_id=?"
            args.append(manifest)
        sql += " ORDER BY created_at, device_id"
        with self.lock:
            return [task_public(r)
                    for r in self.conn.execute(sql, args).fetchall()]

    def list_events(self, task_id: str) -> list[dict]:
        with self.lock:
            self._task(self.conn, task_id)
            rows = self.conn.execute(
                "SELECT * FROM task_events WHERE task_id=? ORDER BY seq",
                (task_id,)).fetchall()
        return [dict(r) for r in rows]

    def report_event(self, task_id: str, ev: dict) -> dict:
        """设备上报一次处理结果/进度。event_uid 幂等；终态冻结第一次结果。"""
        uid = str(ev.get("event_uid", "")).strip()
        if not uid:
            raise RegistryError("BAD_EVENT", "event_uid 必填", 400)
        event = ev.get("event", "")
        if event not in EVENT_STATUS:
            raise RegistryError("BAD_EVENT", f"未知事件类型 {event}", 400)
        with self.lock:
            cur = self.conn.cursor()
            if cur.execute("SELECT 1 FROM task_events WHERE event_uid=?",
                           (uid,)).fetchone():
                return {"task": task_public(self._task(cur, task_id)),
                        "idempotent": True, "deduplicated": True}
            task = self._task(cur, task_id)
            status_now = task["status"]

            # 终态冻结：SWITCHED/REJECTED/FAILED 的结果事件不再产生状态变更，
            # 只登记一条 frozen_ignored 留痕（uid 去重），第一次结果原样保留。
            frozen_ignored = (
                status_now in TERMINAL and event in (
                    "switch_succeeded", "rejected", "failed",
                    "stage_completed", "pull_completed"))

            seq = cur.execute(
                "SELECT COALESCE(MAX(seq),0) FROM task_events "
                "WHERE task_id=?", (task_id,)).fetchone()[0] + 1
            ts = ev.get("ts") or now_iso()
            cur.execute(
                "INSERT INTO task_events(event_uid,task_id,seq,ts,event,attempt,"
                "confirmed_blocks,total_blocks,code,message,details) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (uid, task_id, seq, ts, event, ev.get("attempt"),
                 ev.get("confirmed_blocks"), ev.get("total_blocks"),
                 ev.get("code", ""), ev.get("message", ""),
                 json.dumps(ev.get("details", {}), ensure_ascii=False,
                            sort_keys=True)))

            sets, args = ["updated_at=?"], [ts]
            cb = ev.get("confirmed_blocks")
            if isinstance(cb, int) and cb > task["confirmed_blocks"]:
                # 已确认块只增不减：断点续传的重复确认不计入
                sets.append("confirmed_blocks=?")
                args.append(cb)
            if isinstance(ev.get("attempt"), int) and ev["attempt"] > task["attempt"]:
                sets.append("attempt=?")
                args.append(ev["attempt"])
            if event == "pull_paused" and ev.get("message"):
                sets.append("last_error=?")
                args.append(ev["message"][:500])

            new_status = EVENT_STATUS[event]
            switched_details: dict = {}
            if not frozen_ignored and new_status != status_now \
                    and (new_status in FORWARD.get(status_now, set())
                         or (status_now == "NEW" and new_status == "PULLING")):
                sets.append("status=?")
                args.append(new_status)
                if new_status in TERMINAL:
                    sets += ["reason_code=?", "reason=?"]
                    args += [ev.get("code", ""), ev.get("message", "")]
                    sets.append("idem_result=?")
                    args.append(json.dumps({
                        "frozen_at": ts, "outcome": event,
                        "code": ev.get("code", ""),
                        "message": ev.get("message", ""),
                        "details": ev.get("details", {}),
                    }, ensure_ascii=False, sort_keys=True))
                    if new_status == "SWITCHED":
                        sets.append("switched_at=?")
                        args.append(ts)
                        switched_details = ev.get("details", {})

            args.append(task_id)
            cur.execute(f"UPDATE tasks SET {', '.join(sets)} WHERE task_id=?",
                        args)
            self._sync_device(cur, self._task(cur, task_id), switched_details)
            self.conn.commit()
            return {"task": task_public(self._task(cur, task_id)),
                    "idempotent": False, "seq": seq,
                    "frozen_ignored": frozen_ignored}

    def _sync_device(self, cur: sqlite3.Cursor, task: sqlite3.Row,
                     switched_details: dict | None = None) -> None:
        """维护 devices 聚合（当前镜像 / 待切换槽），供网页与 CLI 概览。"""
        dev = task["device_id"]
        cur.execute("INSERT INTO devices(device_id) VALUES(?) "
                    "ON CONFLICT(device_id) DO NOTHING", (dev,))
        if task["status"] == "SWITCHED":
            d = switched_details or {}
            cur.execute(
                "UPDATE devices SET current_manifest_id=?, current_generation=?,"
                "current_root=?, last_switched_at=?, pending_task_id='' "
                "WHERE device_id=?",
                (task["manifest_id"], task["generation"],
                 d.get("root_sha256", ""), task["switched_at"], dev))
            return
        if task["status"] in ("REJECTED", "FAILED"):
            cur.execute(
                "UPDATE devices SET pending_task_id='' WHERE device_id=? "
                "AND pending_task_id=?", (dev, task["task_id"]))
            return
        pend = cur.execute(
            "SELECT task_id FROM tasks WHERE device_id=? AND status='STAGED' "
            "ORDER BY generation DESC LIMIT 1", (dev,)).fetchone()
        cur.execute("UPDATE devices SET pending_task_id=? WHERE device_id=?",
                    (pend["task_id"] if pend else "", dev))

    def list_devices(self) -> list[dict]:
        with self.lock:
            devs = [dict(r) for r in self.conn.execute(
                "SELECT * FROM devices ORDER BY device_id").fetchall()]
            for d in devs:
                d["tasks"] = [task_public(r) for r in self.conn.execute(
                    "SELECT * FROM tasks WHERE device_id=? "
                    "ORDER BY generation", (d["device_id"],)).fetchall()]
            return devs
