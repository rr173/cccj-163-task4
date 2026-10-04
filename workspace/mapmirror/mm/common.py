"""路侧地图镜像仓库（mapmirror）公共原语。

- canonical/sha256: 与地图中心 engine.py 完全一致的规范化哈希
- BlockFormat: 压缩对象块的打包/解包/校验（gzip + 对象序列）
- atomic: 目录原子操作（原子写文件、原子换 symlink、fsync 目录）

零第三方依赖，仅使用 Python 标准库。
"""
from __future__ import annotations

import gzip
import hashlib
import json
import os
import tempfile
from typing import Any

# 与地图中心相同的规范化规则：排序键、无空白、UTF-8。
def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False).encode()


def sha_obj(obj: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical(obj)).hexdigest()


def sha_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class VerifyError(Exception):
    """内容验真失败（块散列/对象散列/根散列不一致）。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ----------------------------------------------------------------- blocks ---
# 块格式（自描述、可流式校验）:
#   gzip(
#     b"MMBL1\n"
#     对每个对象:
#       一行 {"oid": str, "sha256": str, "size": int}      (JSON, 不含 oid 顺序依赖)
#       b"\n" + 原始 JSON 字节（恰好 size 字节）
#   )
# 块散列 = sha256(压缩后字节)，由传输层与仓库存储核对；
# 对象散列 = sha256(canonical(对象))，解包后逐个核对。
MAGIC = b"MMBL1\n"


def pack_block(objects: list[tuple[str, dict]]) -> bytes:
    """objects: 按 manifest 中顺序的 (oid, object) 列表，返回压缩块字节。"""
    raw = bytearray(MAGIC)
    for oid, obj in objects:
        body = canonical(obj)
        meta = {"oid": oid, "sha256": sha_bytes(body), "size": len(body)}
        raw += canonical(meta) + b"\n" + body
    return gzip.compress(bytes(raw), mtime=0)


def unpack_block(blob: bytes, expect_block_sha: str | None = None,
                 expect_objects: dict | None = None) -> list[tuple[str, dict, str]]:
    """校验并解包一个块。

    expect_block_sha: 若给出，先校验压缩块整体散列（块散列）。
    expect_objects: oid -> 期望 sha256；若给出，逐对象复核散列。
    返回 [(oid, object, 对象实际 sha256)]；任何不符抛 VerifyError。
    """
    if expect_block_sha:
        got = sha_bytes(blob)
        if got != expect_block_sha:
            raise VerifyError(
                "BLOCK_HASH_MISMATCH",
                f"块散列不符：期望 {expect_block_sha}，实得 {got}")
    try:
        raw = gzip.decompress(blob)
    except Exception as e:  # gzip 损坏
        raise VerifyError("BLOCK_CORRUPT", f"块无法解压：{e}")
    if not raw.startswith(MAGIC):
        raise VerifyError("BLOCK_BAD_MAGIC", "块魔数错误或被截断")
    out: list[tuple[str, dict, str]] = []
    pos = len(MAGIC)
    while pos < len(raw):
        nl = raw.find(b"\n", pos)
        if nl < 0:
            raise VerifyError("BLOCK_CORRUPT", "块元数据被截断")
        try:
            meta = json.loads(raw[pos:nl])
            oid = meta["oid"]
            size = int(meta["size"])
            want = meta["sha256"]
        except Exception as e:
            raise VerifyError("BLOCK_CORRUPT", f"块元数据非法：{e}")
        body = raw[nl + 1: nl + 1 + size]
        if len(body) != size:
            raise VerifyError("BLOCK_CORRUPT", f"对象 {oid} 数据被截断")
        got = sha_bytes(body)
        if got != want:
            raise VerifyError("OBJECT_HASH_MISMATCH",
                              f"对象 {oid} 散列不符：期望 {want}，实得 {got}")
        if expect_objects and oid in expect_objects and expect_objects[oid] != got:
            raise VerifyError(
                "OBJECT_HASH_MISMATCH",
                f"对象 {oid} 与清单不符：清单 {expect_objects[oid]}，块内 {got}")
        try:
            obj = json.loads(body)
        except Exception as e:
            raise VerifyError("BLOCK_CORRUPT", f"对象 {oid} JSON 非法：{e}")
        out.append((oid, obj, got))
        pos = nl + 1 + size
    return out


def root_hash(object_entries: list[dict]) -> str:
    """整包根散列：对清单中对象条目（oid/sha256/size/type）规范化后求 sha256。

    设备暂存完成后用相同函数重算，与 manifest.root_sha256 比对。
    """
    return sha_obj(sorted(object_entries, key=lambda e: e["oid"]))


# ------------------------------------------------------------- atomic io ---

def atomic_write_bytes(path: str, data: bytes) -> None:
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=d)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    try:
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass


def atomic_write_json(path: str, obj: Any) -> None:
    atomic_write_bytes(path, json.dumps(obj, ensure_ascii=False, indent=2).encode())


def hardlink_or_copy(src: str, dst: str) -> None:
    """优先硬链接（内容寻址仓库去重），跨文件系统时回退原子复制。"""
    os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
    try:
        os.link(src, dst)
    except FileExistsError:
        pass
    except OSError:
        with open(src, "rb") as f:
            data = f.read()
        atomic_write_bytes(dst, data)


def atomic_symlink(target: str, link_path: str) -> None:
    """以 rename 原子方式把 link_path 指向 target（相对/绝对均可）。"""
    d = os.path.dirname(link_path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".lnk-", dir=d)
    os.close(fd)
    os.unlink(tmp)
    os.symlink(target, tmp)
    os.replace(tmp, link_path)
    try:
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)
    except OSError:
        pass
