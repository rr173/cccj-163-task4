"""不可变镜像清单（manifest）。

发布端：从地图中心对象集合 + 设备集合生成 manifest 与压缩对象块；
设备端：用同一组 verify_* 函数验真（块散列 → 对象散列 → 整包根散列）。

manifest 一旦创建其 manifest_id 即固定（对全部内容求散列），
重复发布同一内容返回同一 id，仓库中的记录永远不被改写。
"""
from __future__ import annotations

from .common import VerifyError, canonical, pack_block, root_hash, sha_bytes, sha_obj


def make_manifest(*, snapshot_id: str, center_anchor: str,
                  objects: list[tuple[str, str, dict]],
                  devices: list[str], block_size: int = 256 * 1024,
                  note: str = "") -> tuple[dict, list[dict]]:
    """构造 (manifest, blocks)。

    objects: [(oid, type, obj)]（mapcenter.build_bundle 的输出）
    devices: 目标盒子集合（去重排序；空集合非法）
    block_size: 块目标大小（压缩前字节数近似上限；单对象超大时自成一块）
    """
    devs = sorted({d for d in devices if d})
    if not devs:
        raise ValueError("设备集合不能为空")

    # 对象条目 + 按目标大小切分块（块内顺序固定，便于断点续传与逐块计数）
    entries: list[dict] = []
    groups: list[list[tuple[str, dict]]] = []
    cur: list[tuple[str, dict]] = []
    cur_size = 0
    for oid, otype, obj in objects:
        body = canonical(obj)
        entries.append({
            "oid": oid, "type": otype,
            "sha256": sha_bytes(body), "size": len(body),
        })
        item = (oid, obj)
        if cur and cur_size + len(body) > block_size:
            groups.append(cur)
            cur, cur_size = [], 0
        cur.append(item)
        cur_size += len(body)
    if cur:
        groups.append(cur)
    if not groups:
        raise ValueError("镜像对象集合为空")

    blocks = []
    oid_index = {e["oid"]: e for e in entries}
    for idx, grp in enumerate(groups):
        blob = pack_block(grp)
        members = sorted(oid for oid, _o in grp)
        blocks.append({
            "index": idx,
            "sha256": sha_bytes(blob),
            "size": len(blob),
            "objects": members,
            "_blob": blob,  # 仅发布端内存使用，不落清单
        })

    body = {
        "format": "mapmirror-manifest/1",
        "snapshot_id": snapshot_id,
        "center_anchor": center_anchor,
        "block_size": block_size,
        "note": note,
        "objects": sorted(entries, key=lambda e: e["oid"]),
        "blocks": [{k: b[k] for k in ("index", "sha256", "size", "objects")}
                   for b in blocks],
        "devices": devs,
    }
    body["root_sha256"] = root_hash(body["objects"])
    manifest_id = "MF-" + sha_obj(body).removeprefix("sha256:")[:16]
    return {"manifest_id": manifest_id, **body}, blocks


def verify_manifest(m: dict) -> None:
    """结构/自洽性校验：根散列、块成员、对象条目、设备集合。"""
    if m.get("format") != "mapmirror-manifest/1":
        raise VerifyError("MANIFEST_BAD_FORMAT",
                          f"不支持的清单格式：{m.get('format')}")
    if root_hash(m["objects"]) != m["root_sha256"]:
        raise VerifyError("ROOT_HASH_MISMATCH", "清单根散列重算不符")
    oids = {e["oid"] for e in m["objects"]}
    covered: set[str] = set()
    for b in m["blocks"]:
        for oid in b["objects"]:
            if oid not in oids:
                raise VerifyError("MANIFEST_BAD_STRUCTURE",
                                  f"块 {b['index']} 含未知对象 {oid}")
            if oid in covered:
                raise VerifyError("MANIFEST_BAD_STRUCTURE",
                                  f"对象 {oid} 被多个块包含")
            covered.add(oid)
    if covered != oids:
        raise VerifyError("MANIFEST_BAD_STRUCTURE",
                          f"块覆盖对象集合不完整，缺 {sorted(oids - covered)}")
    idxs = [b["index"] for b in m["blocks"]]
    if idxs != list(range(len(m["blocks"]))):
        raise VerifyError("MANIFEST_BAD_STRUCTURE", "块编号不连续")
    if not m.get("devices"):
        raise VerifyError("MANIFEST_BAD_STRUCTURE", "清单设备集合为空")
    want = "MF-" + sha_obj({k: v for k, v in m.items()
                            if k != "manifest_id"}).removeprefix("sha256:")[:16]
    if want != m["manifest_id"]:
        raise VerifyError("MANIFEST_ID_MISMATCH",
                          f"清单 id 与内容不符：{m['manifest_id']} != {want}")
