"""发布人员工作流：从地图中心选已生效快照 + 设备集合，生成不可变清单并入库。

流程严格有序:
  1. 只读中心，列出/导出已生效快照（中心被篡改 -> CENTER_TAMPERED，拒绝）；
  2. 用快照对象构造 manifest（manifest_id 由全部内容散列决定）；
  3. 先逐块上传压缩对象块（仓库按 sha256 去重），再提交清单；
  4. 重试完全相同的选择 -> 同一 manifest_id，仓库返回 idempotent 与第一次任务。

发布端不持有任何镜像状态；地图中心全程只读。
"""
from __future__ import annotations

from .manifest import make_manifest
from .mapcenter import MapCenterError
from .transport import TransportError


class PublishError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def list_center_snapshots(center) -> list[dict]:
    try:
        return center.list_effective()
    except MapCenterError as e:
        raise PublishError(e.code, e.message)


def publish(center, transport, *, snapshot_id: str, devices: list[str],
            block_size: int = 256 * 1024, note: str = "",
            created_by: str = "ops") -> dict:
    devices = sorted({d.strip() for d in devices if d and d.strip()})
    if not devices:
        raise PublishError("NO_DEVICES", "设备集合不能为空")
    try:
        bundle = center.export(snapshot_id)
    except MapCenterError as e:
        raise PublishError(e.code, e.message)

    manifest, blocks = make_manifest(
        snapshot_id=bundle["snapshot_id"],
        center_anchor=bundle["center_anchor"],
        objects=bundle["objects"],
        devices=devices,
        block_size=block_size,
        note=note)

    uploaded, reused = [], []
    try:
        for b in blocks:
            resp = transport.put_block(b["sha256"], b.pop("_blob"))
            (reused if resp.get("idempotent") else uploaded).append(b["sha256"])
        result, _status = transport.post_json(
            "/api/manifests", {"manifest": manifest, "created_by": created_by})
    except TransportError as e:
        raise PublishError(e.code, e.message)

    result["blocks_total"] = len(blocks)
    result["blocks_uploaded"] = len(uploaded)
    result["blocks_reused"] = len(reused)
    result["root_sha256"] = manifest["root_sha256"]
    result["snapshot_id"] = manifest["snapshot_id"]
    return result
