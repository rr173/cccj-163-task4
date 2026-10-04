"""地图中心只读适配器。

镜像仓库的发布端从地图中心（既有高精地图服务 engine.py 的数据）选定
**已生效快照** 并导出对象集合。关键不变量：

- 适配器只有 GET / 读文件，从不写入或实例化 MapEngine 的写路径；
- 导出前逐瓦片复核内容散列 `sha256(canonical(tile)) == tile_id 对应 hash`，
  地图中心状态被篡改时抛 CENTER_TAMPERED，绝不据其生成清单；
- 中心锚点 center_anchor 对快照被固定的核心字段求散列，
  设备换指针前可据此辨认"这是哪一个中心快照"。
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request

from .common import VerifyError, canonical, sha_bytes, sha_obj

# 快照被"固定"的字段（与 engine.create_route 中写入的不可变部分一致；
# tile_contents / advisory 是派生物，不参与锚点）。
PINNED_FIELDS = (
    "snapshot_id", "vehicle_id", "from_node", "to_node", "status",
    "created_at", "version", "tiles", "overlays", "route",
    "parent_snapshot_id", "completed_at",
)


class MapCenterError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def snapshot_pinned(snap_view: dict) -> dict:
    missing = [k for k in PINNED_FIELDS if k not in snap_view]
    if missing:
        raise MapCenterError(
            "CENTER_SNAPSHOT_INCOMPLETE",
            f"中心快照缺少固定字段：{', '.join(missing)}")
    return {k: snap_view[k] for k in PINNED_FIELDS}


def center_anchor(pinned: dict) -> str:
    return sha_obj(pinned)


def verify_tiles(tile_contents: dict, tiles_map: dict) -> None:
    """tiles_map: tile_id -> 声称的 sha256；tile_contents: tile_id -> 对象。"""
    for tid, want in sorted(tiles_map.items()):
        obj = tile_contents.get(tid)
        if obj is None:
            raise VerifyError("CENTER_OBJECT_MISSING",
                              f"地图中心缺少瓦片内容 {tid}（{want}）")
        got = sha_bytes(canonical(obj))
        if got != want:
            raise VerifyError("CENTER_TAMPERED",
                              f"地图中心瓦片 {tid} 内容散列与索引不符："
                              f"索引 {want}，实得 {got}")


def build_bundle(snap_view: dict) -> dict:
    """把一个中心快照视图转成镜像对象集合。

    返回 {
      snapshot_id, pinned, center_anchor,
      objects: [(oid, type, obj)],
      object_shas: {oid: sha256}
    }
    """
    pinned = snapshot_pinned(snap_view)
    verify_tiles(snap_view.get("tile_contents", {}), pinned["tiles"])
    sid = pinned["snapshot_id"]
    objects: list[tuple[str, str, dict]] = [
        (f"snapshot/{sid}", "snapshot", pinned),
    ]
    for tid in sorted(pinned["tiles"]):
        objects.append((f"tile/{tid}", "tile", snap_view["tile_contents"][tid]))
    shas = {oid: sha_bytes(canonical(obj)) for oid, _t, obj in objects}
    return {
        "snapshot_id": sid,
        "pinned": pinned,
        "center_anchor": center_anchor(pinned),
        "objects": objects,
        "object_shas": shas,
    }


class MapCenter:
    """接口：list_effective() / export(snapshot_id)。全部只读。"""

    def list_effective(self) -> list[dict]:
        raise NotImplementedError

    def export(self, snapshot_id: str) -> dict:
        raise NotImplementedError


class LocalMapCenter(MapCenter):
    """直接只读加载 engine 的 data/mapstate.json（镜像进程与中心同机时）。"""

    def __init__(self, path: str):
        self.path = path
        try:
            with open(path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
        except FileNotFoundError:
            raise MapCenterError("CENTER_UNREACHABLE",
                                 f"地图中心状态文件不存在：{path}")
        except json.JSONDecodeError as e:
            raise MapCenterError("CENTER_UNREACHABLE",
                                 f"地图中心状态无法解析：{e}")

    def _snapshots(self) -> dict:
        return self.state.get("snapshots", {})

    @staticmethod
    def _effective(snap: dict) -> bool:
        # "已生效快照"：被车辆固定（active）；completed 的行程其快照仍可镜像，
        # 但发布语义上以 active 为准；superseded 子快照链也允许，只要瓦片自洽。
        # 这里接受 active / completed / superseded，拒绝任何被标记作废之外的未知状态。
        return snap.get("status") in ("active", "completed", "superseded")

    def list_effective(self) -> list[dict]:
        out = []
        snaps = self._snapshots()
        for sid, snap in snaps.items():
            if not self._effective(snap):
                continue
            out.append({
                "snapshot_id": sid,
                "vehicle_id": snap.get("vehicle_id"),
                "status": snap.get("status"),
                "version": snap.get("version", {}).get("version"),
                "created_at": snap.get("created_at"),
                "tiles": len(snap.get("tiles", {})),
            })
        out.sort(key=lambda x: x["created_at"] or "")
        return out

    def _view(self, sid: str) -> dict:
        snap = self._snapshots().get(sid)
        if snap is None:
            raise MapCenterError("SNAPSHOT_NOT_EFFECTIVE",
                                 f"地图中心不存在快照 {sid}")
        if not self._effective(snap):
            raise MapCenterError("SNAPSHOT_NOT_EFFECTIVE",
                                 f"快照 {sid} 状态 {snap.get('status')}，不可发布")
        view = dict(snap)
        view["tile_contents"] = {
            tid: self.state["tiles"][h]
            for tid, h in snap["tiles"].items()
            if h in self.state.get("tiles", {})
        }
        return view

    def export(self, snapshot_id: str) -> dict:
        return build_bundle(self._view(snapshot_id))


class HttpMapCenter(MapCenter):
    """通过地图服务 HTTP API 只读拉取（仅 GET）。"""

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str) -> dict:
        req = urllib.request.Request(self.base + path, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise MapCenterError("SNAPSHOT_NOT_EFFECTIVE",
                                     f"地图中心返回 404：{path}")
            raise MapCenterError("CENTER_UNREACHABLE",
                                 f"地图中心 HTTP {e.code}：{path}")
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            raise MapCenterError("CENTER_UNREACHABLE", f"地图中心不可达：{e}")

    def list_effective(self) -> list[dict]:
        data = self._get("/api/snapshots")
        return [s for s in data.get("snapshots", [])
                if s.get("status") in ("active", "completed", "superseded")]

    def export(self, snapshot_id: str) -> dict:
        return build_bundle(self._get(f"/api/snapshots/{snapshot_id}"))
