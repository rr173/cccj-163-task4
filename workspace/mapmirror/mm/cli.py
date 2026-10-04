"""统一命令行（零依赖，python -m mm 直接运行）。

仓库端:
  python -m mm serve --store ./rstore --port 8090

发布端（对地图中心只读）:
  python -m mm center-list  --center ./data/mapstate.json
  python -m mm publish --center ./data/mapstate.json --registry http://127.0.0.1:8090 \\
      --snapshot S-0001 --devices BOX-A,BOX-B --note "v2 路侧镜像"

设备端:
  python -m mm device pull     --device BOX-A --device-root ./dev-A \\
      --registry http://127.0.0.1:8090 --manifest MF-xxxx
  python -m mm device activate --device BOX-A --device-root ./dev-A \\
      --registry http://127.0.0.1:8090 --manifest MF-xxxx
  python -m mm device status   --device BOX-A --device-root ./dev-A
  python -m mm device show     --registry http://... --task T-xxxx   # 仓库同源视图

共用视图（网页与 CLI 同一数据源；show 直接打仓库 /api/tasks）:
  python -m mm tasks    [--device BOX-A] [--registry http://...]
  python -m mm devices  [--registry http://...]
"""
from __future__ import annotations

import argparse
import json
import sys

from .mapcenter import HttpMapCenter, LocalMapCenter
from .publisher import PublishError, list_center_snapshots, publish
from .transport import FaultInject, HttpTransport, TransportError
from .device import DeviceAgent, AgentError


def _p(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _center(args):
    if args.center.startswith("http://") or args.center.startswith("https://"):
        return HttpMapCenter(args.center)
    return LocalMapCenter(args.center)


def _transport(registry: str, fault_file: str = ""):
    t = HttpTransport(registry)
    if fault_file:
        with open(fault_file, encoding="utf-8") as f:
            return FaultInject(t, json.load(f))
    return t


def cmd_serve(args):
    from .server import run
    run(args.store, args.host, args.port)


def cmd_center_list(args):
    _p({"snapshots": list_center_snapshots(_center(args))})


def cmd_publish(args):
    result = publish(
        _center(args), _transport(args.registry, args.faults),
        snapshot_id=args.snapshot,
        devices=args.devices.split(","),
        block_size=args.block_size,
        note=args.note)
    _p(result)


def _agent(args) -> DeviceAgent:
    return DeviceAgent(args.device, args.device_root, args.registry,
                       transport=_transport(args.registry, args.faults))


def cmd_device_pull(args):
    ag = _agent(args)
    _p(ag.process(args.manifest))


def cmd_device_activate(args):
    ag = _agent(args)
    _p(ag.activate(args.manifest))


def cmd_device_status(args):
    ag = _agent(args)
    _p(ag.status())


def cmd_show(args):
    t = HttpTransport(args.registry)
    out = t.get_json(f"/api/tasks/{args.task}")
    out["events"] = t.get_json(f"/api/tasks/{args.task}/events")["events"]
    _p(out)


def cmd_tasks(args):
    t = HttpTransport(args.registry)
    qs = []
    if args.device:
        qs.append(f"device={args.device}")
    if args.manifest:
        qs.append(f"manifest={args.manifest}")
    path = "/api/tasks" + ("?" + "&".join(qs) if qs else "")
    _p(t.get_json(path))


def cmd_devices(args):
    t = HttpTransport(args.registry)
    _p(t.get_json("/api/devices"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="mm",
                                 description="路侧地图镜像仓库")
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("serve", help="启动镜像仓库服务")
    s.add_argument("--store", default="./registry-store")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8090)
    s.set_defaults(func=cmd_serve)

    cl = sub.add_parser("center-list", help="列出地图中心已生效快照（只读）")
    cl.add_argument("--center", required=True,
                    help="mapstate.json 路径 或 http(s)://地图服务")
    cl.set_defaults(func=cmd_center_list)

    pub = sub.add_parser("publish", help="选快照+设备集合，发布不可变清单")
    pub.add_argument("--center", required=True)
    pub.add_argument("--registry", required=True)
    pub.add_argument("--snapshot", required=True)
    pub.add_argument("--devices", required=True, help="逗号分隔设备 id")
    pub.add_argument("--block-size", type=int, default=256 * 1024)
    pub.add_argument("--note", default="")
    pub.add_argument("--faults", default="", help="故障计划 JSON（可选）")
    pub.set_defaults(func=cmd_publish)

    dev = sub.add_parser("device", help="设备代理命令")
    dsub = dev.add_subparsers(dest="dcmd", required=True)
    common_box = dict()
    for name, helptext in (("pull", "拉取/续传并暂存校验"),
                           ("activate", "原子换指针切换"),
                           ("status", "本地状态（重启可辨别）")):
        d = dsub.add_parser(name, help=helptext)
        d.add_argument("--device", required=True)
        d.add_argument("--device-root", required=True)
        d.add_argument("--registry", required=True)
        d.add_argument("--manifest")
        d.add_argument("--faults", default="")
        d.set_defaults(func={"pull": cmd_device_pull,
                             "activate": cmd_device_activate,
                             "status": cmd_device_status}[name])

    show = sub.add_parser("show", help="查看单个任务（仓库同源：进度+事件+拒绝理由）")
    show.add_argument("--registry", required=True)
    show.add_argument("--task", required=True)
    show.set_defaults(func=cmd_show)

    tl = sub.add_parser("tasks", help="任务进度总览（与网页同源）")
    tl.add_argument("--registry", required=True)
    tl.add_argument("--device", default="")
    tl.add_argument("--manifest", default="")
    tl.set_defaults(func=cmd_tasks)

    dv = sub.add_parser("devices", help="设备/当前镜像/待切换槽总览（与网页同源）")
    dv.add_argument("--registry", required=True)
    dv.set_defaults(func=cmd_devices)

    args = ap.parse_args(argv)
    try:
        args.func(args)
    except (PublishError, AgentError, TransportError) as e:
        _p({"error": {"code": getattr(e, "code", "TRANSPORT_ERROR"),
                      "message": e.message if hasattr(e, "message") else str(e)}})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
