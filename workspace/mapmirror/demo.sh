#!/usr/bin/env bash
# 路侧地图镜像仓库一键演示（零依赖；自动重置状态，结束后校验地图中心未被改动）。
set -euo pipefail
cd "$(dirname "$0")"
WORK=$(mktemp -d)
PORT=${1:-8099}
REG="http://127.0.0.1:$PORT"
trap 'kill $SRV_PID 2>/dev/null || true; rm -rf "$WORK"' EXIT

say(){ printf "\n\033[1;36m== %s ==\033[0m\n" "$*"; }
ok(){ printf "\033[1;32m%s\033[0m\n" "$*"; }

# ---- 0) 造一个真实地图中心（既有高精地图服务；本演示全程对它只读）----
say "0. 初始化地图中心（创建 2 条已生效行程快照）"
python3 - "$WORK/center.json" <<'PY'
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..")))
from engine import MapEngine
e = MapEngine(sys.argv[1])
e.create_route({"vehicle_id":"VH-A","from_node":"A00/w","to_node":"B01/e"})
e.create_route({"vehicle_id":"VH-B","from_node":"A00/w","to_node":"B01/e"})
print("中心快照:", list(e.state["snapshots"]))
PY
cp "$WORK/center.json" "$WORK/center.before"

# ---- 1) 启动仓库 ----
say "1. 启动镜像仓库"
python3 -m mm serve --store "$WORK/registry" --port "$PORT" >"$WORK/srv.log" 2>&1 &
SRV_PID=$!
for i in $(seq 1 50); do curl -sf "$REG/api/devices" >/dev/null && break; sleep 0.1; done
ok "仓库已启动：$REG （网页同源）"

# ---- 2) 发布不可变清单 ----
say "2. 发布：选已生效快照 S-0001 + 设备集合 BOX-A,BOX-B"
PUB=$(python3 -m mm publish --center "$WORK/center.json" --registry "$REG" \
  --snapshot S-0001 --devices BOX-A,BOX-B --block-size 4096 --note "首发镜像")
MF=$(printf '%s' "$PUB" | python3 -c "import json,sys;print(json.load(sys.stdin)['manifest_id'])")
ok "清单 $MF（manifest_id 由全部内容散列决定，载荷不可变）"
say "2b. 重试完全相同的发布 -> 同 id，块全部复用"
python3 -m mm publish --center "$WORK/center.json" --registry "$REG" \
  --snapshot S-0001 --devices BOX-A,BOX-B --block-size 4096 --note "首发镜像" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('idempotent=',d['idempotent'],'复用块=',d['blocks_reused'])"

# ---- 3) 断网续传 ----
say "3. BOX-A 拉取中断网（第一块第一次请求失败）"
B0=$(python3 -c "import json,urllib.request as u;m=json.load(u.urlopen('$REG/api/manifests/$MF'));print(m['blocks'][0]['sha256'])")
echo "{\"rules\":[{\"op\":\"get_block\",\"block\":\"$B0\",\"times\":1,\"fault\":\"unreachable\"}]}" > "$WORK/fault.json"
python3 -m mm device pull --device BOX-A --device-root "$WORK/boxA" \
  --registry "$REG" --manifest "$MF" --faults "$WORK/fault.json" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('状态',d['status'],'进度',d.get('confirmed_blocks'),'/',d.get('total_blocks'),'(已确认块保留)')"
say "3b. 网络恢复后续传（已确认块不重复下载/计数），根散列校验通过 -> 可切换"
python3 -m mm device pull --device BOX-A --device-root "$WORK/boxA" \
  --registry "$REG" --manifest "$MF" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('状态',d['status'],'进度',d['confirmed_blocks'],'/',d['total_blocks'])"

# ---- 4) 多发布竞争 + 原子换指针 ----
say "4. 发布代次 2（快照 S-0002）到 BOX-A、BOX-B"
MF2=$(python3 -m mm publish --center "$WORK/center.json" --registry "$REG" \
  --snapshot S-0002 --devices BOX-A,BOX-B --block-size 4096 \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['manifest_id'])")
python3 -m mm device pull --device BOX-A --device-root "$WORK/boxA" \
  --registry "$REG" --manifest "$MF2" >/dev/null
python3 -m mm device activate --device BOX-A --device-root "$WORK/boxA" \
  --registry "$REG" --manifest "$MF2" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('BOX-A 原子换指针：',d.get('previous'),'->',d['manifest_id'],'gen',d['generation'])"

say "4b. BOX-B：代次2先占待切换槽，代次1后到被拒（一个盒子只有一个待切换镜像）"
python3 -m mm device pull --device BOX-B --device-root "$WORK/boxB" \
  --registry "$REG" --manifest "$MF2" | python3 -c "import json,sys;print('gen2:',json.load(sys.stdin)['status'])"
python3 -m mm device pull --device BOX-B --device-root "$WORK/boxB" \
  --registry "$REG" --manifest "$MF" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print('gen1:',d['status'],'-',d['reason_code'])"

say "4c. 重试切换同一清单 -> 返回第一次切换结果（不重复换指针）"
python3 -m mm device activate --device BOX-A --device-root "$WORK/boxA" \
  --registry "$REG" --manifest "$MF2" \
  | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['status'],'idempotent=',d['idempotent'])"

# ---- 5) 重启可辨别 ----
say "5. 重启后设备状态（当前镜像 / 每个任务的态 / 每次处理结果）"
python3 -m mm device status --device BOX-A --device-root "$WORK/boxA" \
  --registry "$REG" --manifest "$MF" \
  | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('当前镜像:', d['current']['manifest_id'], 'gen', d['current']['generation'])
for t in d['tasks']:
    print(f\"  gen{t['generation']} {t['manifest_id']} {t['status']} 尝试{t['attempt']} 结果{len(t['attempts'])}条\")"

# ---- 6) 网页与设备 CLI 同一数据源 ----
say "6. 仓库事实源（网页 / 设备 CLI 都读 /api）：BOX-B 拒绝理由"
python3 -m mm tasks --registry "$REG" --device BOX-B | python3 -c "
import json,sys
for t in json.load(sys.stdin)['tasks']:
    print(f\"  gen{t['generation']} {t['manifest_id']} {t['status']} {t['reason_code']}\")"

# ---- 7) 地图中心与行程快照必须原封不动 ----
say "7. 校验地图中心状态/已创建行程快照未被镜像系统改动"
if cmp -s "$WORK/center.before" "$WORK/center.json"; then
  ok "地图中心字节级一致（镜像仓库全程只读中心）"
else
  echo "中心被改动！"; exit 1
fi
say "完成。网页查看：$REG （任务行可展开看每次处理结果）"
