# 路侧计算盒 · 地图镜像仓库（mapmirror）

零第三方依赖（Python 3.11 标准库 + 原生 HTML/JS），为路侧计算盒提供
"地图中心已生效快照 → 不可变清单 → 压缩对象块 → 设备验真暂存 → 原子换指针"
的镜像分发闭环。**对地图中心（上层 `engine.py` 服务）严格只读。**

## 一分钟跑通

```bash
cd /workspace/mapmirror
bash demo.sh              # 自动重置临时状态，走完发布/断网续传/竞争/切换/重启/中心零改动
python3 -m unittest discover -s tests    # 18 个自动化校验
```

手动：

```bash
python3 -m mm serve --store ./rstore --port 8090          # 仓库 + 网页 http://localhost:8090

# 发布端（中心可以是 mapstate.json 或 http(s):// 地图服务，均只读）
python3 -m mm center-list --center ../data/mapstate.json
python3 -m mm publish  --center ../data/mapstate.json \
    --registry http://127.0.0.1:8090 \
    --snapshot S-0001 --devices BOX-A,BOX-B --note "路侧首发"

# 设备端（盒子上）
python3 -m mm device pull     --device BOX-A --device-root ./dev-A \
    --registry http://127.0.0.1:8090 --manifest MF-xxxx   # 断网后重跑即续传
python3 -m mm device activate --device BOX-A --device-root ./dev-A \
    --registry http://127.0.0.1:8090 --manifest MF-xxxx   # 全部验真后才原子换指针
python3 -m mm device status   --device BOX-A --device-root ./dev-A \
    --registry http://127.0.0.1:8090

# 与网页完全同源的进度/拒绝理由查询
python3 -m mm tasks   --registry http://127.0.0.1:8090 [--device BOX-A]
python3 -m mm devices --registry http://127.0.0.1:8090
python3 -m mm show    --registry http://127.0.0.1:8090 --task T-xxxx
```

`--faults plan.json` 可在任意操作注入一次性故障（演示/测试用）：

```json
{"rules":[{"op":"get_block","block":"sha256:ab…","times":1,"fault":"unreachable"},
         {"op":"get_block","block":"sha256:cd…","times":9,"fault":"corrupt_bytes"}]}
```

## 需求 → 实现对照

| 需求 | 实现 |
|---|---|
| 选**已生效快照**+设备集合，生成**不可变清单** | `mapcenter.py` 只接受中心 active/completed/superseded 快照；`manifest.py` 的 `manifest_id = sha256(完整清单体)`，同 id 不同内容 → `MANIFEST_ID_MISMATCH/409`，载荷永不改写 |
| 压缩对象块 | 自定义 MMBL1 容器（gzip + 逐对象元数据/原始字节），块按目标大小切分，块内顺序固定（`common.pack_block`） |
| 校验**块散列**与**整包根散列**后暂存 | 下载即验块 sha256 与块内逐对象 sha256（`unpack_block`）；全部到齐后设备独立重算 `root_sha256=sha256(排序后的对象条目)`（`device._stage`），任何不符 `FAILED`，不进暂存/待切换槽 |
| 网络中断可继续拉取，**已确认块不重复计数** | 已确认块内容寻址缓存（`blobs/`，跨清单去重）；本地 blocks 表 `confirmed` 只置一次；`pull_progress` 事件每块恰好一次，计数单调 0→N |
| 多个发布竞争同一盒子，**只允许一个待切换镜像** | 设备本地 `STAGED` 待切换槽 + 两次仲裁（拉取前、暂存落位后/切换前）。同设备代次单调递增；较老代次 → `OLDER_GENERATION`；较新代次先占槽后旧代次 → `PENDING_SLOT_CONFLICT`；更新代次到达时旧待切换 `SUPERSEDED_BY_NEWER_GENERATION` 让位且永不可切换 |
| **较老代次或坏对象不能覆盖当前镜像** | activate 最后闸门复核 current 代次、待切换槽归属、镜像目录全部对象散列与根散列；坏对象 `FAILED`，activate 返回冻结失败态 |
| **所有对象验真后原子换指针**；切换前失败保持原状态 | `images/<MF>/` 建好并复核后才写 `CURRENT.json`，用临时 symlink + `rename(2)` 原子替换 `current`（含目录 fsync）；任何异常旧指针原样保留 |
| 重启后可辨别**拉取中/可切换/已切换及每次处理结果** | 设备 SQLite：tasks（NEW/PULLING/STAGED/SWITCHED/REJECTED/FAILED）+ blocks 确认位图 + attempts 全量留痕；当前镜像从 `current/CURRENT.json` 恢复（`device status`） |
| **网页与设备命令行同一数据源** | 都打仓库 `/api/tasks`、`/api/devices`、`/api/manifests`；拒绝码/拒绝原因/事件流同一份；仓库 SQLite 是唯一事实源 |
| **重试同一清单返回第一次任务和切换结果** | `task_id=sha256(manifest_id|device_id)` 且仓库 `(manifest,device)` 唯一；终态（SWITCHED/REJECTED/FAILED）冻结 `frozen_result`，重复结果事件记 `frozen_ignored` 不改变状态；设备终态本地直接幂等返回 |
| **不得改动地图中心状态和已创建行程快照** | 中心适配器只有 GET/只读文件；导出前逐瓦片复核内容散列，篡改 → `CENTER_TAMPERED` 拒绝出清单；测试对中心状态做字节级前后比对 |

## 验真链条

```
发布端                              设备
快照固定字段 ──► center_anchor
对象 canonical ─► oid.sha256 ─┐
对象条目排序 ──► root_sha256   │ 清单 manifest_id=sha256(清单体)
打包 gzip ──► block.sha256 ────┼────────► 下载复核 block.sha256
                              │          解包复核每个 oid.sha256
                              └────────► 齐活重算 root_sha256
                                         全过 → images/<MF>（对象再复核）
                                         → CURRENT.json → rename 原子换指针
```

设备**从不信任**仓库给的中间值：块散列、对象散列、根散列均在本地独立重算。

## API（均为 JSON；网页与 CLI 共用）

| 方法 | 路径 | 作用 |
|---|---|---|
| GET | `/api/devices` | 设备当前镜像 / 待切换槽 / 各代次任务（网页总览） |
| GET/POST | `/api/manifests` · `/api/manifests/{id}` | 清单列表 · 提交（201 新建/200 幂等；409 不可变冲突）· 详情 |
| PUT/GET | `/api/blocks/<sha>` | 上传压缩块（sha256 去重，422 散列不符）· 下载（服务端出库前复核） |
| GET | `/api/tasks?device=&manifest=` | 任务进度总览（与 CLI `tasks` 同源） |
| GET | `/api/tasks/{id}` · `.../events` | 任务（含 `frozen_result`）· append-only 每次处理结果 |
| POST | `/api/tasks/{id}/events` | 设备上报（`event_uid` 幂等；`pull_progress/pull_paused/stage_completed/switch_succeeded/rejected/failed`） |

**拒绝码**：`OLDER_GENERATION`、`PENDING_SLOT_CONFLICT`、
`SUPERSEDED_BY_NEWER_GENERATION`、`SUPERSEDED_BY_SWITCH`、
`BLOCK_HASH_MISMATCH`、`OBJECT_HASH_MISMATCH`、`ROOT_HASH_MISMATCH`、
`BLOCK_CORRUPT`、`MANIFEST_ID_MISMATCH`、`MANIFEST_IMMUTABLE`、
`CENTER_TAMPERED`、`SNAPSHOT_NOT_EFFECTIVE`。

## 存储布局

```
mapmirror/
  mm/
    common.py      规范化哈希(与 engine 一致) / MMBL1 块 / 原子写与原子 symlink
    mapcenter.py   地图中心只读适配器（Local 文件 / HTTP GET；导出前瓦片验真）
    manifest.py    不可变清单构造与 verify_manifest
    publisher.py   发布工作流（选快照+设备 → 传块 → 提交清单，幂等）
    registry.py    仓库：SQLite(manifests/blocks/tasks/task_events/devices/meta)
    server.py      标准库 HTTP：/api + /static
    transport.py   HTTP 传输 + 故障注入（断网/坏字节）
    device.py      设备代理：续传/验真/仲裁/暂存/原子换指针/重启恢复
    cli.py         统一命令行
  web/             控制台（index.html/app.js，只消费 /api，2s 轮询）
  tests/test_units.py   块/清单纯函数（10 项）
  tests/test_e2e.py     真实地图中心上端到端（8 项，含中心零改动字节校验）
  demo.sh          一键演示

rstore/registry.db        仓库事实源（WAL）；rstore/blobs/<ab>/<sha> 块库
<device-root>/agent.db    设备状态；blobs/ 块缓存；staging/ 暂存；images/ 镜像
<device-root>/current     原子替换的 symlink，指向当前运行镜像
```
