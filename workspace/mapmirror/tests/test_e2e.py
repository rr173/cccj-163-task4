"""端到端需求校验（零依赖，可重复运行）：

  1. 发布不可变清单：块去重、重复发布幂等、清单不可变
  2. 拉取断网续传：已确认块不重复下载/计数
  3. 坏对象（块散列不符）-> FAILED，绝不暂存/换指针
  4. 整包根散列必须通过才进待切换槽
  5. 多发布竞争：同盒只有一个待切换；较老代次不能覆盖当前镜像
  6. 原子换指针 + 切换前失败保持原状态；重试同一清单返回第一次切换结果
  7. 重启恢复：拉取中/可切换/已切换及每次 attempt 结果均可辨别
  8. 网页与设备 CLI 同一数据源
  9. 地图中心状态与已创建行程快照全程不被改动
"""
from __future__ import annotations

import json
import os
import shutil
import sys
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from mm.device import DeviceAgent
from mm.mapcenter import LocalMapCenter
from mm.publisher import publish
from mm.registry import Registry
from mm.server import RegistryServer
from mm.transport import FaultInject, HttpTransport, TransportError
from http.server import ThreadingHTTPServer

WORK = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".testwork")
CENTER_STATE = os.path.join(WORK, "mapstate.json")


def _build_center():
    """用既有 engine 造一个真实中心：发布一版地图 + 两条已固定行程快照。"""
    from engine import MapEngine
    os.makedirs(WORK, exist_ok=True)
    if os.path.exists(CENTER_STATE):
        os.unlink(CENTER_STATE)
    eng = MapEngine(CENTER_STATE)
    # 种子世界已有 north=1/south=1；直接为两台车固定行程快照
    eng.create_route({"vehicle_id": "VH-A", "from_node": "A00/w",
                      "to_node": "B01/e"})
    eng.create_route({"vehicle_id": "VH-B", "from_node": "A00/w",
                      "to_node": "B01/e"})
    return eng


class Harness:
    def __init__(self):
        self.reg_store = os.path.join(WORK, "registry")
        shutil.rmtree(self.reg_store, ignore_errors=True)
        self.srv = RegistryServer(self.reg_store)
        self.httpd: ThreadingHTTPServer = self.srv.serve("127.0.0.1", 0)
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.port}"

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.srv.registry.close()

    def tr(self):
        return HttpTransport(self.url)

    def agent(self, dev, root, plan=None):
        tr = FaultInject(HttpTransport(self.url), plan) if plan \
            else HttpTransport(self.url)
        return DeviceAgent(dev, root, self.url, transport=tr)

    def blob_path(self, digest):
        h = digest.removeprefix("sha256:")
        return os.path.join(self.reg_store, "blobs", h[:2], h)


class MapMirrorE2E(unittest.TestCase):
    h: Harness

    @classmethod
    def setUpClass(cls):
        shutil.rmtree(WORK, ignore_errors=True)
        cls.eng = _build_center()
        cls.snaps = list(cls.eng.state["snapshots"].keys())
        cls.s1, cls.s2 = cls.snaps[0], cls.snaps[1]
        cls.center_before = open(CENTER_STATE, "rb").read()
        cls.h = Harness()

    @classmethod
    def tearDownClass(cls):
        cls.h.stop()
        center_after = open(CENTER_STATE, "rb").read()
        # 需求：系统不得改动地图中心状态和已创建行程快照
        assert center_after == cls.center_before, "地图中心状态被改动！"

    def setUp(self):
        self.center = LocalMapCenter(CENTER_STATE)

    def box(self, name):
        root = os.path.join(WORK, name)
        shutil.rmtree(root, ignore_errors=True)
        return root

    # ------------------------------------------------- 1 发布/不可变/去重 --
    def test_01_publish_immutable_and_dedup(self):
        r1 = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                     devices=["BOX-A"], note="第一次发布", block_size=4096)
        self.assertFalse(r1["idempotent"])
        self.mf1 = r1["manifest_id"]
        self.assertGreaterEqual(r1["blocks_total"], 1)
        # 块内容寻址：第二次发布完全相同内容 -> 同 manifest，块全部复用
        r2 = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                     devices=["BOX-A"], note="第一次发布", block_size=4096)
        self.assertTrue(r2["idempotent"])
        self.assertEqual(r2["manifest_id"], self.mf1)
        self.assertEqual(r2["blocks_reused"], r1["blocks_total"])
        # 同快照不同设备集合 -> 新清单；BOX-A 的任务保持第一次结果
        r3 = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                     devices=["BOX-A", "BOX-B"], note="扩盒子", block_size=4096)
        self.assertNotEqual(r3["manifest_id"], self.mf1)
        tasks_a = [t for t in r3["tasks"] if t["device_id"] == "BOX-A"]
        self.assertEqual(len(tasks_a), 1)
        # 清单载荷不可变：直接篡改 DB 中 payload 的尝试应被 API 侧 id 校验覆盖；
        # 这里验证同 id 不同内容提交被 409 拒绝
        manifest = self.h.tr().get_json(f"/api/manifests/{self.mf1}")
        manifest["note"] = "被篡改"
        try:
            self.h.tr().post_json("/api/manifests", {"manifest": manifest})
            self.fail("应拒绝不可变清单改写")
        except TransportError as e:
            self.assertEqual(e.code, "MANIFEST_ID_MISMATCH")

    # ------------------------------------------------------- 2 断网续传 -----
    def test_02_resume_after_outage(self):
        pub = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                      devices=["BOX-C"], block_size=4096)
        mf, total = pub["manifest_id"], pub["blocks_total"]
        manifest = self.h.tr().get_json(f"/api/manifests/{mf}")
        first_block = manifest["blocks"][0]["sha256"]
        plan = {"rules": [{"op": "get_block", "block": first_block,
                           "times": 1, "fault": "unreachable"}]}
        root = self.box("BOX-C")
        ag = self.h.agent("BOX-C", root, plan)
        out = ag.process(mf)
        self.assertEqual(out["status"], "PULLING")
        self.assertTrue(out["paused"])
        self.assertEqual(out["confirmed_blocks"], 0)

        # 重启一个全新代理实例（相同磁盘状态），无故障继续
        ag2 = self.h.agent("BOX-C", root)
        out2 = ag2.process(mf)
        self.assertEqual(out2["status"], "STAGED")
        # 已确认块不重复计数：服务器确认计数恰好等于块总数，事件流中每块只计一次
        task = self.h.tr().get_json(
            f"/api/tasks/{out2['task_id']}")
        self.assertEqual(task["confirmed_blocks"], total)
        progress_events = [e for e in
                           self.h.tr().get_json(
                               f"/api/tasks/{out2['task_id']}/events")["events"]
                           if e["event"] == "pull_progress"]
        max_confirmed = max(e["confirmed_blocks"] for e in progress_events)
        self.assertEqual(max_confirmed, total)
        self.assertEqual(len(progress_events), total,
                         "每个块只能被确认计数一次")
        # 再次 process：STAGED 直接幂等返回，不重复下载
        out3 = ag2.process(mf)
        self.assertTrue(out3.get("idempotent"))
        self.assertEqual(out3["status"], "STAGED")

    # ------------------------------------------------- 3 坏对象被挡住 --------
    def test_03_bad_object_rejected(self):
        pub = publish(self.center, self.h.tr(), snapshot_id=self.s2,
                      devices=["BOX-D"], block_size=4096)
        mf = pub["manifest_id"]
        manifest = self.h.tr().get_json(f"/api/manifests/{mf}")
        bad_block = manifest["blocks"][0]["sha256"]
        plan = {"rules": [{"op": "get_block", "block": bad_block,
                           "times": 10, "fault": "corrupt_bytes"}]}
        root = self.box("BOX-D")
        ag = self.h.agent("BOX-D", root, plan)
        out = ag.process(mf)
        self.assertEqual(out["status"], "FAILED")
        self.assertEqual(out["reason_code"], "BLOCK_HASH_MISMATCH")
        # 坏字节绝不进本地缓存
        self.assertFalse(os.path.exists(
            os.path.join(root, "blobs", bad_block.removeprefix("sha256:")[:2],
                         bad_block.removeprefix("sha256:"))))
        # 坏对象任务不能切换（保持 FAILED 冻结结果）
        act = ag.activate(mf)
        self.assertEqual(act["status"], "FAILED")
        self.assertEqual(act["reason_code"], "BLOCK_HASH_MISMATCH")
        # 仓库任务同态（网页/CLI 同源可见拒绝理由）
        task = self.h.tr().get_json(f"/api/tasks/{out['task_id']}")
        self.assertEqual(task["status"], "FAILED")
        self.assertEqual(task["reason_code"], "BLOCK_HASH_MISMATCH")
        # 重试同一清单返回第一次失败结果，状态不复活
        out2 = self.h.agent("BOX-D", root).process(mf)
        self.assertTrue(out2["idempotent"])
        self.assertEqual(out2["status"], "FAILED")

    # ------------------------------ 4/5/6 竞争、原子换指针、幂等切换 ---------
    def test_04_contention_and_atomic_switch(self):
        # 发布 gen1(s1) 与 gen2(s2) 给同一盒子 BOX-E
        p1 = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                     devices=["BOX-E"], block_size=4096)
        p2 = publish(self.center, self.h.tr(), snapshot_id=self.s2,
                     devices=["BOX-E"], block_size=4096)
        g1, g2 = p1["manifest_id"], p2["manifest_id"]
        root = self.box("BOX-E")

        # gen2 先暂存，占住唯一待切换槽
        a = self.h.agent("BOX-E", root)
        self.assertEqual(a.process(g2)["status"], "STAGED")
        # gen1（较老代次）后到 -> REJECTED，不能顶掉较新待切换
        out_old = a.process(g1)
        self.assertEqual(out_old["status"], "REJECTED")
        self.assertEqual(out_old["reason_code"], "PENDING_SLOT_CONFLICT")
        self.assertTrue(os.path.islink(os.path.join(root, "current")) is False)

        # gen2 原子切换成功
        sw = a.activate(g2)
        self.assertEqual(sw["status"], "SWITCHED")
        link = os.readlink(os.path.join(root, "current"))
        self.assertIn(g2, link)

        # gen1 再次尝试：较老代次不能覆盖当前镜像（activate 闸门）
        # 其任务已 REJECTED；activate 返回冻结的拒绝
        again = a.activate(g1)
        self.assertEqual(again["status"], "REJECTED")
        # 当前指针纹丝不动
        self.assertIn(g2, os.readlink(os.path.join(root, "current")))

        # 重试切换 gen2 -> 返回第一次切换结果，不重复换指针
        sw2 = a.activate(g2)
        self.assertTrue(sw2["idempotent"])
        self.assertEqual(sw2["status"], "SWITCHED")

    # ----------------------- 7 新代次顶替旧待切换槽 --------------------------
    def test_05_newer_generation_takes_pending_slot(self):
        p1 = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                     devices=["BOX-F"], block_size=4096)
        root = self.box("BOX-F")
        a = self.h.agent("BOX-F", root)
        self.assertEqual(a.process(p1["manifest_id"])["status"], "STAGED")
        # 更新发布到达：旧待切换让位（拒绝，不可再切换），新的占槽
        p2 = publish(self.center, self.h.tr(), snapshot_id=self.s2,
                     devices=["BOX-F"], block_size=4096)
        out = a.process(p2["manifest_id"])
        self.assertEqual(out["status"], "STAGED")
        st = a.status()
        old = [t for t in st["tasks"] if t["manifest_id"] == p1["manifest_id"]][0]
        self.assertEqual(old["status"], "REJECTED")
        self.assertEqual(old["reason_code"], "SUPERSEDED_BY_NEWER_GENERATION")
        # 旧清单 activate 无法成功
        old_act = a.activate(p1["manifest_id"])
        self.assertEqual(old_act["status"], "REJECTED")
        self.assertEqual(a.activate(p2["manifest_id"])["status"], "SWITCHED")

    # ----------------------------- 8 重启恢复 + 每次结果 ----------------------
    def test_06_restart_recovery_and_attempts(self):
        p = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                    devices=["BOX-G"], block_size=4096)
        mf = p["manifest_id"]
        root = self.box("BOX-G")
        manifest = self.h.tr().get_json(f"/api/manifests/{mf}")
        first_block = manifest["blocks"][0]["sha256"]
        plan = {"rules": [{"op": "get_block", "block": first_block,
                           "times": 1, "fault": "unreachable"}]}
        a1 = self.h.agent("BOX-G", root, plan)
        a1.process(mf)  # 拉取中断
        # 新实例重启：必须能辨别"拉取中"并看到第一次 attempt
        a2 = self.h.agent("BOX-G", root)
        st = a2.status()
        t = st["tasks"][0]
        self.assertEqual(t["status"], "PULLING")
        self.assertGreaterEqual(t["attempt"], 1)
        self.assertTrue(any(x["result"] == "paused" for x in t["attempts"]))
        a2.process(mf)
        a2.activate(mf)
        # 再重启：当前镜像 + SWITCHED + 完整 attempts 历史都在
        a3 = self.h.agent("BOX-G", root)
        st3 = a3.status()
        self.assertEqual(st3["current"]["manifest_id"], mf)
        self.assertEqual(st3["tasks"][0]["status"], "SWITCHED")
        phases = [x["phase"] + ":" + x["result"] for x in st3["tasks"][0]["attempts"]]
        self.assertTrue(any("pull:paused" == p for p in phases))
        self.assertTrue(any("switch:switched" == p for p in phases))

    # --------------------------------- 9 网页/CLI 同源 ------------------------
    def test_07_web_and_cli_same_source(self):
        # 设备视图与 /api/tasks 是同一份事实
        p = publish(self.center, self.h.tr(), snapshot_id=self.s1,
                    devices=["BOX-H"], block_size=4096)
        mf = p["manifest_id"]
        root = self.box("BOX-H")
        a = self.h.agent("BOX-H", root)
        a.process(mf)
        a.activate(mf)
        local = a.status()
        remote_devices = {d["device_id"]: d
                          for d in self.h.tr().get_json("/api/devices")["devices"]}
        d = remote_devices["BOX-H"]
        self.assertEqual(d["current_manifest_id"], mf)
        server_task = d["tasks"][0]
        local_task = [t for t in local["tasks"]
                      if t["manifest_id"] == mf][0]
        self.assertEqual(server_task["status"], local_task["status"])
        self.assertEqual(d["pending_task_id"], "")
        # 拒绝理由也同源：BOX-D 的坏对象任务
        dd = remote_devices["BOX-D"]["tasks"][0]
        self.assertEqual(dd["reason_code"], "BLOCK_HASH_MISMATCH")

    # ---------------------------- 10 仓库侧坏块自愈式拒绝 ---------------------
    def test_08_registry_corrupt_block_served_detected(self):
        # 先在一个健康盒子拉到完整缓存（充当运营侧重新推送的正确字节来源）
        donor_pub = publish(self.center, self.h.tr(), snapshot_id=self.s2,
                            devices=["BOX-I-DONOR"], block_size=4096)
        donor_root = self.box("BOX-I-DONOR")
        da = self.h.agent("BOX-I-DONOR", donor_root)
        da.process(donor_pub["manifest_id"])

        p = publish(self.center, self.h.tr(), snapshot_id=self.s2,
                    devices=["BOX-I"], block_size=4096)
        mf = p["manifest_id"]
        manifest = self.h.tr().get_json(f"/api/manifests/{mf}")
        target = manifest["blocks"][-1]["sha256"]
        # 模拟磁盘上仓库块被篡改（服务端读取会复核散列 -> BLOCK_CORRUPT）
        path = self.h.blob_path(target)
        data = bytearray(open(path, "rb").read())
        data[10] ^= 0xFF
        open(path + ".bad", "wb").write(bytes(data))
        os.replace(path + ".bad", path)
        root = self.box("BOX-I")
        a = self.h.agent("BOX-I", root)
        out = a.process(mf)
        self.assertEqual(out["status"], "PULLING")  # 当作不可达，可续传
        # 运营侧重新推送正确字节后，断点续传成功
        cached = os.path.join(donor_root, "blobs",
                              target.removeprefix("sha256:")[:2],
                              target.removeprefix("sha256:"))
        self.assertTrue(os.path.exists(cached))
        Registry(self.h.reg_store).put_block(
            target, open(cached, "rb").read())
        out2 = self.h.agent("BOX-I", root).process(mf)
        self.assertEqual(out2["status"], "STAGED")


if __name__ == "__main__":
    unittest.main(verbosity=2)
