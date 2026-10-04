"""HTTP 传输层（标准库 urllib），设备代理与发布端共用。

FaultInject 包装任意 Transport，用于真实模拟:
  * unreachable      —— 网络中断（可在第 N 次拉块时发生，之后恢复，验证断点续传）
  * corrupt_bytes    —— 仓库/链路被篡改（坏对象必须被块散列挡住）
  * slow 仅演示用

故障计划文件是简单 JSON，例如：
  {"rules": [{"op": "get_block", "block": "sha256:ab..", "times": 1,
              "fault": "unreachable"},
             {"op": "get_block", "block": "sha256:cd..", "times": 1,
              "fault": "corrupt_bytes"}]}
"""
from __future__ import annotations

import json
import urllib.error
import urllib.request


class TransportError(Exception):
    def __init__(self, code: str, message: str, status: int = 0):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


class HttpTransport:
    def __init__(self, base_url: str, timeout: float = 15.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str,
                 data: bytes | None = None,
                 ctype: str = "application/json"):
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": ctype} if data is not None else {})
        try:
            return urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            try:
                payload = json.loads(body)
                err = payload.get("error", {})
                raise TransportError(err.get("code", f"HTTP_{e.code}"),
                                     err.get("message", body), e.code)
            except (json.JSONDecodeError, ValueError):
                raise TransportError(f"HTTP_{e.code}", body, e.code)
        except (urllib.error.URLError, OSError, TimeoutError) as e:
            raise TransportError("REGISTRY_UNREACHABLE", f"仓库不可达：{e}")

    def get_json(self, path: str) -> dict:
        with self._request("GET", path) as r:
            return json.loads(r.read().decode())

    def post_json(self, path: str, obj: dict) -> tuple[dict, int]:
        data = json.dumps(obj, ensure_ascii=False).encode()
        with self._request("POST", path, data) as r:
            return json.loads(r.read().decode()), r.status

    def get_block(self, digest: str) -> bytes:
        with self._request("GET", f"/api/blocks/{digest.removeprefix('sha256:')}") as r:
            return r.read()

    def put_block(self, digest: str, data: bytes) -> dict:
        with self._request("PUT",
                           f"/api/blocks/{digest.removeprefix('sha256:')}",
                           data, "application/octet-stream") as r:
            return json.loads(r.read().decode())


class FaultInject:
    """按计划在指定操作上制造一次性故障（用于演示/测试，不影响正常路径）。"""

    def __init__(self, inner: HttpTransport, plan: dict | None = None):
        self.inner = inner
        self.rules = list((plan or {}).get("rules", []))
        self._counts: dict[int, int] = {}

    def _maybe_fault(self, op: str, block: str = ""):
        for i, rule in enumerate(self.rules):
            if rule.get("op") != op:
                continue
            if block and rule.get("block") and rule["block"] != block:
                continue
            n = self._counts.get(i, 0)
            if n >= int(rule.get("times", 1)):
                continue
            self._counts[i] = n + 1
            f = rule["fault"]
            if f == "unreachable":
                raise TransportError("REGISTRY_UNREACHABLE",
                                     "模拟网络中断（故障计划 unreachable）")
            if f == "corrupt_bytes":
                return "corrupt_bytes"
        return None

    def get_json(self, path: str) -> dict:
        self._maybe_fault("get_json")
        return self.inner.get_json(path)

    def post_json(self, path: str, obj: dict):
        self._maybe_fault("post_json")
        return self.inner.post_json(path, obj)

    def put_block(self, digest: str, data: bytes) -> dict:
        self._maybe_fault("put_block", digest)
        return self.inner.put_block(digest, data)

    def get_block(self, digest: str) -> bytes:
        fault = self._maybe_fault("get_block", digest)
        data = self.inner.get_block(digest)
        if fault == "corrupt_bytes":
            # 翻转若干字节，块散列校验必须失败（坏对象绝不能暂存/换指针）
            b = bytearray(data)
            for i in range(min(8, len(b))):
                b[i] ^= 0xFF
            return bytes(b)
        return data
