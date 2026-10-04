"""镜像仓库 HTTP 服务（标准库，零依赖）。

网页 (/ 与 /static/*) 与设备命令行查询任务进度/拒绝理由时，
访问的是同一套 /api 接口、同一个 registry 数据源。
"""
from __future__ import annotations

import json
import os
import re
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .registry import Registry, RegistryError

RE_MANIFEST_ITEM = re.compile(r"^/api/manifests/([^/]+)$")
RE_TASK_ITEM = re.compile(r"^/api/tasks/([^/]+)$")
RE_TASK_EVENTS = re.compile(r"^/api/tasks/([^/]+)/events$")
RE_BLOCK_ITEM = re.compile(r"^/api/blocks/([^/]+)$")

STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "web")

_MIME = {".html": "text/html; charset=utf-8",
         ".js": "application/javascript; charset=utf-8",
         ".css": "text/css; charset=utf-8",
         ".json": "application/json; charset=utf-8"}


class RegistryServer:
    def __init__(self, store: str):
        self.registry = Registry(store)

    def make_handler(self):
        reg = self.registry

        class Handler(BaseHTTPRequestHandler):
            server_version = "mapmirror/1"

            def log_message(self, fmt, *args):
                pass

            def _json(self, obj, status: int = 200):
                body = json.dumps(obj, ensure_ascii=False).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _err(self, e: RegistryError):
                self._json({"error": {"code": e.code, "message": e.message}},
                           e.status)

            def _read(self) -> bytes:
                n = int(self.headers.get("Content-Length", 0))
                return self.rfile.read(n) if n else b""

            def _read_json(self) -> dict:
                try:
                    return json.loads(self._read().decode() or "{}")
                except json.JSONDecodeError:
                    raise RegistryError("BAD_JSON", "请求体不是合法 JSON", 400)

            # ---------------------------------------------------------- GET --
            def do_GET(self):
                parsed = urllib.parse.urlparse(self.path)
                p = urllib.parse.unquote(parsed.path)
                q = urllib.parse.parse_qs(parsed.query)
                try:
                    if p == "/api/devices":
                        return self._json({"devices": reg.list_devices()})
                    if p == "/api/manifests":
                        return self._json({"manifests": reg.list_manifests()})
                    if RE_MANIFEST_ITEM.match(p):
                        mid = RE_MANIFEST_ITEM.match(p).group(1)
                        return self._json(reg.get_manifest(mid))
                    m = RE_TASK_EVENTS.match(p)
                    if m:
                        tid = m.group(1)
                        return self._json(
                            {"task": reg.get_task(tid),
                             "events": reg.list_events(tid)})
                    m = RE_TASK_ITEM.match(p)
                    if m:
                        return self._json(reg.get_task(m.group(1)))
                    if p == "/api/tasks":
                        return self._json({"tasks": reg.list_tasks(
                            device=(q.get("device") or [None])[0],
                            manifest=(q.get("manifest") or [None])[0])})
                    m = RE_BLOCK_ITEM.match(p)
                    if m:
                        digest = "sha256:" + m.group(1)
                        data = reg.get_block(digest)
                        self.send_response(200)
                        self.send_header("Content-Type", "application/octet-stream")
                        self.send_header("Content-Length", str(len(data)))
                        self.send_header("X-Block-Sha256", digest)
                        self.end_headers()
                        return self.wfile.write(data)
                    if p in ("/", "/index.html"):
                        return self._static("index.html")
                    if p.startswith("/static/"):
                        return self._static(p[len("/static/"):])
                    return self._json({"error": {"code": "NOT_FOUND",
                                                 "message": p}}, 404)
                except RegistryError as e:
                    return self._err(e)

            def _static(self, name: str):
                # 防路径穿越
                name = os.path.normpath(name).lstrip("/")
                full = os.path.join(STATIC_DIR, name)
                if not full.startswith(STATIC_DIR + os.sep) or not os.path.isfile(full):
                    return self._json({"error": {"code": "NOT_FOUND",
                                                 "message": name}}, 404)
                with open(full, "rb") as f:
                    data = f.read()
                ext = os.path.splitext(name)[1]
                self.send_response(200)
                self.send_header("Content-Type",
                                 _MIME.get(ext, "application/octet-stream"))
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            # --------------------------------------------------------- POST --
            def do_POST(self):
                p = urllib.parse.urlparse(self.path).path
                try:
                    if p == "/api/manifests":
                        body = self._read_json()
                        manifest = body.get("manifest")
                        if not isinstance(manifest, dict):
                            raise RegistryError("BAD_REQUEST",
                                                "需要 {manifest: {...}}", 400)
                        result = reg.put_manifest(
                            manifest, created_by=str(body.get("created_by", "")))
                        return self._json(result,
                                          200 if result["idempotent"] else 201)
                    m = re.match(r"^/api/tasks/([^/]+)/events$", p)
                    if m:
                        ev = self._read_json()
                        result = reg.report_event(m.group(1), ev)
                        return self._json(result, 200)
                    return self._json({"error": {"code": "NOT_FOUND",
                                                 "message": p}}, 404)
                except RegistryError as e:
                    return self._err(e)

            # ---------------------------------------------------------- PUT --
            def do_PUT(self):
                p = urllib.parse.urlparse(self.path).path
                try:
                    if p.startswith("/api/blocks/"):
                        digest = "sha256:" + p.rsplit("/", 1)[1]
                        result = reg.put_block(digest, self._read())
                        return self._json(result,
                                          200 if result["idempotent"] else 201)
                    return self._json({"error": {"code": "NOT_FOUND",
                                                 "message": p}}, 404)
                except RegistryError as e:
                    return self._err(e)

        return Handler

    def serve(self, host: str, port: int) -> ThreadingHTTPServer:
        httpd = ThreadingHTTPServer((host, port), self.make_handler())
        return httpd


def run(store: str, host: str, port: int):
    srv = RegistryServer(store)
    httpd = srv.serve(host, port)
    print(f"mapmirror registry on http://{host}:{port}  (store={store})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        srv.registry.close()
