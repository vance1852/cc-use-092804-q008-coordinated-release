"""无第三方依赖的协同发布 HTTP JSON 接口。"""

from __future__ import annotations

import argparse
import json
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

from .errors import ReleaseError, ValidationFailed
from .service import ReleaseCoordinator
from .storage import connect


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: Mapping[str, Any]


class JsonApplication:
    """JSON 应用。

    生产部署传入 ``service_factory``：ThreadingHTTPServer 每个请求在工作线程
    处理，线程各自持有独立 SQLite 连接，避免多线程共用连接造成事务交错。
    测试可直接传入单例 service（同线程串行调用）。
    """

    def __init__(
        self,
        service: ReleaseCoordinator | None = None,
        *,
        service_factory: Callable[[], ReleaseCoordinator] | None = None,
    ) -> None:
        if service is None and service_factory is None:
            raise ValueError("必须提供 service 或 service_factory")
        self._service = service
        self._service_factory = service_factory
        self._local = threading.local()

    @property
    def service(self) -> ReleaseCoordinator:
        if self._service_factory is None:
            assert self._service is not None
            return self._service
        current = getattr(self._local, "service", None)
        if current is None:
            current = self._service_factory()
            self._local.service = current
        return current

    @staticmethod
    def _actor(headers: Mapping[str, str]) -> str:
        actor = headers.get("x-actor-id", "").strip()
        if not actor:
            raise ValidationFailed("缺少 X-Actor-Id")
        return actor

    @staticmethod
    def _json(body: bytes) -> dict[str, Any]:
        if not body:
            return {}
        try:
            value = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValidationFailed("请求体必须是 UTF-8 JSON 对象") from exc
        if not isinstance(value, dict):
            raise ValidationFailed("请求体必须是 JSON 对象")
        return value

    def handle(self, method: str, target: str, headers: Mapping[str, str] | None = None, body: bytes = b"") -> Response:
        normalized = {key.lower(): value for key, value in (headers or {}).items()}
        path = urlparse(target).path.rstrip("/") or "/"
        parts = [part for part in path.split("/") if part]
        try:
            if method == "GET" and path == "/health":
                return Response(200, {"status": "ok"})
            payload = self._json(body) if method in {"POST", "PUT", "PATCH"} else {}
            actor = self._actor(normalized)

            if method == "POST" and path == "/users":
                return Response(201, self.service.create_user(
                    payload["user_id"], payload["display_name"], payload["role"]))

            if method == "POST" and path == "/batches":
                return Response(201, self.service.create_batch(
                    actor, payload["batch_id"], payload["machine_model"], payload.get("note", "")))

            if method == "POST" and path == "/devices":
                return Response(201, self.service.register_device(
                    actor, payload["device_serial"], payload["batch_id"], payload.get("installed_combination")))
            if method == "GET" and len(parts) == 2 and parts[0] == "devices":
                return Response(200, self.service.device_status(parts[1]))

            if method == "POST" and len(parts) == 3 and parts[0] == "candidates" and parts[2] == "baseline":
                return Response(201, self.service.register_baseline(actor, parts[1], payload))
            if method == "POST" and len(parts) == 3 and parts[0] == "candidates" and parts[2] == "revisions":
                return Response(201, self.service.create_candidate(actor, parts[1], payload))
            if method == "GET" and len(parts) == 4 and parts[0] == "candidates" and parts[2] == "revisions":
                return Response(200, self.service.candidate(parts[1], int(parts[3])))

            if method == "POST" and len(parts) == 5 and parts[0] == "candidates" \
                    and parts[2] == "revisions" and parts[4] == "waves":
                kind = payload.get("kind", "pilot")
                opener = self.service.open_pilot_wave if kind == "pilot" else self.service.open_expansion_wave
                return Response(201, opener(
                    actor, payload["wave_id"], parts[1], int(parts[3]),
                    payload["batch_id"], payload["idempotency_key"], payload.get("devices")))

            if method == "POST" and len(parts) == 5 and parts[0] == "candidates" \
                    and parts[2] == "revisions" and parts[4] in ("control", "ai"):
                return Response(200, self.service.approve(
                    actor, parts[1], int(parts[3]), parts[4], payload.get("note", "")))

            if method == "POST" and len(parts) == 3 and parts[0] == "waves" and parts[2] == "receipts":
                return Response(201, self.service.record_receipt(
                    actor, parts[1], payload["device_serial"], payload["step_id"],
                    payload["status"], payload["summary"], payload.get("idempotency_key", "")))
            if method == "GET" and len(parts) == 2 and parts[0] == "waves":
                return Response(200, self.service.wave_report(parts[1]))

            if method == "POST" and len(parts) == 3 and parts[0] == "devices" and parts[2] == "rollback":
                return Response(200, self.service.rollback_device(
                    actor, parts[1], payload["target_candidate_id"], int(payload["target_revision"]),
                    payload["reason"], payload["idempotency_key"]))
            if method == "POST" and len(parts) == 3 and parts[0] == "field-actions" and parts[2] == "complete":
                return Response(200, self.service.complete_field_action(actor, int(parts[1])))

            if method == "GET" and path == "/audit/chain":
                return Response(200, self.service.audit_chain(actor))

            return Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})
        except ReleaseError as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})


def make_handler(application: JsonApplication):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ReleaseGate/1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            encoded = json.dumps(response.body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="启动控制与 AI 协同发布门禁服务")
    parser.add_argument("--database", type=Path, default=Path("release-coordination.sqlite3"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8083)
    args = parser.parse_args(argv)
    database = args.database

    def service_factory() -> ReleaseCoordinator:
        return ReleaseCoordinator(connect(database))

    server = ThreadingHTTPServer(
        (args.host, args.port), make_handler(JsonApplication(service_factory=service_factory))
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
