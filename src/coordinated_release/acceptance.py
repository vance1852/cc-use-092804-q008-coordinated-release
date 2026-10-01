"""贯通发布候选、样机验收、独立批准、分阶段推广和失败回退的离线验收。"""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from .clock import FrozenClock
from .service import ReleaseService


def _artifact(version: str, seed: str) -> dict[str, str]:
    return {"version": version, "sha256": (seed * 64)[:64]}


def _candidate_payload(
    candidate_id: str,
    idem_key: str,
    *,
    control_os: tuple[str, str],
    ai_model: tuple[str, str],
    rollback_control_os: str,
    rollback_ai_model: str,
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "release_line": "delivery-2026q4",
        "idempotency_key": idem_key,
        "prototype_batch": "proto-01",
        "artifacts": {
            "control_os": _artifact(*control_os),
            "network_config": _artifact("1.4.0", "f"),
            "toolchain": _artifact("2.0.1", "0"),
            "ai_model": _artifact(*ai_model),
        },
        "target_models": ["R-9000"],
        "dependency_ranges": {
            "control_os": ">=3.0.0,<4.0.0",
            "network_config": ">=1.3.0,<2.0.0",
            "toolchain": ">=2.0.0,<3.0.0",
            "ai_model": ">=5.0.0,<6.0.0",
        },
        "migration_steps": [
            {
                "step": 1,
                "name": "刷写控制操作系统与网络配置",
                "description": "停机上电窗口内刷写控制 OS 并下发实时网络配置",
                "rollback_actions": ["回刷控制 OS 镜像", "复核实时总线通信矩阵"],
            },
            {
                "step": 2,
                "name": "部署工具链生成物与 AI 模型",
                "description": "下发工具链生成物并加载 AI 模型，执行控制参数对齐",
                "rollback_actions": ["恢复上一版 AI 模型与控制参数包", "整机标定复核"],
            },
        ],
        "rollback_versions": {
            "control_os": rollback_control_os,
            "network_config": "1.4.0",
            "toolchain": "2.0.1",
            "ai_model": rollback_ai_model,
        },
    }


def _pass_acceptances(service: ReleaseService, candidate_id: str, devices: list[str]) -> None:
    for device_id in devices:
        for side, actor in (("control", "ctrl-acc"), ("ai", "ai-acc")):
            service.record_acceptance(
                actor,
                candidate_id,
                {
                    "device_id": device_id,
                    "side": side,
                    "result": "pass",
                    "summary": f"{device_id} {side} 侧样机验收通过",
                    "idempotency_key": f"acc-{candidate_id}-{device_id}-{side}",
                },
            )


def run(workspace: Path) -> dict[str, object]:
    connection = sqlite3.connect(":memory:", isolation_level=None)
    connection.row_factory = sqlite3.Row
    service = ReleaseService(connection, FrozenClock(datetime(2026, 9, 30, 8, 0, tzinfo=timezone.utc)))
    for user_id, role in (
        ("plan", "planner"),
        ("ctrl-acc", "control_acceptor"),
        ("ai-acc", "ai_acceptor"),
        ("ctrl-owner", "control_owner"),
        ("ai-owner", "ai_owner"),
        ("ops", "fleet_operator"),
        ("audit", "auditor"),
    ):
        service.create_user(user_id, user_id, role)
    for device_id, batch in (("proto-1", "proto-01"), ("proto-2", "proto-01"), ("fleet-1", "delivery-01"), ("fleet-2", "delivery-01")):
        service.register_device("plan", {"device_id": device_id, "model": "R-9000", "batch_id": batch})

    # 第一代组合：完成全部门禁并推广到全部设备，成为已证明兼容的组合。
    service.create_candidate("plan", _candidate_payload("cand-v1", "cand-key-1", control_os=("3.1.4", "a"), ai_model=("5.0.7", "b"), rollback_control_os="3.1.4", rollback_ai_model="5.0.7"))
    _pass_acceptances(service, "cand-v1", ["proto-1", "proto-2"])
    service.approve_candidate("ctrl-owner", "cand-v1", {"side": "control", "note": "控制侧同意扩大范围"})
    service.approve_candidate("ai-owner", "cand-v1", {"side": "ai", "note": "AI 侧同意扩大范围"})
    service.start_rollout("ops", "cand-v1", {"device_ids": ["proto-1", "proto-2", "fleet-1", "fleet-2"], "idempotency_key": "roll-v1"})
    for device_id in ("proto-1", "proto-2", "fleet-1", "fleet-2"):
        for step in (1, 2):
            service.report_receipt("ops", "cand-v1", device_id, {"step": step, "status": "done", "detail": f"{device_id} 第 {step} 步完成", "idempotency_key": f"rcpt-v1-{device_id}-{step}"})

    # 第二代组合：任一制品变化即取代旧候选，未完成签署失效；新候选重新走门禁。
    service.create_candidate("plan", _candidate_payload("cand-v2", "cand-key-2", control_os=("3.2.0", "c"), ai_model=("5.1.0", "d"), rollback_control_os="3.1.4", rollback_ai_model="5.0.7"))
    _pass_acceptances(service, "cand-v2", ["proto-1", "proto-2"])
    service.approve_candidate("ctrl-owner", "cand-v2", {"side": "control", "note": "控制参数与模型已对齐"})
    service.approve_candidate("ai-owner", "cand-v2", {"side": "ai", "note": "模型验收结论有效"})
    service.start_rollout("ops", "cand-v2", {"device_ids": ["fleet-1", "fleet-2"], "idempotency_key": "roll-v2"})

    # fleet-1 分阶段回执全部完成；同一回执按键重放结果一致。
    for step in (1, 2):
        service.report_receipt("ops", "cand-v2", "fleet-1", {"step": step, "status": "done", "detail": f"fleet-1 第 {step} 步完成", "idempotency_key": f"rcpt-v2-fleet-1-{step}"})
    replay = service.report_receipt("ops", "cand-v2", "fleet-1", {"step": 2, "status": "done", "detail": "fleet-1 第 2 步完成", "idempotency_key": "rcpt-v2-fleet-1-2"})

    # fleet-2 第 2 步失败，只能回退到已证明兼容的 cand-v1 组合，并生成现场动作。
    service.report_receipt("ops", "cand-v2", "fleet-2", {"step": 1, "status": "done", "detail": "fleet-2 第 1 步完成", "idempotency_key": "rcpt-v2-fleet-2-1"})
    service.report_receipt("ops", "cand-v2", "fleet-2", {"step": 2, "status": "failed", "detail": "模型加载后整机动作偏离标定", "idempotency_key": "rcpt-v2-fleet-2-2"})
    rollback = service.rollback_device("ops", "cand-v2", "fleet-2", {"target_candidate_id": "cand-v1", "idempotency_key": "rb-fleet-2"})
    first_action = rollback["field_actions"][0]
    service.complete_field_action("ops", first_action["action_id"], {"note": "已回刷控制 OS 镜像"})

    fleet = service.fleet_status("audit", "delivery-2026q4")
    result = {
        "status": "ok",
        "receipt_replay_consistent": replay["migration_state"] == "completed",
        "rollback_combination": rollback["combination"],
        "fleet_gates": {device["device_id"]: device["gate"] for device in fleet["devices"]},
        "fleet_pending_actions": {
            device["device_id"]: len(device["pending_field_actions"]) for device in fleet["devices"]
        },
        "audit": service.audit_chain("audit"),
        "workspace": workspace.name,
    }
    connection.close()
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="运行协同发布服务离线验收")
    parser.add_argument("--workspace", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    print(json.dumps(run(args.workspace), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
