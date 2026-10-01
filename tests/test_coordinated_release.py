from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone

from coordinated_release.api import JsonApplication
from coordinated_release.clock import FrozenClock
from coordinated_release.errors import Conflict, Forbidden, InvalidState
from coordinated_release.service import ReleaseService


def artifact(version: str, seed: str) -> dict[str, str]:
    return {"version": version, "sha256": (seed * 64)[:64]}


def candidate_payload(
    candidate_id: str,
    idem_key: str,
    *,
    control_os: tuple[str, str] = ("3.1.4", "a"),
    ai_model: tuple[str, str] = ("5.0.7", "b"),
    rollback_control_os: str = "3.1.4",
    rollback_ai_model: str = "5.0.7",
    release_line: str = "delivery-2026q4",
) -> dict[str, object]:
    return {
        "candidate_id": candidate_id,
        "release_line": release_line,
        "idempotency_key": idem_key,
        "prototype_batch": "proto-01",
        "artifacts": {
            "control_os": artifact(*control_os),
            "network_config": artifact("1.4.0", "f"),
            "toolchain": artifact("2.0.1", "0"),
            "ai_model": artifact(*ai_model),
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


class ReleaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 30, 8, 0, tzinfo=timezone.utc))
        self.service = ReleaseService(self.connection, self.clock)
        for user_id, role in (
            ("plan", "planner"),
            ("ctrl-acc", "control_acceptor"),
            ("ai-acc", "ai_acceptor"),
            ("ctrl-owner", "control_owner"),
            ("ai-owner", "ai_owner"),
            ("ops", "fleet_operator"),
            ("audit", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        for device_id, batch in (
            ("proto-1", "proto-01"),
            ("proto-2", "proto-01"),
            ("fleet-1", "delivery-01"),
        ):
            self.service.register_device(
                "plan", {"device_id": device_id, "model": "R-9000", "batch_id": batch}
            )

    def tearDown(self) -> None:
        self.connection.close()

    def _accept(self, candidate_id: str, devices: tuple[str, ...] = ("proto-1", "proto-2")) -> None:
        for device_id in devices:
            for side, actor in (("control", "ctrl-acc"), ("ai", "ai-acc")):
                self.service.record_acceptance(
                    actor,
                    candidate_id,
                    {
                        "device_id": device_id,
                        "side": side,
                        "result": "pass",
                        "summary": f"{device_id} {side} 侧验收通过",
                        "idempotency_key": f"acc-{candidate_id}-{device_id}-{side}",
                    },
                )

    def _approve(self, candidate_id: str) -> None:
        self._accept(candidate_id)
        self.service.approve_candidate("ctrl-owner", candidate_id, {"side": "control", "note": "控制侧同意"})
        self.service.approve_candidate("ai-owner", candidate_id, {"side": "ai", "note": "AI 侧同意"})

    def _migrate(self, candidate_id: str, device_id: str, key_prefix: str) -> None:
        for step in (1, 2):
            self.service.report_receipt(
                "ops",
                candidate_id,
                device_id,
                {
                    "step": step,
                    "status": "done",
                    "detail": f"{device_id} 第 {step} 步完成",
                    "idempotency_key": f"{key_prefix}-{device_id}-{step}",
                },
            )

    def test_complete_workflow_reaches_completed_combination(self) -> None:
        created = self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        self.assertEqual(created["state"], "open")
        self.assertEqual(len(created["content_sha256"]), 64)
        self._approve("cand-v1")
        status = self.service.candidate_status("audit", "cand-v1")
        self.assertEqual(status["state"], "approved")
        self.assertTrue(status["gates"]["prototype_acceptance"]["complete"])
        self.assertTrue(status["gates"]["independent_approval"]["complete"])
        self.service.start_rollout("ops", "cand-v1", {"device_ids": ["fleet-1"], "idempotency_key": "roll-1"})
        self._migrate("cand-v1", "fleet-1", "rcpt")
        view = self.service.device_status("audit", "fleet-1")
        self.assertEqual(view["gate"], "completed")
        self.assertEqual(view["combination"]["candidate_id"], "cand-v1")
        self.assertEqual(view["combination"]["reason"], "migration_completed")
        self.assertEqual(view["combination"]["artifacts"]["ai_model"]["version"], "5.0.7")
        self.assertEqual(view["combination"]["artifacts"]["control_os"]["sha256"], "a" * 64)

    def test_new_candidate_supersedes_and_invalidates_incomplete_sign_offs(self) -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        self.service.record_acceptance(
            "ctrl-acc",
            "cand-v1",
            {
                "device_id": "proto-1",
                "side": "control",
                "result": "pass",
                "summary": "控制侧通过",
                "idempotency_key": "acc-1",
            },
        )
        created = self.service.create_candidate(
            "plan",
            candidate_payload("cand-v2", "key-v2", ai_model=("5.1.0", "d"), rollback_ai_model="5.0.7"),
        )
        self.assertEqual(created["superseded"], ["cand-v1"])
        old = self.service.candidate_status("audit", "cand-v1")
        self.assertEqual(old["state"], "superseded")
        self.assertEqual(old["superseded_by"], "cand-v2")
        with self.assertRaises(InvalidState):
            self.service.record_acceptance(
                "ai-acc",
                "cand-v1",
                {
                    "device_id": "proto-1",
                    "side": "ai",
                    "result": "pass",
                    "summary": "AI 侧通过",
                    "idempotency_key": "acc-2",
                },
            )
        with self.assertRaises(InvalidState):
            self.service.approve_candidate("ctrl-owner", "cand-v1", {"side": "control", "note": "晚到的批准"})
        events = self.connection.execute(
            "SELECT payload_json FROM release_audit_events WHERE event_type='candidate.superseded'"
        ).fetchall()
        payload = json.loads(events[0]["payload_json"])
        self.assertEqual(payload["reason"], "artifact_changed")
        self.assertIn("proto-1:ai", payload["incomplete_sign_offs"]["missing_acceptances"])
        self.assertEqual(payload["incomplete_sign_offs"]["missing_approvals"], ["control", "ai"])

    def test_approval_requires_both_sides_of_prototype_acceptance(self) -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        with self.assertRaises(InvalidState):
            self.service.approve_candidate("ctrl-owner", "cand-v1", {"side": "control", "note": "太早"})
        for device_id in ("proto-1", "proto-2"):
            self.service.record_acceptance(
                "ctrl-acc",
                "cand-v1",
                {
                    "device_id": device_id,
                    "side": "control",
                    "result": "pass",
                    "summary": "控制侧通过",
                    "idempotency_key": f"acc-c-{device_id}",
                },
            )
        with self.assertRaises(InvalidState):
            self.service.approve_candidate("ctrl-owner", "cand-v1", {"side": "control", "note": "AI 侧还缺"})

    def test_acceptance_scoped_to_prototype_batch_and_side_role(self) -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        with self.assertRaises(Conflict):
            self.service.record_acceptance(
                "ctrl-acc",
                "cand-v1",
                {
                    "device_id": "fleet-1",
                    "side": "control",
                    "result": "pass",
                    "summary": "非样机批次设备",
                    "idempotency_key": "acc-x",
                },
            )
        with self.assertRaises(Forbidden):
            self.service.record_acceptance(
                "ctrl-acc",
                "cand-v1",
                {
                    "device_id": "proto-1",
                    "side": "ai",
                    "result": "pass",
                    "summary": "越侧签署",
                    "idempotency_key": "acc-y",
                },
            )

    def test_approval_side_roles_and_duplicate(self) -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        self._accept("cand-v1")
        with self.assertRaises(Forbidden):
            self.service.approve_candidate("ctrl-owner", "cand-v1", {"side": "ai", "note": "越侧"})
        self.service.approve_candidate("ctrl-owner", "cand-v1", {"side": "control", "note": "同意"})
        with self.assertRaises(Conflict):
            self.service.approve_candidate("ctrl-owner", "cand-v1", {"side": "control", "note": "重复"})

    def test_rollout_requires_approval(self) -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        with self.assertRaises(InvalidState):
            self.service.start_rollout("ops", "cand-v1", {"device_ids": ["fleet-1"], "idempotency_key": "roll-1"})

    def test_receipts_merge_idempotently_by_device_and_step(self) -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        self._approve("cand-v1")
        self.service.start_rollout("ops", "cand-v1", {"device_ids": ["fleet-1"], "idempotency_key": "roll-1"})
        out_of_order = {
            "step": 2,
            "status": "done",
            "detail": "跳步",
            "idempotency_key": "rcpt-2",
        }
        with self.assertRaises(InvalidState):
            self.service.report_receipt("ops", "cand-v1", "fleet-1", out_of_order)
        first = self.service.report_receipt(
            "ops",
            "cand-v1",
            "fleet-1",
            {"step": 1, "status": "done", "detail": "第 1 步完成", "idempotency_key": "rcpt-1"},
        )
        replay = self.service.report_receipt(
            "ops",
            "cand-v1",
            "fleet-1",
            {"step": 1, "status": "done", "detail": "第 1 步完成", "idempotency_key": "rcpt-1"},
        )
        self.assertEqual(first, replay)
        with self.assertRaises(Conflict):
            self.service.report_receipt(
                "ops",
                "cand-v1",
                "fleet-1",
                {"step": 1, "status": "done", "detail": "换键重复上报", "idempotency_key": "rcpt-1b"},
            )
        count = self.connection.execute("SELECT count(*) FROM step_receipts").fetchone()[0]
        self.assertEqual(count, 1)

    def test_candidate_creation_is_idempotent(self) -> None:
        payload = candidate_payload("cand-v1", "key-v1")
        first = self.service.create_candidate("plan", payload)
        second = self.service.create_candidate("plan", payload)
        self.assertEqual(first, second)
        count = self.connection.execute("SELECT count(*) FROM release_candidates").fetchone()[0]
        self.assertEqual(count, 1)
        changed = candidate_payload("cand-v9", "key-v1")
        with self.assertRaises(Conflict):
            self.service.create_candidate("plan", changed)

    def _deploy_v1_then_fail_v2(self, rollback_ai_model: str = "5.0.7") -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v1", "key-v1"))
        self._approve("cand-v1")
        self.service.start_rollout("ops", "cand-v1", {"device_ids": ["fleet-1"], "idempotency_key": "roll-v1"})
        self._migrate("cand-v1", "fleet-1", "rcpt-v1")
        self.service.create_candidate(
            "plan",
            candidate_payload(
                "cand-v2",
                "key-v2",
                control_os=("3.2.0", "c"),
                ai_model=("5.1.0", "d"),
                rollback_control_os="3.1.4",
                rollback_ai_model=rollback_ai_model,
            ),
        )
        self._approve("cand-v2")
        self.service.start_rollout("ops", "cand-v2", {"device_ids": ["fleet-1"], "idempotency_key": "roll-v2"})
        self.service.report_receipt(
            "ops",
            "cand-v2",
            "fleet-1",
            {"step": 1, "status": "done", "detail": "第 1 步完成", "idempotency_key": "rcpt-v2-1"},
        )
        self.service.report_receipt(
            "ops",
            "cand-v2",
            "fleet-1",
            {"step": 2, "status": "failed", "detail": "整机动作偏离标定", "idempotency_key": "rcpt-v2-2"},
        )

    def test_failed_migration_rolls_back_to_proven_combination(self) -> None:
        self._deploy_v1_then_fail_v2()
        view = self.service.device_status("audit", "fleet-1")
        self.assertEqual(view["gate"], "rollback_required")
        rollback = self.service.rollback_device(
            "ops", "cand-v2", "fleet-1", {"target_candidate_id": "cand-v1", "idempotency_key": "rb-1"}
        )
        self.assertEqual(rollback["to_candidate"], "cand-v1")
        self.assertEqual(rollback["combination"]["ai_model"]["version"], "5.0.7")
        self.assertEqual(rollback["combination"]["control_os"]["version"], "3.1.4")
        self.assertEqual(len(rollback["field_actions"]), 4)
        replay = self.service.rollback_device(
            "ops", "cand-v2", "fleet-1", {"target_candidate_id": "cand-v1", "idempotency_key": "rb-1"}
        )
        self.assertEqual(rollback, replay)
        view = self.service.device_status("audit", "fleet-1")
        self.assertEqual(view["gate"], "field_actions_pending")
        self.assertEqual(view["combination"]["candidate_id"], "cand-v1")
        self.assertEqual(view["combination"]["reason"], "rollback")
        for action in view["pending_field_actions"]:
            self.service.complete_field_action("ops", action["action_id"], {"note": "已处理"})
        view = self.service.device_status("audit", "fleet-1")
        self.assertEqual(view["gate"], "rolled_back")
        self.assertEqual(view["pending_field_actions"], [])

    def test_rollback_rejects_unapproved_or_undeclared_targets(self) -> None:
        self._deploy_v1_then_fail_v2()
        self.service.create_candidate(
            "plan",
            candidate_payload("cand-v3", "key-v3", ai_model=("5.2.0", "e"), rollback_ai_model="5.1.0"),
        )
        with self.assertRaises(Conflict):
            self.service.rollback_device(
                "ops", "cand-v2", "fleet-1", {"target_candidate_id": "cand-v3", "idempotency_key": "rb-bad"}
            )

    def test_rollback_rejects_versions_outside_declared_range(self) -> None:
        self._deploy_v1_then_fail_v2(rollback_ai_model="5.0.0")
        with self.assertRaises(Conflict):
            self.service.rollback_device(
                "ops", "cand-v2", "fleet-1", {"target_candidate_id": "cand-v1", "idempotency_key": "rb-1"}
            )

    def test_supersede_halts_in_progress_migration(self) -> None:
        self.service.create_candidate("plan", candidate_payload("cand-v0", "key-v0"))
        self._approve("cand-v0")
        self.service.create_candidate(
            "plan",
            candidate_payload(
                "cand-v1",
                "key-v1",
                control_os=("3.2.0", "c"),
                ai_model=("5.1.0", "d"),
                rollback_control_os="3.1.4",
                rollback_ai_model="5.0.7",
            ),
        )
        self._approve("cand-v1")
        self.service.start_rollout("ops", "cand-v1", {"device_ids": ["fleet-1"], "idempotency_key": "roll-v1"})
        self.service.report_receipt(
            "ops",
            "cand-v1",
            "fleet-1",
            {"step": 1, "status": "done", "detail": "第 1 步完成", "idempotency_key": "rcpt-1"},
        )
        self.service.create_candidate(
            "plan",
            candidate_payload("cand-v2", "key-v2", ai_model=("5.1.1", "e"), rollback_ai_model="5.1.0"),
        )
        with self.assertRaises(InvalidState):
            self.service.report_receipt(
                "ops",
                "cand-v1",
                "fleet-1",
                {"step": 2, "status": "done", "detail": "被取代后继续", "idempotency_key": "rcpt-2"},
            )
        view = self.service.device_status("audit", "fleet-1")
        self.assertEqual(view["gate"], "candidate_superseded")
        rollback = self.service.rollback_device(
            "ops", "cand-v1", "fleet-1", {"target_candidate_id": "cand-v0", "idempotency_key": "rb-1"}
        )
        self.assertEqual(rollback["to_candidate"], "cand-v0")
        self.assertEqual(rollback["combination"]["control_os"]["version"], "3.1.4")

    def test_fleet_status_and_audit_chain(self) -> None:
        self._deploy_v1_then_fail_v2()
        self.service.rollback_device(
            "ops", "cand-v2", "fleet-1", {"target_candidate_id": "cand-v1", "idempotency_key": "rb-1"}
        )
        fleet = self.service.fleet_status("audit", "delivery-2026q4")
        gates = {device["device_id"]: device["gate"] for device in fleet["devices"]}
        self.assertEqual(gates["fleet-1"], "field_actions_pending")
        self.assertEqual(gates["proto-1"], "rollout_pending")
        self.assertEqual([row["candidate_id"] for row in fleet["candidates"]], ["cand-v1", "cand-v2"])
        self.assertEqual(fleet["candidates"][0]["state"], "superseded")
        chain = self.service.audit_chain("audit")
        self.assertTrue(chain["valid"])
        self.assertGreater(chain["events"], 0)


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(ReleaseService(self.connection))

    def tearDown(self) -> None:
        self.connection.close()

    def _post(self, path: str, payload: dict, actor: str = "plan"):
        return self.app.handle(
            "POST",
            path,
            {"X-Actor-Id": actor},
            json.dumps(payload).encode("utf-8"),
        )

    def test_health(self) -> None:
        response = self.app.handle("GET", "/health")
        self.assertEqual(response.status, 200)

    def test_candidate_roundtrip_over_http(self) -> None:
        self.assertEqual(self._post("/users", {"user_id": "plan", "display_name": "计划", "role": "planner"}).status, 201)
        self.assertEqual(
            self._post("/devices", {"device_id": "proto-1", "model": "R-9000", "batch_id": "proto-01"}).status,
            201,
        )
        created = self._post("/candidates", candidate_payload("cand-v1", "key-v1"))
        self.assertEqual(created.status, 201)
        self.assertEqual(created.body["state"], "open")
        missing_actor = self.app.handle("GET", "/candidates/cand-v1")
        self.assertEqual(missing_actor.status, 422)
        view = self.app.handle("GET", "/candidates/cand-v1", {"X-Actor-Id": "plan"})
        self.assertEqual(view.status, 200)
        self.assertEqual(view.body["gates"]["prototype_acceptance"]["required_devices"], ["proto-1"])
        fleet = self.app.handle("GET", "/fleet?release_line=delivery-2026q4", {"X-Actor-Id": "plan"})
        self.assertEqual(fleet.status, 200)
        self.assertEqual(fleet.body["devices"][0]["gate"], "prototype_acceptance")


if __name__ == "__main__":
    unittest.main()
