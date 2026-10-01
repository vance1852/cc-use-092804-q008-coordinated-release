"""协同发布域规则、事务门禁、幂等归并与 HTTP 接口测试。"""

from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timezone

from release_coordination.acceptance import _manifest
from release_coordination.api import JsonApplication
from release_coordination.clock import FrozenClock
from release_coordination.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from release_coordination.service import ReleaseCoordinator


STEPS = ("flash-os", "apply-net", "apply-params", "load-model")
CONTROL_STEPS = ("flash-os", "apply-net", "apply-params")


class ReleaseTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.clock = FrozenClock(datetime(2026, 9, 28, 8, 0, tzinfo=timezone.utc))
        self.service = ReleaseCoordinator(self.connection, self.clock)
        for user_id, role in (
            ("releng", "release_engineer"),
            ("ctl", "control_owner"),
            ("ai", "ai_owner"),
            ("field", "field_tech"),
            ("audit", "auditor"),
        ):
            self.service.create_user(user_id, user_id, role)
        self.service.create_batch("releng", "pilot-batch", "RX-1", "样机批次")
        for serial in ("d1", "d2"):
            self.service.register_device("releng", serial, "pilot-batch")
        self.baseline = self.service.register_baseline("releng", "combo", _manifest("factory"))
        self.candidate = self.service.create_candidate(
            "releng", "combo",
            _manifest("v1", rollback=[{"candidate_id": "combo", "revision": self.baseline["revision"]}]),
        )
        self.cid, self.rev = self.candidate["candidate_id"], self.candidate["revision"]
        self.wave = self.service.open_pilot_wave(
            "releng", "wave-pilot", self.cid, self.rev, "pilot-batch", "idem-wave")

    def tearDown(self) -> None:
        self.connection.close()

    def _accept_device(self, serial: str, *, failed_step: str | None = None) -> None:
        for step in STEPS:
            status = "failed" if step == failed_step else "ok"
            self.service.record_receipt(
                "field", self.wave["wave_id"], serial, step, status, {"check": "ok"})

    def _approve_both(self) -> None:
        self.service.approve("ctl", self.cid, self.rev, "control")
        self.service.approve("ai", self.cid, self.rev, "ai")


class ManifestValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.service = ReleaseCoordinator(self.connection)
        self.service.create_user("releng", "releng", "release_engineer")

    def tearDown(self) -> None:
        self.connection.close()

    def test_manifest_must_cover_both_sides(self) -> None:
        raw = _manifest("x")
        raw["artifacts"] = [a for a in raw["artifacts"] if a["kind"] != "ai_model"]
        with self.assertRaises(ValidationFailed):
            self.service.register_baseline("releng", "c", raw)

    def test_duplicate_artifact_kind_rejected(self) -> None:
        raw = _manifest("x")
        raw["artifacts"].append(dict(raw["artifacts"][0]))
        with self.assertRaises(ValidationFailed):
            self.service.register_baseline("releng", "c", raw)

    def test_steps_must_cover_both_sides(self) -> None:
        raw = _manifest("x")
        raw["migration_steps"] = [s for s in raw["migration_steps"] if s["side"] != "ai"]
        with self.assertRaises(ValidationFailed):
            self.service.register_baseline("releng", "c", raw)

    def test_candidate_requires_rollback_ref(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.create_candidate("releng", "c", _manifest("x"))

    def test_baseline_cannot_declare_rollback(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.register_baseline(
                "releng", "c", _manifest("x", rollback=[{"candidate_id": "combo", "revision": 1}]))

    def test_rollback_target_must_exist_and_be_proven(self) -> None:
        raw = _manifest("x", rollback=[{"candidate_id": "ghost", "revision": 9}])
        with self.assertRaises(ValidationFailed):
            self.service.create_candidate("releng", "c", raw)

    def test_artifact_digest_changes_with_any_artifact(self) -> None:
        first = self.service.register_baseline("releng", "c", _manifest("a"))
        second = self.service.register_baseline("releng", "c2", _manifest("b"))
        self.assertNotEqual(first["content_sha256"], second["content_sha256"])


class CandidateImmutabilityTests(ReleaseTestBase):
    def test_sealed_candidate_is_immutable_full_manifest(self) -> None:
        view = self.service.candidate(self.cid, self.rev)
        manifest = view["manifest"]
        self.assertEqual(
            sorted(a["kind"] for a in manifest["artifacts"]),
            ["ai_model", "control_os", "network_config", "toolchain_output"])
        self.assertEqual(manifest["target_models"], ["RX-1", "RX-2"])
        self.assertTrue(manifest["dependency_scope"])
        self.assertTrue(manifest["migration_steps"])
        self.assertEqual(manifest["rollback_refs"], [{"candidate_id": "combo", "revision": 1}])
        self.assertEqual(view["state"], "sealed")

    def test_any_artifact_change_invalidates_unfinished_signatures(self) -> None:
        self._accept_device("d1")
        self._accept_device("d2")
        self.service.approve("ctl", self.cid, self.rev, "control")
        # AI 侧尚未签署；只换 AI 模型产生新修订。
        new = self.service.create_candidate(
            "releng", "combo",
            _manifest("v2", rollback=[{"candidate_id": "combo", "revision": 1}]))
        self.assertEqual(new["revision"], self.rev + 1)
        old = self.service.candidate(self.cid, self.rev)
        self.assertTrue(old["superseded"])
        with self.assertRaises(InvalidState):
            self.service.approve("ai", self.cid, self.rev, "ai")
        # 签署不向新修订迁移。
        self.assertEqual(self.service.candidate(self.cid, new["revision"])["approvals"], [])

    def test_new_revision_does_not_invalidate_baseline(self) -> None:
        self.assertFalse(self.service.candidate("combo", 1)["superseded"])


class AcceptanceGateTests(ReleaseTestBase):
    def test_pilot_collects_both_side_summaries(self) -> None:
        self._accept_device("d1")
        report = self.service.wave_report(self.wave["wave_id"])
        self.assertEqual(report["control_acceptance"]["devices_accepted"], 1)
        self.assertEqual(report["ai_acceptance"]["devices_accepted"], 1)
        self.assertFalse(report["control_acceptance"]["passed"])
        self.assertFalse(report["ai_acceptance"]["passed"])
        self._accept_device("d2")
        report = self.service.wave_report(self.wave["wave_id"])
        self.assertTrue(report["control_acceptance"]["passed"])
        self.assertTrue(report["ai_acceptance"]["passed"])

    def test_approval_requires_own_side_acceptance(self) -> None:
        self._accept_device("d1")
        self._accept_device("d2", failed_step="load-model")
        with self.assertRaises(InvalidState):
            self.service.approve("ai", self.cid, self.rev, "ai")
        # 控制侧全过，可以签控制侧。
        self.service.approve("ctl", self.cid, self.rev, "control")

    def test_independent_owners_required(self) -> None:
        self._accept_device("d1")
        self._accept_device("d2")
        self.service.approve("ctl", self.cid, self.rev, "control")
        # 控制负责人不能签 AI 侧。
        with self.assertRaises(Forbidden):
            self.service.approve("ctl", self.cid, self.rev, "ai")
        # 现场工程师不能签署。
        with self.assertRaises(Forbidden):
            self.service.approve("field", self.cid, self.rev, "ai")

    def test_double_sign_same_side_conflicts(self) -> None:
        self._accept_device("d1")
        self._accept_device("d2")
        self.service.approve("ctl", self.cid, self.rev, "control")
        with self.assertRaises(Conflict):
            self.service.approve("ctl", self.cid, self.rev, "control")

    def test_expansion_blocked_until_both_approvals(self) -> None:
        self._accept_device("d1")
        self._accept_device("d2")
        self.service.create_batch("releng", "fleet-batch", "RX-2", "小批量")
        for serial in ("f1", "f2"):
            self.service.register_device("releng", serial, "fleet-batch")
        with self.assertRaises(InvalidState):
            self.service.open_expansion_wave(
                "releng", "wave-fleet", self.cid, self.rev, "fleet-batch", "idem-fleet")
        self._approve_both()
        expansion = self.service.open_expansion_wave(
            "releng", "wave-fleet", self.cid, self.rev, "fleet-batch", "idem-fleet")
        self.assertEqual(expansion["sequence"], 1)
        self.assertEqual(self.service.candidate(self.cid, self.rev)["state"], "released")

    def test_only_one_pilot_wave_per_revision(self) -> None:
        with self.assertRaises(InvalidState):
            self.service.open_pilot_wave(
                "releng", "wave-pilot-2", self.cid, self.rev, "pilot-batch", "k2")

    def test_batch_model_must_match_target(self) -> None:
        self.service.create_batch("releng", "other-batch", "RX-9", "别的机型")
        self.service.register_device("releng", "x1", "other-batch")
        with self.assertRaises(ValidationFailed):
            self.service.open_pilot_wave(
                "releng", "wave-x", self.cid, self.rev, "other-batch", "kx")


class ReceiptIdempotencyTests(ReleaseTestBase):
    def test_receipt_idempotent_replay(self) -> None:
        first = self.service.record_receipt(
            "field", self.wave["wave_id"], "d1", "flash-os", "ok", {"v": 1}, "key-1")
        second = self.service.record_receipt(
            "field", self.wave["wave_id"], "d1", "flash-os", "ok", {"v": 1}, "key-1")
        self.assertEqual(first["receipt_id"], second["receipt_id"])

    def test_same_key_different_payload_conflicts(self) -> None:
        self.service.record_receipt(
            "field", self.wave["wave_id"], "d1", "flash-os", "ok", {"v": 1}, "key-1")
        with self.assertRaises(Conflict):
            self.service.record_receipt(
                "field", self.wave["wave_id"], "d1", "flash-os", "ok", {"v": 2}, "key-1")

    def test_same_device_step_different_summary_conflicts(self) -> None:
        self.service.record_receipt(
            "field", self.wave["wave_id"], "d1", "flash-os", "ok", {"v": 1})
        with self.assertRaises(Conflict):
            self.service.record_receipt(
                "field", self.wave["wave_id"], "d1", "flash-os", "ok", {"v": 2})

    def test_failed_receipt_remains_replayable_after_device_fails(self) -> None:
        self.service.record_receipt(
            "field", self.wave["wave_id"], "d1", "apply-params", "failed", {"err": "偏离"}, "fail-key")
        replay = self.service.record_receipt(
            "field", self.wave["wave_id"], "d1", "apply-params", "failed", {"err": "偏离"}, "fail-key")
        self.assertEqual(replay["status"], "failed")
        with self.assertRaises(InvalidState):
            self.service.record_receipt(
                "field", self.wave["wave_id"], "d1", "load-model", "ok", {})


class RollbackTests(ReleaseTestBase):
    def _fail_device(self, serial: str = "d2") -> None:
        for step in CONTROL_STEPS:
            status = "failed" if step == "apply-params" else "ok"
            self.service.record_receipt("field", self.wave["wave_id"], serial, step, status, {})

    def test_rollback_only_to_declared_proven_combination(self) -> None:
        self._fail_device()
        # 不在 rollback_refs 中的组合禁止回退。
        other = self.service.register_baseline("releng", "other-combo", _manifest("other"))
        with self.assertRaises(InvalidState):
            self.service.rollback_device(
                "releng", "d2", "other-combo", other["revision"], "原因", "rb1")
        result = self.service.rollback_device(
            "releng", "d2", "combo", self.baseline["revision"], "参数步骤失败", "rb1")
        self.assertEqual(result["rolled_back_to"], {"candidate_id": "combo", "revision": 1})
        # 回退目标是完整组合，四类制品齐备。
        self.assertEqual(
            sorted(a["kind"] for a in result["target_combination"]["artifacts"]),
            ["ai_model", "control_os", "network_config", "toolchain_output"])

    def test_rollback_requires_failure(self) -> None:
        with self.assertRaises(InvalidState):
            self.service.rollback_device(
                "releng", "d1", "combo", 1, "原因", "rb2")

    def test_rollback_creates_field_actions_for_attempted_steps(self) -> None:
        self._fail_device()
        result = self.service.rollback_device(
            "releng", "d2", "combo", 1, "参数失败", "rb3")
        # flash-os / apply-net / apply-params 已尝试；load-model 未尝试，且无现场动作。
        self.assertEqual(
            sorted(a["step_id"] for a in result["pending_field_actions"]),
            ["apply-net", "apply-params", "flash-os"])
        view = self.service.device_status("d2")
        self.assertEqual(view["state"], "rolled_back")
        self.assertEqual(len(view["pending_field_actions"]), 3)
        for action in view["pending_field_actions"]:
            self.service.complete_field_action("field", action["action_id"])
        self.assertEqual(self.service.device_status("d2")["state"], "recovered")

    def test_rollback_is_idempotent(self) -> None:
        self._fail_device()
        first = self.service.rollback_device("releng", "d2", "combo", 1, "原因", "rb4")
        second = self.service.rollback_device("releng", "d2", "combo", 1, "原因", "rb4")
        self.assertEqual(first["rolled_back_to"], second["rolled_back_to"])

    def test_cannot_assemble_unapproved_mix(self) -> None:
        # 新建候选 rev3，声明回退到 rev2（尚封存、未发布、非基线），封存即拒绝。
        with self.assertRaises(InvalidState):
            self.service.create_candidate(
                "releng", "combo",
                _manifest("v3", rollback=[{"candidate_id": "combo", "revision": self.rev}]))


class DeviceViewTests(ReleaseTestBase):
    def test_status_shows_combination_and_blocking_gate(self) -> None:
        self.service.record_receipt(
            "field", self.wave["wave_id"], "d1", "flash-os", "ok", {})
        view = self.service.device_status("d1")
        self.assertEqual(view["state"], "in_progress")
        self.assertIsNone(view["installed_combination"])
        self.assertEqual(
            sorted(a["kind"] for a in view["scheduled_combination"]["artifacts"]),
            ["ai_model", "control_os", "network_config", "toolchain_output"])
        self.assertEqual(view["gate"]["blocking_step"], "apply-net")
        step_status = {s["step_id"]: s["status"] for s in view["gate"]["steps"]}
        self.assertEqual(step_status["flash-os"], "ok")
        self.assertEqual(step_status["apply-net"], "missing")

    def test_status_shows_field_actions_after_rollback(self) -> None:
        for step in CONTROL_STEPS:
            status = "failed" if step == "apply-params" else "ok"
            self.service.record_receipt("field", self.wave["wave_id"], "d1", step, status, {})
        self.service.rollback_device("releng", "d1", "combo", 1, "失败", "rb5")
        view = self.service.device_status("d1")
        self.assertIsNone(view["gate"])
        self.assertTrue(view["pending_field_actions"])
        self.assertTrue(all("description" in a for a in view["pending_field_actions"]))


class AuditTests(ReleaseTestBase):
    def test_audit_chain_valid(self) -> None:
        self._accept_device("d1")
        chain = self.service.audit_chain("audit")
        self.assertTrue(chain["valid"])
        self.assertGreater(chain["events"], 0)

    def test_auditor_is_read_only(self) -> None:
        with self.assertRaises(Forbidden):
            self.service.create_candidate(
                "audit", "combo",
                _manifest("x", rollback=[{"candidate_id": "combo", "revision": 1}]))


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:", isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.app = JsonApplication(ReleaseCoordinator(self.connection))

    def tearDown(self) -> None:
        self.connection.close()

    def _call(self, method: str, path: str, actor: str = "releng", payload=None):
        body = json.dumps(payload, ensure_ascii=False).encode() if payload is not None else b""
        response = self.app.handle(method, path, {"X-Actor-Id": actor}, body)
        return response.status, response.body

    def test_health_and_errors(self) -> None:
        self.assertEqual(self.app.handle("GET", "/health").status, 200)
        self.assertEqual(self.app.handle("POST", "/batches", body=b"bad").status, 422)
        self.assertEqual(self.app.handle("POST", "/batches", body=b"{}").status, 422)
        self.assertEqual(self._call("GET", "/nope")[0], 404)

    def test_full_flow_over_http(self) -> None:
        for uid, role in (
            ("releng", "release_engineer"), ("ctl", "control_owner"),
            ("ai", "ai_owner"), ("field", "field_tech")):
            self.assertEqual(
                self._call("POST", "/users", payload={"user_id": uid, "display_name": uid, "role": role})[0],
                201)
        self.assertEqual(
            self._call("POST", "/batches", payload={"batch_id": "b1", "machine_model": "RX-1"})[0], 201)
        self.assertEqual(
            self._call("POST", "/devices", payload={"device_serial": "d1", "batch_id": "b1"})[0], 201)
        status, baseline = self._call("POST", "/candidates/combo/baseline", payload=_manifest("fac"))
        self.assertEqual(status, 201)
        status, candidate = self._call(
            "POST", "/candidates/combo/revisions",
            payload=_manifest("rel", rollback=[{"candidate_id": "combo", "revision": 1}]))
        self.assertEqual(status, 201)
        rev = candidate["revision"]
        status, _ = self._call(
            "POST", f"/candidates/combo/revisions/{rev}/waves",
            payload={"wave_id": "w1", "kind": "pilot", "batch_id": "b1", "idempotency_key": "k1"})
        self.assertEqual(status, 201)
        for step in STEPS:
            status, _ = self._call(
                "POST", "/waves/w1/receipts", actor="field",
                payload={"device_serial": "d1", "step_id": step, "status": "ok", "summary": {}})
            self.assertEqual(status, 201)
        self.assertEqual(
            self._call("POST", f"/candidates/combo/revisions/{rev}/control", actor="ctl")[0], 200)
        self.assertEqual(
            self._call("POST", f"/candidates/combo/revisions/{rev}/ai", actor="ai")[0], 200)
        status, view = self._call("GET", "/devices/d1")
        self.assertEqual(status, 200)
        self.assertEqual(view["state"], "accepted")
        status, report = self._call("GET", "/waves/w1")
        self.assertEqual(status, 200)
        self.assertTrue(report["control_acceptance"]["passed"])
        self.assertTrue(report["ai_acceptance"]["passed"])


if __name__ == "__main__":
    unittest.main()
