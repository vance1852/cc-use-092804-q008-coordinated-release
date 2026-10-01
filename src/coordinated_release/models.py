"""协同发布候选与设备的输入契约。

发布候选把控制操作系统、网络配置、工具链生成物和 AI 模型四类制品的摘要、
目标机型、依赖范围、迁移步骤和可回退版本固化为一份不可变定义。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

from .errors import ValidationFailed


IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{1,63}$")
SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
VERSION_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.+-]{0,63}$")
RANGE_PART = re.compile(r"^(>=|<=|==|!=|>|<)[A-Za-z0-9][A-Za-z0-9.+-]{0,63}$")

ARTIFACT_KINDS = ("control_os", "network_config", "toolchain", "ai_model")
ARTIFACT_LABELS = {
    "control_os": "控制操作系统",
    "network_config": "网络配置",
    "toolchain": "工具链生成物",
    "ai_model": "AI 模型",
}
SIDES = ("control", "ai")


def required_text(value: object, field: str, maximum: int = 256) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationFailed(f"{field} 不能为空")
    result = value.strip()
    if len(result) > maximum:
        raise ValidationFailed(f"{field} 不能超过 {maximum} 个字符")
    return result


def identifier(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not IDENTIFIER.fullmatch(result):
        raise ValidationFailed(f"{field} 格式不正确")
    return result


def version_text(value: object, field: str) -> str:
    result = required_text(value, field, 64)
    if not VERSION_TEXT.fullmatch(result):
        raise ValidationFailed(f"{field} 不是合法版本号")
    return result


@dataclass(frozen=True, slots=True)
class Artifact:
    version: str
    sha256: str

    def as_dict(self) -> dict[str, str]:
        return {"version": self.version, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class MigrationStep:
    step: int
    name: str
    description: str
    rollback_actions: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "name": self.name,
            "description": self.description,
            "rollback_actions": list(self.rollback_actions),
        }


@dataclass(frozen=True, slots=True)
class CandidateDefinition:
    """不可变发布候选的完整定义。"""

    prototype_batch: str
    artifacts: dict[str, Artifact]
    target_models: tuple[str, ...]
    dependency_ranges: dict[str, str]
    migration_steps: tuple[MigrationStep, ...]
    rollback_versions: dict[str, str]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "CandidateDefinition":
        if not isinstance(raw, Mapping):
            raise ValidationFailed("候选定义必须是 JSON 对象")
        return cls(
            prototype_batch=identifier(raw.get("prototype_batch"), "prototype_batch"),
            artifacts=_artifacts(raw.get("artifacts")),
            target_models=_target_models(raw.get("target_models")),
            dependency_ranges=_dependency_ranges(raw.get("dependency_ranges")),
            migration_steps=_migration_steps(raw.get("migration_steps")),
            rollback_versions=_rollback_versions(raw.get("rollback_versions")),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "prototype_batch": self.prototype_batch,
            "artifacts": {kind: self.artifacts[kind].as_dict() for kind in ARTIFACT_KINDS},
            "target_models": list(self.target_models),
            "dependency_ranges": {kind: self.dependency_ranges[kind] for kind in ARTIFACT_KINDS},
            "migration_steps": [step.as_dict() for step in self.migration_steps],
            "rollback_versions": {kind: self.rollback_versions[kind] for kind in ARTIFACT_KINDS},
        }

    @property
    def total_steps(self) -> int:
        return len(self.migration_steps)


def _artifacts(value: object) -> dict[str, Artifact]:
    if not isinstance(value, Mapping):
        raise ValidationFailed("artifacts 必须是对象")
    missing = [kind for kind in ARTIFACT_KINDS if kind not in value]
    if missing:
        labels = "、".join(ARTIFACT_LABELS[kind] for kind in missing)
        raise ValidationFailed(f"artifacts 缺少制品: {labels}")
    result: dict[str, Artifact] = {}
    for kind in ARTIFACT_KINDS:
        item = value[kind]
        if not isinstance(item, Mapping):
            raise ValidationFailed(f"artifacts.{kind} 必须是对象")
        version = version_text(item.get("version"), f"artifacts.{kind}.version")
        sha256 = required_text(item.get("sha256"), f"artifacts.{kind}.sha256", 64).lower()
        if not SHA256_HEX.fullmatch(sha256):
            raise ValidationFailed(f"artifacts.{kind}.sha256 必须是 64 位 SHA-256 摘要")
        result[kind] = Artifact(version, sha256)
    return result


def _target_models(value: object) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValidationFailed("target_models 必须是非空机型列表")
    models = tuple(dict.fromkeys(required_text(item, "target_models", 64) for item in value))
    return models


def _dependency_ranges(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValidationFailed("dependency_ranges 必须是对象")
    missing = [kind for kind in ARTIFACT_KINDS if kind not in value]
    if missing:
        labels = "、".join(ARTIFACT_LABELS[kind] for kind in missing)
        raise ValidationFailed(f"dependency_ranges 缺少制品: {labels}")
    result: dict[str, str] = {}
    for kind in ARTIFACT_KINDS:
        text = required_text(value[kind], f"dependency_ranges.{kind}", 128)
        parts = [part.strip() for part in text.split(",") if part.strip()]
        if not parts or any(RANGE_PART.fullmatch(part) is None for part in parts):
            raise ValidationFailed(f"dependency_ranges.{kind} 必须是逗号分隔的版本比较式")
        result[kind] = ",".join(parts)
    return result


def _migration_steps(value: object) -> tuple[MigrationStep, ...]:
    if not isinstance(value, list) or not value:
        raise ValidationFailed("migration_steps 必须是非空步骤列表")
    steps: list[MigrationStep] = []
    for index, item in enumerate(value, start=1):
        if not isinstance(item, Mapping):
            raise ValidationFailed(f"migration_steps[{index}] 必须是对象")
        step_no = item.get("step")
        if isinstance(step_no, bool) or not isinstance(step_no, int) or step_no != index:
            raise ValidationFailed("migration_steps 必须从 1 开始连续编号")
        name = required_text(item.get("name"), f"migration_steps[{index}].name", 128)
        description = required_text(item.get("description"), f"migration_steps[{index}].description", 512)
        raw_actions = item.get("rollback_actions", [])
        if not isinstance(raw_actions, list):
            raise ValidationFailed(f"migration_steps[{index}].rollback_actions 必须是列表")
        actions = tuple(
            required_text(action, f"migration_steps[{index}].rollback_actions", 256)
            for action in raw_actions
        )
        steps.append(MigrationStep(step_no, name, description, actions))
    return tuple(steps)


def _rollback_versions(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ValidationFailed("rollback_versions 必须是对象")
    missing = [kind for kind in ARTIFACT_KINDS if kind not in value]
    if missing:
        labels = "、".join(ARTIFACT_LABELS[kind] for kind in missing)
        raise ValidationFailed(f"rollback_versions 缺少制品: {labels}")
    return {
        kind: version_text(value[kind], f"rollback_versions.{kind}")
        for kind in ARTIFACT_KINDS
    }
