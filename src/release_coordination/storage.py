"""协同发布服务的 SQLite 模式和事务辅助。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS release_users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN
        ('release_engineer','control_owner','ai_owner','field_tech','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS release_device_batches (
    batch_id TEXT PRIMARY KEY,
    machine_model TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS release_devices (
    device_serial TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES release_device_batches(batch_id),
    machine_model TEXT NOT NULL,
    installed_candidate_id TEXT,
    installed_revision INTEGER,
    scheduled_candidate_id TEXT,
    scheduled_revision INTEGER,
    state TEXT NOT NULL DEFAULT 'registered'
        CHECK(state IN ('registered','scheduled','in_progress','accepted','failed',
                       'rolled_back','recovered')),
    registered_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_devices_batch
ON release_devices(batch_id, machine_model);

-- 发布候选按修订版本不可变：封存后制品清单、目标机型、依赖范围、
-- 迁移步骤与可回退版本均不可修改，任何制品变化只能产生新修订。
CREATE TABLE IF NOT EXISTS release_candidates (
    candidate_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'assembling'
        CHECK(state IN ('assembling','sealed','released')),
    superseded INTEGER NOT NULL DEFAULT 0 CHECK(superseded IN (0,1)),
    -- 工厂/试制阶段已证明兼容的组合，可作为首批发布的回退目标。
    baseline_certified INTEGER NOT NULL DEFAULT 0 CHECK(baseline_certified IN (0,1)),
    artifacts_json TEXT NOT NULL,
    target_models_json TEXT NOT NULL,
    dependency_scope_json TEXT NOT NULL,
    migration_steps_json TEXT NOT NULL,
    rollback_refs_json TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL,
    sealed_at TEXT,
    PRIMARY KEY(candidate_id, revision)
);

-- 签署绑定修订与内容摘要；新修订上旧签署不迁移。
CREATE TABLE IF NOT EXISTS release_approvals (
    candidate_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    scope TEXT NOT NULL CHECK(scope IN ('control','ai')),
    approver_id TEXT NOT NULL REFERENCES release_users(user_id),
    content_sha256 TEXT NOT NULL,
    approved_at TEXT NOT NULL,
    PRIMARY KEY(candidate_id, revision, scope),
    FOREIGN KEY(candidate_id, revision) REFERENCES release_candidates(candidate_id, revision)
);

-- 分阶段放行：0 为指定样机批次，其后为批准扩大的波次。
CREATE TABLE IF NOT EXISTS release_waves (
    wave_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('pilot','expansion')),
    batch_id TEXT NOT NULL REFERENCES release_device_batches(batch_id),
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','completed')),
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY(candidate_id, revision, sequence),
    UNIQUE(wave_id),
    FOREIGN KEY(candidate_id, revision) REFERENCES release_candidates(candidate_id, revision)
);

CREATE TABLE IF NOT EXISTS release_wave_devices (
    wave_id TEXT NOT NULL REFERENCES release_waves(wave_id),
    candidate_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    device_serial TEXT NOT NULL REFERENCES release_devices(device_serial),
    sequence INTEGER NOT NULL,
    PRIMARY KEY(candidate_id, revision, device_serial),
    UNIQUE(wave_id, device_serial)
);

-- 分阶段回执按设备和步骤归并：同一修订下设备×步骤唯一。
CREATE TABLE IF NOT EXISTS release_receipts (
    receipt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    candidate_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    wave_id TEXT NOT NULL REFERENCES release_waves(wave_id),
    device_serial TEXT NOT NULL REFERENCES release_devices(device_serial),
    step_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('control','ai')),
    status TEXT NOT NULL CHECK(status IN ('ok','failed')),
    summary_json TEXT NOT NULL,
    summary_sha256 TEXT NOT NULL,
    recorded_by TEXT NOT NULL REFERENCES release_users(user_id),
    recorded_at TEXT NOT NULL,
    UNIQUE(candidate_id, revision, device_serial, step_id)
);

CREATE INDEX IF NOT EXISTS idx_release_receipts_wave
ON release_receipts(wave_id, device_serial);

-- 回退后仍需在现场完成的动作。
CREATE TABLE IF NOT EXISTS release_field_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_serial TEXT NOT NULL REFERENCES release_devices(device_serial),
    from_candidate_id TEXT NOT NULL,
    from_revision INTEGER NOT NULL,
    to_candidate_id TEXT NOT NULL,
    to_revision INTEGER NOT NULL,
    step_id TEXT NOT NULL,
    description TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done')),
    reason TEXT NOT NULL,
    completed_by TEXT REFERENCES release_users(user_id),
    completed_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(device_serial, from_candidate_id, from_revision, step_id)
);

CREATE INDEX IF NOT EXISTS idx_field_actions_device
ON release_field_actions(device_serial, state);

CREATE TABLE IF NOT EXISTS release_idempotency (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_sha256 TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(scope, idempotency_key)
);

CREATE TABLE IF NOT EXISTS release_audit_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_release_audit_entity
ON release_audit_events(entity_type, entity_id, event_id);
"""


def connect(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    initialize(connection)
    return connection


def initialize(connection: sqlite3.Connection) -> None:
    connection.executescript(SCHEMA)


@contextmanager
def transaction(connection: sqlite3.Connection, *, immediate: bool = False) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
