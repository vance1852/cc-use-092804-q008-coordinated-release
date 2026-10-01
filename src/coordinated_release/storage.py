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
    role TEXT NOT NULL CHECK(role IN ('planner','control_acceptor','ai_acceptor',
        'control_owner','ai_owner','fleet_operator','auditor')),
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_devices_batch
ON devices(batch_id, model);

CREATE TABLE IF NOT EXISTS release_candidates (
    candidate_id TEXT PRIMARY KEY,
    release_line TEXT NOT NULL,
    definition_json TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','approved','superseded')),
    superseded_by TEXT,
    revision INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NOT NULL REFERENCES release_users(user_id),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_candidates_line
ON release_candidates(release_line, created_at);

CREATE TABLE IF NOT EXISTS candidate_acceptances (
    candidate_id TEXT NOT NULL REFERENCES release_candidates(candidate_id),
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    side TEXT NOT NULL CHECK(side IN ('control','ai')),
    result TEXT NOT NULL CHECK(result IN ('pass','fail')),
    summary TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    recorded_by TEXT NOT NULL REFERENCES release_users(user_id),
    recorded_at TEXT NOT NULL,
    PRIMARY KEY(candidate_id, device_id, side)
);

CREATE TABLE IF NOT EXISTS candidate_approvals (
    candidate_id TEXT NOT NULL REFERENCES release_candidates(candidate_id),
    side TEXT NOT NULL CHECK(side IN ('control','ai')),
    approver_id TEXT NOT NULL REFERENCES release_users(user_id),
    note TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(candidate_id, side)
);

CREATE TABLE IF NOT EXISTS device_migrations (
    migration_id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    candidate_id TEXT NOT NULL REFERENCES release_candidates(candidate_id),
    state TEXT NOT NULL DEFAULT 'migrating'
        CHECK(state IN ('migrating','completed','failed','halted','rolled_back')),
    current_step INTEGER NOT NULL DEFAULT 0,
    failed_step INTEGER,
    failure_detail TEXT,
    started_by TEXT NOT NULL REFERENCES release_users(user_id),
    started_at TEXT NOT NULL,
    ended_at TEXT,
    UNIQUE(device_id, candidate_id)
);

CREATE INDEX IF NOT EXISTS idx_migrations_candidate
ON device_migrations(candidate_id, state);

CREATE TABLE IF NOT EXISTS step_receipts (
    candidate_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    step INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('done','failed')),
    detail TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    reported_by TEXT NOT NULL REFERENCES release_users(user_id),
    reported_at TEXT NOT NULL,
    PRIMARY KEY(candidate_id, device_id, step),
    FOREIGN KEY(device_id, candidate_id)
        REFERENCES device_migrations(device_id, candidate_id)
);

CREATE TABLE IF NOT EXISTS device_combinations (
    combination_id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    candidate_id TEXT NOT NULL REFERENCES release_candidates(candidate_id),
    reason TEXT NOT NULL CHECK(reason IN ('migration_completed','rollback')),
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_combinations_device
ON device_combinations(device_id, combination_id);

CREATE TABLE IF NOT EXISTS field_actions (
    action_id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL REFERENCES devices(device_id),
    migration_id INTEGER NOT NULL REFERENCES device_migrations(migration_id),
    description TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','done')),
    completed_by TEXT REFERENCES release_users(user_id),
    completed_at TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_field_actions_device
ON field_actions(device_id, state);

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


def row_dict(row: sqlite3.Row | None) -> dict[str, object] | None:
    return None if row is None else dict(row)
