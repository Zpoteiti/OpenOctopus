"""Schema bootstrap tests against a real PostgreSQL database."""

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError

EXPECTED_COLUMNS = {
    "system_config": 3,
    "users": 6,
    "discord_configs": 5,
    "telegram_configs": 5,
    "sessions": 10,
    "messages": 8,
    "pending_messages": 7,
    "turn_runs": 6,
    "devices": 9,
    "workspaces": 6,
    "workspace_members": 3,
    "workspace_deletions": 3,
    "cron_jobs": 11,
}

EXPECTED_INDEXES = {
    ("users_email_key", "users"),
    ("idx_sessions_user_id", "sessions"),
    ("idx_sessions_user_session_key", "sessions"),
    ("idx_messages_session_created", "messages"),
    ("idx_pending_messages_session_received", "pending_messages"),
    ("idx_pending_messages_session_key_received", "pending_messages"),
    ("idx_turn_runs_one_running_per_session", "turn_runs"),
    ("idx_turn_runs_session_started", "turn_runs"),
    ("idx_devices_user_id", "devices"),
    ("devices_user_id_name_key", "devices"),
    ("idx_workspace_members_user", "workspace_members"),
    ("idx_cron_jobs_user_id", "cron_jobs"),
    ("idx_cron_jobs_next_fire", "cron_jobs"),
}


async def test_all_tables_exist(pg_engine):
    async with pg_engine.connect() as conn:
        tables = set(await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names()))
    assert set(EXPECTED_COLUMNS).issubset(tables)


async def test_column_counts(pg_engine):
    async with pg_engine.connect() as conn:
        for table, expected in EXPECTED_COLUMNS.items():
            cols = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_columns(table))
            assert len(cols) == expected, f"{table}: expected {expected}, got {len(cols)}"


async def test_indexes_exist(pg_engine):
    def _collect_indexes(sync_conn):
        indexes = set()
        for table in EXPECTED_COLUMNS:
            for idx in inspect(sync_conn).get_indexes(table):
                indexes.add((idx["name"], table))
        return indexes

    async with pg_engine.connect() as conn:
        indexes = await conn.run_sync(_collect_indexes)
    assert EXPECTED_INDEXES.issubset(indexes)


async def test_device_schema_hashes_tokens_and_rejects_invalid_names(pg_engine):
    async with pg_engine.begin() as conn:
        user_id = (
            await conn.execute(
                text(
                    "INSERT INTO users (email, password_hash, name) "
                    "VALUES ('device-schema@test.com', 'hash', 'Schema') RETURNING id"
                )
            )
        ).scalar_one()

    with pytest.raises(IntegrityError):
        async with pg_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO devices (user_id, name, token_hash, token_hint, workspace_path) "
                    "VALUES (:user_id, 'server', decode(repeat('00', 32), 'hex'), 'hint', '/path')"
                ),
                {"user_id": user_id},
            )

    with pytest.raises(IntegrityError):
        async with pg_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO devices (user_id, name, token_hash, token_hint, workspace_path) "
                    "VALUES (:user_id, :name, decode(repeat('01', 32), 'hex'), 'hint', '/path')"
                ),
                {"user_id": user_id, "name": "a" * 65},
            )

    with pytest.raises(IntegrityError):
        async with pg_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO devices (user_id, name, token_hash, token_hint, workspace_path) "
                    "VALUES (:user_id, 'laptop', decode('00', 'hex'), 'hint', '/path')"
                ),
                {"user_id": user_id},
            )
