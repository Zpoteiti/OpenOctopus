import pytest
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError

EXPECTED_COLUMNS = {
    "system_config": 3,
    "users": 6,
    "discord_configs": 5,
    "telegram_configs": 5,
    "sessions": 10,
    "messages": 9,
    "pending_messages": 7,
    "devices": 11,
    "workspaces": 5,
    "workspace_members": 3,
    "cron_jobs": 11,
}

EXPECTED_INDEXES = {
    ("users_email_key", "users"),
    ("idx_sessions_user_id", "sessions"),
    ("idx_sessions_user_session_key", "sessions"),
    ("idx_messages_session_created", "messages"),
    ("idx_pending_messages_session_received", "pending_messages"),
    ("idx_pending_messages_session_key_received", "pending_messages"),
    ("idx_devices_user_id", "devices"),
    ("devices_user_id_name_key", "devices"),
    ("idx_workspace_members_user", "workspace_members"),
    ("idx_cron_jobs_user_id", "cron_jobs"),
    ("idx_cron_jobs_next_fire", "cron_jobs"),
}


@pytest.mark.asyncio
async def test_all_tables_exist(pg_engine):
    async with pg_engine.connect() as conn:
        tables = set(await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_table_names()))
    assert set(EXPECTED_COLUMNS).issubset(tables)


@pytest.mark.asyncio
async def test_column_counts(pg_engine):
    async with pg_engine.connect() as conn:
        for table, expected in EXPECTED_COLUMNS.items():
            cols = await conn.run_sync(lambda sync_conn: inspect(sync_conn).get_columns(table))
            assert len(cols) == expected, f"{table}: expected {expected}, got {len(cols)}"


@pytest.mark.asyncio
async def test_indexes_exist(pg_engine):
    async with pg_engine.connect() as conn:
        indexes = await conn.run_sync(
            lambda sync_conn: {
                (idx["name"], idx["table_name"]) for idx in inspect(sync_conn).get_indexes()
            }
        )
    assert EXPECTED_INDEXES.issubset(indexes)


@pytest.mark.asyncio
async def test_shell_timeout_max_check(pg_engine):
    from sqlalchemy import text

    async with pg_engine.begin() as conn:
        with pytest.raises(IntegrityError):
            await conn.execute(
                text(
                    "INSERT INTO devices (token, user_id, name, workspace_path, shell_timeout_max) "
                    "VALUES ('t1', gen_random_uuid(), 'dev', '/path', -1)"
                )
            )
