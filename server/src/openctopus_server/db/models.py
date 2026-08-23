import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    LargeBinary,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func, text

from openctopus_server.db.base import Base
from openctopus_server.network_policy import DEFAULT_SSRF_DENYLIST_JSON


class SystemConfig(Base):
    __tablename__ = "system_config"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[Any] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class DiscordConfig(Base):
    __tablename__ = "discord_configs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    bot_token: Mapped[str] = mapped_column(Text, nullable=False)
    partner_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    allow_list: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class TelegramConfig(Base):
    __tablename__ = "telegram_configs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    bot_token: Mapped[str] = mapped_column(Text, nullable=False)
    partner_chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    allow_list: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class Session(Base):
    __tablename__ = "sessions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    session_key: Mapped[str] = mapped_column(Text, nullable=False)
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'New chat'"))
    last_inbound_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_read_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    cancel_requested: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("FALSE")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (UniqueConstraint("user_id", "session_key"),)


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False
    )
    message_kind: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    delivery_refs: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    llm_fingerprint: Mapped[str | None] = mapped_column(Text)
    is_compacted: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("FALSE")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "message_kind IN ('human','assistant','tool_result','synthetic_tool_result','synthetic_assistant_error','compaction_summary')",
            name="check_message_kind",
        ),
    )


class PendingMessage(Base):
    __tablename__ = "pending_messages"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    session_key: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    effort: Mapped[str | None] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "effort IS NULL OR effort IN ('off','low','medium','high','xhigh','max')",
            name="check_pending_message_effort",
        ),
    )


class TurnRun(Base):
    __tablename__ = "turn_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="CASCADE"), nullable=False
    )
    runner_instance_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('running','completed','failed','abandoned','cancelled')",
            name="check_turn_run_status",
        ),
    )


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    token_hash: Mapped[bytes] = mapped_column(LargeBinary(32), nullable=False, unique=True)
    token_hint: Mapped[str] = mapped_column(Text, nullable=False)
    workspace_path: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'~/openoctopus/workspace'")
    )
    restrict_to_workspace: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("TRUE")
    )
    ssrf_denylist: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(f"'{DEFAULT_SSRF_DENYLIST_JSON}'::jsonb"),
    )
    shell_timeout_max: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("600")
    )
    env_allowlist: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(
            "'[\"PATH\",\"HOME\",\"LANG\",\"TERM\",\"SystemRoot\",\"ComSpec\",\"PATHEXT\",\"TEMP\",\"TMP\",\"USERPROFILE\"]'::jsonb"
        ),
    )
    mcp_servers: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )
    mcp_catalog: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(
            "'{\"version\": 1, \"digest\": \"d5f4bb30627f342c5625dfe6a6d7a282874bd8121b32dbdd2004756e4b1ad8cf\", \"servers\": []}'::jsonb"
        ),
    )
    config_revision: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("1"),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("user_id", "name"),
        CheckConstraint(
            "char_length(name) <= 64 "
            "AND name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' "
            "AND name <> 'server'",
            name="check_device_name",
        ),
        CheckConstraint(
            "octet_length(token_hash) = 32",
            name="check_device_token_hash_length",
        ),
        CheckConstraint(
            "shell_timeout_max >= 0 AND shell_timeout_max <= 86400",
            name="check_device_shell_timeout_max",
        ),
        CheckConstraint(
            "config_revision >= 1",
            name="check_device_config_revision",
        ),
    )


class Workspace(Base):
    __tablename__ = "workspaces"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    suffix: Mapped[str] = mapped_column(Text, nullable=False)
    quota_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class WorkspaceMember(Base):
    __tablename__ = "workspace_members"

    workspace_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("workspaces.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    joined_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class WorkspaceDeletion(Base):
    __tablename__ = "workspace_deletions"

    kind: Mapped[str] = mapped_column(Text, primary_key=True)
    target_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "kind IN ('personal', 'shared')",
            name="check_workspace_deletion_kind",
        ),
    )


class CronJob(Base):
    __tablename__ = "cron_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    session_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sessions.id"), nullable=False
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    schedule: Mapped[str] = mapped_column(Text, nullable=False)
    tz: Mapped[str | None] = mapped_column(Text)
    one_shot: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    message: Mapped[str] = mapped_column(Text, nullable=False)
    last_fired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    next_fire_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


Index("idx_sessions_user_id", Session.user_id)
Index("idx_sessions_user_session_key", Session.user_id, Session.session_key, unique=True)
Index("idx_messages_session_created", Message.session_id, Message.created_at)
Index(
    "idx_pending_messages_session_received",
    PendingMessage.session_id,
    PendingMessage.received_at,
    PendingMessage.id,
)
Index(
    "idx_pending_messages_session_key_received",
    PendingMessage.session_key,
    PendingMessage.received_at,
    PendingMessage.id,
)
Index(
    "idx_turn_runs_one_running_per_session",
    TurnRun.session_id,
    unique=True,
    postgresql_where=text("status = 'running'"),
)
Index(
    "idx_turn_runs_session_started",
    TurnRun.session_id,
    TurnRun.started_at.desc(),
    TurnRun.id.desc(),
)
Index("idx_devices_user_id", Device.user_id)
Index("idx_workspace_members_user", WorkspaceMember.user_id)
Index("idx_cron_jobs_user_id", CronJob.user_id)
Index(
    "idx_cron_jobs_next_fire",
    CronJob.next_fire_at,
    postgresql_where=text("next_fire_at IS NOT NULL"),
)
