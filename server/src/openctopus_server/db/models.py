import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func, text

from openctopus_server.db.base import Base


class SystemConfig(Base):
    __tablename__ = "system_config"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
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
    is_admin: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("FALSE")
    )
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
    title: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'New chat'")
    )
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
    role: Mapped[str] = mapped_column(Text, nullable=False)
    message_kind: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    delivery_refs: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    llm_fingerprint: Mapped[str | None] = mapped_column(Text)
    is_compaction_summary: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("FALSE")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("role IN ('user','assistant')", name="check_message_role"),
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
    content: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
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


class Device(Base):
    __tablename__ = "devices"

    token: Mapped[str] = mapped_column(Text, primary_key=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    workspace_path: Mapped[str] = mapped_column(Text, nullable=False)
    sandbox_mode: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("TRUE")
    )
    shell_timeout_max: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("600")
    )
    ssrf_denylist: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(
            "'[\"127.0.0.0/8\",\"::1/128\",\"10.0.0.0/8\",\"172.16.0.0/12\",\"192.168.0.0/16\",\"100.64.0.0/10\",\"169.254.0.0/16\",\"169.254.169.254/32\",\"fc00::/7\",\"fe80::/10\"]'::jsonb"
        ),
    )
    env_allowlist: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[\"PATH\",\"HOME\",\"LANG\",\"TERM\"]'::jsonb"),
    )
    command_denylist: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text(
            "'[\"shutdown\",\"reboot\",\"halt\",\"poweroff\",\"mkfs\",\"dd\",\"mount\",\"umount\",\"systemctl\",\"service\"]'::jsonb"
        ),
    )
    mcp_servers: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("user_id", "name"),
        CheckConstraint("shell_timeout_max >= 0", name="check_shell_timeout_max_non_negative"),
        CheckConstraint(
            "name ~ '^[a-z0-9]+(-[a-z0-9]+)*$' AND name <> 'server'",
            name="check_device_name",
        ),
    )


class Workspace(Base):
    __tablename__ = "workspaces"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
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
    one_shot: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("FALSE")
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    last_fired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    next_fire_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False
    )
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
Index("idx_devices_user_id", Device.user_id)
Index("idx_workspace_members_user", WorkspaceMember.user_id)
Index("idx_cron_jobs_user_id", CronJob.user_id)
Index(
    "idx_cron_jobs_next_fire",
    CronJob.next_fire_at,
    postgresql_where=text("next_fire_at IS NOT NULL"),
)
