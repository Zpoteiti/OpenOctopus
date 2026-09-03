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
    timezone: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'UTC'")
    )
    is_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("FALSE"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "char_length(timezone) BETWEEN 1 AND 64",
            name="check_user_timezone_length",
        ),
    )


class DiscordConfig(Base):
    __tablename__ = "discord_configs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    bot_token: Mapped[str] = mapped_column(Text, nullable=False)
    application_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    bot_user_id: Mapped[str] = mapped_column(Text, nullable=False)
    bot_display_name: Mapped[str | None] = mapped_column(Text)
    bot_avatar_url: Mapped[str | None] = mapped_column(Text)
    binding_generation: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    revision: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("1")
    )
    owner_platform_user_id: Mapped[str | None] = mapped_column(Text)
    owner_dm_chat_id: Mapped[str | None] = mapped_column(Text)
    paired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    allow_list: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    pairing_code_hash: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    pairing_expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("revision >= 1", name="check_discord_config_revision"),
        CheckConstraint(
            "jsonb_typeof(allow_list) = 'array'",
            name="check_discord_allow_list_array",
        ),
        CheckConstraint(
            "pairing_code_hash IS NULL OR octet_length(pairing_code_hash) = 32",
            name="check_discord_pairing_hash_length",
        ),
        CheckConstraint(
            "(owner_platform_user_id IS NULL AND owner_dm_chat_id IS NULL AND paired_at IS NULL) "
            "OR (owner_platform_user_id IS NOT NULL AND owner_dm_chat_id IS NOT NULL "
            "AND paired_at IS NOT NULL)",
            name="check_discord_pairing_identity",
        ),
        CheckConstraint(
            "(pairing_code_hash IS NULL) = (pairing_expires_at IS NULL)",
            name="check_discord_pairing_code_state",
        ),
    )


class DingTalkConfig(Base):
    __tablename__ = "dingtalk_configs"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    client_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    client_secret: Mapped[str] = mapped_column(Text, nullable=False)
    bot_user_id: Mapped[str] = mapped_column(Text, nullable=False)
    bot_display_name: Mapped[str | None] = mapped_column(Text)
    bot_avatar_url: Mapped[str | None] = mapped_column(Text)
    binding_generation: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    revision: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("1")
    )
    owner_platform_user_id: Mapped[str | None] = mapped_column(Text)
    owner_dm_chat_id: Mapped[str | None] = mapped_column(Text)
    paired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    allow_list: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    pairing_code_hash: Mapped[bytes | None] = mapped_column(LargeBinary(32))
    pairing_expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("revision >= 1", name="check_dingtalk_config_revision"),
        CheckConstraint(
            "jsonb_typeof(allow_list) = 'array'",
            name="check_dingtalk_allow_list_array",
        ),
        CheckConstraint(
            "pairing_code_hash IS NULL OR octet_length(pairing_code_hash) = 32",
            name="check_dingtalk_pairing_hash_length",
        ),
        CheckConstraint(
            "(owner_platform_user_id IS NULL AND owner_dm_chat_id IS NULL AND paired_at IS NULL) "
            "OR (owner_platform_user_id IS NOT NULL AND owner_dm_chat_id IS NOT NULL "
            "AND paired_at IS NOT NULL)",
            name="check_dingtalk_pairing_identity",
        ),
        CheckConstraint(
            "(pairing_code_hash IS NULL) = (pairing_expires_at IS NULL)",
            name="check_dingtalk_pairing_code_state",
        ),
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
    attachment_refs: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    delivery_refs: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    sender_id: Mapped[str | None] = mapped_column(Text)
    sender_display_name: Mapped[str | None] = mapped_column(Text)
    sender_classification: Mapped[str | None] = mapped_column(Text)
    ingress_tool_profile: Mapped[str | None] = mapped_column(Text)
    source_message_id: Mapped[str | None] = mapped_column(Text)
    channel_binding_generation: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    channel_context: Mapped[list[dict[str, Any]]] = mapped_column(
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
        CheckConstraint(
            "sender_classification IS NULL "
            "OR sender_classification IN ('owner','allowed_non_owner','internal')",
            name="check_message_sender_classification",
        ),
        CheckConstraint(
            "ingress_tool_profile IS NULL "
            "OR ingress_tool_profile IN ('owner_full','message_only')",
            name="check_message_ingress_tool_profile",
        ),
        CheckConstraint(
            "message_kind <> 'human' "
            "OR (sender_id IS NOT NULL AND sender_classification IS NOT NULL "
            "AND ingress_tool_profile IS NOT NULL)",
            name="check_human_message_authority_present",
        ),
        CheckConstraint(
            "message_kind = 'human' "
            "OR (sender_id IS NULL AND sender_display_name IS NULL "
            "AND sender_classification IS NULL AND ingress_tool_profile IS NULL "
            "AND source_message_id IS NULL AND channel_binding_generation IS NULL "
            "AND channel_context = '[]'::jsonb)",
            name="check_nonhuman_message_authority_absent",
        ),
        CheckConstraint(
            "sender_classification IS NULL "
            "OR (sender_classification = 'owner' "
            "AND ingress_tool_profile = 'owner_full') "
            "OR (sender_classification = 'allowed_non_owner' "
            "AND ingress_tool_profile = 'message_only') "
            "OR sender_classification = 'internal'",
            name="check_message_authority_profile",
        ),
        CheckConstraint(
            "jsonb_typeof(channel_context) = 'array'",
            name="check_message_channel_context_array",
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
    attachment_refs: Mapped[list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    sender_id: Mapped[str] = mapped_column(Text, nullable=False)
    sender_display_name: Mapped[str | None] = mapped_column(Text)
    sender_classification: Mapped[str] = mapped_column(Text, nullable=False)
    ingress_tool_profile: Mapped[str] = mapped_column(Text, nullable=False)
    source_message_id: Mapped[str | None] = mapped_column(Text)
    channel_binding_generation: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    channel_context: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    effort: Mapped[str | None] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "effort IS NULL OR effort IN ('off','low','medium','high','xhigh','max')",
            name="check_pending_message_effort",
        ),
        CheckConstraint(
            "sender_classification IN ('owner','allowed_non_owner','internal')",
            name="check_pending_sender_classification",
        ),
        CheckConstraint(
            "ingress_tool_profile IN ('owner_full','message_only')",
            name="check_pending_ingress_tool_profile",
        ),
        CheckConstraint(
            "(sender_classification IN ('owner','internal') "
            "AND ingress_tool_profile = 'owner_full') "
            "OR (sender_classification = 'allowed_non_owner' "
            "AND ingress_tool_profile = 'message_only')",
            name="check_pending_authority_profile",
        ),
        CheckConstraint(
            "jsonb_typeof(channel_context) = 'array'",
            name="check_pending_channel_context_array",
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
    tool_profile: Mapped[str] = mapped_column(Text, nullable=False)
    input_message_ids: Mapped[list[str]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    failed_delivery_targets: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb")
    )
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "status IN ('running','completed','failed','abandoned','cancelled')",
            name="check_turn_run_status",
        ),
        CheckConstraint(
            "tool_profile IN ('owner_full','message_only')",
            name="check_turn_run_tool_profile",
        ),
        CheckConstraint(
            "jsonb_typeof(input_message_ids) = 'array'",
            name="check_turn_run_input_message_ids_array",
        ),
        CheckConstraint(
            "jsonb_typeof(failed_delivery_targets) = 'array'",
            name="check_turn_run_failed_delivery_targets_array",
        ),
    )


class ChannelMessageReceipt(Base):
    __tablename__ = "channel_message_receipts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    session_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="SET NULL")
    )
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    binding_generation: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    source_message_id: Mapped[str] = mapped_column(Text, nullable=False)
    disposition: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "channel",
            "binding_generation",
            "chat_id",
            "source_message_id",
            name="uq_channel_receipt_source",
        ),
        CheckConstraint(
            "channel IN ('discord','dingtalk')",
            name="check_channel_receipt_channel",
        ),
        CheckConstraint(
            "disposition IN ('context','context_omitted','trigger','attachment_rejected')",
            name="check_channel_receipt_disposition",
        ),
    )


class ChannelDelivery(Base):
    __tablename__ = "channel_deliveries"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    session_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sessions.id", ondelete="CASCADE")
    )
    turn_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("turn_runs.id", ondelete="SET NULL")
    )
    assistant_message_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("messages.id", ondelete="SET NULL")
    )
    tool_use_id: Mapped[str | None] = mapped_column(Text)
    delivery_key: Mapped[str] = mapped_column(Text, nullable=False)
    origin: Mapped[str] = mapped_column(Text, nullable=False)
    channel: Mapped[str] = mapped_column(Text, nullable=False)
    chat_id: Mapped[str] = mapped_column(Text, nullable=False)
    binding_generation: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    total_actions: Mapped[int] = mapped_column(Integer, nullable=False)
    visible_sent_actions: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    __table_args__ = (
        UniqueConstraint("user_id", "delivery_key", name="uq_channel_delivery_key"),
        CheckConstraint(
            "origin IN ('final','message_tool','policy_notice','pairing_confirmation')",
            name="check_channel_delivery_origin",
        ),
        CheckConstraint(
            "channel IN ('discord','dingtalk')",
            name="check_channel_delivery_channel",
        ),
        CheckConstraint(
            "status IN ('prepared','attempting','sent','partial','failed','unknown')",
            name="check_channel_delivery_status",
        ),
        CheckConstraint(
            "total_actions >= 0 AND total_actions <= 32",
            name="check_channel_delivery_total_actions",
        ),
        CheckConstraint(
            "visible_sent_actions >= 0 AND visible_sent_actions <= total_actions",
            name="check_channel_delivery_visible_actions",
        ),
    )


class ChannelDeliveryAction(Base):
    __tablename__ = "channel_delivery_actions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    delivery_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("channel_deliveries.id", ondelete="CASCADE"),
        nullable=False,
    )
    action_index: Mapped[int] = mapped_column(Integer, nullable=False)
    action_kind: Mapped[str] = mapped_column(Text, nullable=False)
    visible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    platform_message_id: Mapped[str | None] = mapped_column(Text)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    last_error_message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))

    __table_args__ = (
        UniqueConstraint(
            "delivery_id",
            "action_index",
            name="uq_channel_delivery_action_index",
        ),
        CheckConstraint(
            "action_index >= 0 AND action_index < 32",
            name="check_channel_delivery_action_index",
        ),
        CheckConstraint(
            "action_kind IN ('text_message','file_upload','file_message')",
            name="check_channel_delivery_action_kind",
        ),
        CheckConstraint(
            "status IN ('prepared','attempting','sent','failed','unknown','skipped')",
            name="check_channel_delivery_action_status",
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
    name: Mapped[str] = mapped_column(Text, nullable=False)
    schedule_kind: Mapped[str] = mapped_column(Text, nullable=False)
    schedule_value: Mapped[str] = mapped_column(Text, nullable=False)
    timezone: Mapped[str | None] = mapped_column(Text)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    last_fired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    next_fire_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(
            "schedule_kind IN ('every', 'cron', 'at')",
            name="check_cron_schedule_kind",
        ),
        CheckConstraint(
            "(schedule_kind = 'every' AND timezone IS NULL) "
            "OR (schedule_kind IN ('cron', 'at') AND timezone IS NOT NULL)",
            name="check_cron_schedule_timezone",
        ),
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
Index("idx_channel_deliveries_status", ChannelDelivery.status)
Index("idx_devices_user_id", Device.user_id)
Index("idx_workspace_members_user", WorkspaceMember.user_id)
Index("idx_cron_jobs_user_id", CronJob.user_id)
Index(
    "idx_cron_jobs_next_fire",
    CronJob.next_fire_at,
    CronJob.id,
)
