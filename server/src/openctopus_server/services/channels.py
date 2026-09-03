from __future__ import annotations

import asyncio
import hashlib
import re
import secrets
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.async_utils import await_future_cancellation_safe
from openctopus_server.channels.types import ExternalChannel
from openctopus_server.db.models import DingTalkConfig, DiscordConfig
from openctopus_server.dto.channel import ChannelState
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ConfigError

PAIRING_CODE_TTL = timedelta(minutes=10)
_DISCORD_USER_ID = re.compile(r"^[0-9]{1,20}$")


@dataclass(frozen=True, slots=True)
class ValidatedBotIdentity:
    identity_id: str
    bot_user_id: str
    display_name: str | None
    avatar_url: str | None


class ChannelCredentialsInvalidError(Exception):
    pass


class ChannelCredentialsUnverifiedError(Exception):
    pass


class ChannelCredentialValidator(Protocol):
    async def validate_discord(self, bot_token: str) -> ValidatedBotIdentity: ...

    async def validate_dingtalk(
        self,
        client_id: str,
        client_secret: str,
    ) -> ValidatedBotIdentity: ...


@dataclass(frozen=True, slots=True)
class SanitizedChannelError:
    code: str
    message: str
    at: datetime


@dataclass(frozen=True, slots=True)
class ChannelRuntimeSnapshot:
    state: ChannelState
    last_error: SanitizedChannelError | None = None


class ChannelConfigRuntime(Protocol):
    def status(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> ChannelRuntimeSnapshot | None: ...

    async def apply(self, user_id: UUID, channel: ExternalChannel) -> None: ...

    async def remove(self, user_id: UUID, channel: ExternalChannel) -> None: ...


@dataclass(frozen=True, slots=True)
class ChannelConfigPatch:
    bot_token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    allow_list: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class ChannelConfigView:
    channel: ExternalChannel
    configured: bool
    state: ChannelState
    bot_id: str | None
    bot_name: str | None
    bot_avatar_url: str | None
    owner_id: str | None
    owner_dm_chat_id: str | None
    allow_list: tuple[str, ...]
    pairing_expires_at: datetime | None
    pairing_code: str | None
    last_error: SanitizedChannelError | None


@dataclass(frozen=True, slots=True)
class _StoredConfig:
    channel: ExternalChannel
    user_id: UUID
    identity_id: str
    credential: str
    secondary_credential: str | None
    bot_user_id: str
    bot_display_name: str | None
    bot_avatar_url: str | None
    binding_generation: UUID
    revision: int
    owner_platform_user_id: str | None
    owner_dm_chat_id: str | None
    paired_at: datetime | None
    allow_list: tuple[str, ...]
    pairing_code_hash: bytes | None
    pairing_expires_at: datetime | None


async def list_configs(
    db: AsyncSession,
    *,
    user_id: UUID,
    runtime: ChannelConfigRuntime,
) -> list[ChannelConfigView]:
    discord = await db.get(DiscordConfig, user_id)
    dingtalk = await db.get(DingTalkConfig, user_id)
    stored = {
        "discord": _stored_discord(discord) if discord is not None else None,
        "dingtalk": _stored_dingtalk(dingtalk) if dingtalk is not None else None,
    }
    await db.rollback()
    return [
        _view(
            channel=channel,
            stored=stored[channel],
            runtime_snapshot=_read_runtime_status(runtime, user_id, channel),
        )
        for channel in ("discord", "dingtalk")
    ]


async def patch_config(
    db: AsyncSession,
    *,
    user_id: UUID,
    channel: str,
    patch: ChannelConfigPatch,
    validator: ChannelCredentialValidator,
    runtime: ChannelConfigRuntime,
) -> ChannelConfigView:
    platform = _external_channel(channel)
    _validate_patch(platform, patch)
    while True:
        current = await _load_stored(db, user_id=user_id, channel=platform)
        await db.rollback()
        credentials = _candidate_credentials(platform, current=current, patch=patch)
        validated: ValidatedBotIdentity | None = None
        if _has_credential_patch(platform, patch) or current is None:
            validated = await _validate_credentials(
                validator,
                channel=platform,
                credentials=credentials,
            )
        persisted = await _persist_patch(
            db,
            user_id=user_id,
            channel=platform,
            patch=patch,
            current=current,
            credentials=credentials,
            validated=validated,
        )
        if persisted is not None:
            stored, pairing_code = persisted
            break

    runtime_snapshot = await _apply_runtime(runtime, user_id, platform)
    return _view(
        channel=platform,
        stored=stored,
        runtime_snapshot=runtime_snapshot,
        pairing_code=pairing_code,
    )


async def _persist_patch(
    db: AsyncSession,
    *,
    user_id: UUID,
    channel: ExternalChannel,
    patch: ChannelConfigPatch,
    current: _StoredConfig | None,
    credentials: tuple[str, str | None],
    validated: ValidatedBotIdentity | None,
) -> tuple[_StoredConfig, str | None] | None:
    pairing_code: str | None = None
    now = datetime.now(UTC)
    try:
        async with db.begin():
            if channel == "discord":
                discord_row = await db.scalar(
                    select(DiscordConfig)
                    .where(DiscordConfig.user_id == user_id)
                    .with_for_update()
                )
                if not _matches_current(discord_row, current):
                    return None
                if discord_row is None:
                    assert validated is not None
                    pairing_code, pairing_hash, pairing_expiry = _new_pairing(now)
                    discord_row = DiscordConfig(
                        user_id=user_id,
                        bot_token=credentials[0],
                        application_id=validated.identity_id,
                        bot_user_id=validated.bot_user_id,
                        bot_display_name=validated.display_name,
                        bot_avatar_url=validated.avatar_url,
                        binding_generation=uuid4(),
                        revision=1,
                        allow_list=list(patch.allow_list or ()),
                        pairing_code_hash=pairing_hash,
                        pairing_expires_at=pairing_expiry,
                        created_at=now,
                        updated_at=now,
                    )
                    await _ensure_identity_available(
                        db,
                        channel=channel,
                        identity_id=validated.identity_id,
                        user_id=user_id,
                    )
                    db.add(discord_row)
                else:
                    if validated is not None:
                        await _ensure_identity_available(
                            db,
                            channel=channel,
                            identity_id=validated.identity_id,
                            user_id=user_id,
                        )
                        identity_changed = (
                            discord_row.application_id != validated.identity_id
                        )
                        discord_row.bot_token = credentials[0]
                        _update_discord_identity(discord_row, validated)
                        if identity_changed:
                            pairing_code = _replace_binding(discord_row, now=now)
                    if patch.allow_list is not None:
                        discord_row.allow_list = list(patch.allow_list)
                    discord_row.revision += 1
                    discord_row.updated_at = now
                await db.flush()
                stored = _stored_discord(discord_row)
            else:
                dingtalk_row = await db.scalar(
                    select(DingTalkConfig)
                    .where(DingTalkConfig.user_id == user_id)
                    .with_for_update()
                )
                if not _matches_current(dingtalk_row, current):
                    return None
                assert credentials[1] is not None
                if dingtalk_row is None:
                    assert validated is not None
                    pairing_code, pairing_hash, pairing_expiry = _new_pairing(now)
                    dingtalk_row = DingTalkConfig(
                        user_id=user_id,
                        client_id=validated.identity_id,
                        client_secret=credentials[1],
                        bot_user_id=validated.bot_user_id,
                        bot_display_name=validated.display_name,
                        bot_avatar_url=validated.avatar_url,
                        binding_generation=uuid4(),
                        revision=1,
                        allow_list=list(patch.allow_list or ()),
                        pairing_code_hash=pairing_hash,
                        pairing_expires_at=pairing_expiry,
                        created_at=now,
                        updated_at=now,
                    )
                    await _ensure_identity_available(
                        db,
                        channel=channel,
                        identity_id=validated.identity_id,
                        user_id=user_id,
                    )
                    db.add(dingtalk_row)
                else:
                    if validated is not None:
                        await _ensure_identity_available(
                            db,
                            channel=channel,
                            identity_id=validated.identity_id,
                            user_id=user_id,
                        )
                        identity_changed = (
                            dingtalk_row.client_id != validated.identity_id
                        )
                        dingtalk_row.client_id = validated.identity_id
                        dingtalk_row.client_secret = credentials[1]
                        _update_dingtalk_identity(dingtalk_row, validated)
                        if identity_changed:
                            pairing_code = _replace_binding(dingtalk_row, now=now)
                    if patch.allow_list is not None:
                        dingtalk_row.allow_list = list(patch.allow_list)
                    dingtalk_row.revision += 1
                    dingtalk_row.updated_at = now
                await db.flush()
                stored = _stored_dingtalk(dingtalk_row)
    except IntegrityError as exc:
        await db.rollback()
        raise ConfigError(
            ErrorCode.CHANNEL_BOT_ALREADY_BOUND,
            "Channel Bot is already bound to another user",
        ) from exc
    return stored, pairing_code


async def rotate_pairing_code(
    db: AsyncSession,
    *,
    user_id: UUID,
    channel: str,
    runtime: ChannelConfigRuntime,
) -> ChannelConfigView:
    platform = _external_channel(channel)
    now = datetime.now(UTC)
    await db.rollback()
    async with db.begin():
        code, digest, expires_at = _new_pairing(now)
        if platform == "discord":
            discord = await db.scalar(
                select(DiscordConfig)
                .where(DiscordConfig.user_id == user_id)
                .with_for_update()
            )
            if discord is None:
                raise _config_not_found()
            _rotate_unpaired(discord, digest=digest, expires_at=expires_at, now=now)
            await db.flush()
            stored = _stored_discord(discord)
        else:
            dingtalk = await db.scalar(
                select(DingTalkConfig)
                .where(DingTalkConfig.user_id == user_id)
                .with_for_update()
            )
            if dingtalk is None:
                raise _config_not_found()
            _rotate_unpaired(dingtalk, digest=digest, expires_at=expires_at, now=now)
            await db.flush()
            stored = _stored_dingtalk(dingtalk)

    runtime_snapshot = await _apply_runtime(runtime, user_id, platform)
    return _view(
        channel=platform,
        stored=stored,
        runtime_snapshot=runtime_snapshot,
        pairing_code=code,
    )


async def delete_config(
    db: AsyncSession,
    *,
    user_id: UUID,
    channel: str,
    runtime: ChannelConfigRuntime,
) -> None:
    platform = _external_channel(channel)
    model = DiscordConfig if platform == "discord" else DingTalkConfig
    await db.execute(delete(model).where(model.user_id == user_id))
    await db.commit()
    removal = asyncio.create_task(runtime.remove(user_id, platform))
    try:
        await await_future_cancellation_safe(removal)
    except Exception:
        pass


async def _load_stored(
    db: AsyncSession,
    *,
    user_id: UUID,
    channel: ExternalChannel,
) -> _StoredConfig | None:
    if channel == "discord":
        discord = await db.get(DiscordConfig, user_id)
        return _stored_discord(discord) if discord is not None else None
    dingtalk = await db.get(DingTalkConfig, user_id)
    return _stored_dingtalk(dingtalk) if dingtalk is not None else None


def _matches_current(
    row: DiscordConfig | DingTalkConfig | None,
    current: _StoredConfig | None,
) -> bool:
    if current is None:
        return row is None
    return row is not None and row.revision == current.revision


def _candidate_credentials(
    channel: ExternalChannel,
    *,
    current: _StoredConfig | None,
    patch: ChannelConfigPatch,
) -> tuple[str, str | None]:
    if channel == "discord":
        token = patch.bot_token or (current.credential if current is not None else None)
        if token is None:
            raise ConfigError(
                ErrorCode.CONFIG_VALIDATION_FAILED,
                "Discord create requires bot_token",
            )
        return token, None

    client_id = patch.client_id or (current.credential if current is not None else None)
    client_secret = patch.client_secret or (
        current.secondary_credential if current is not None else None
    )
    if client_id is None or client_secret is None:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "DingTalk create requires client_id and client_secret",
        )
    return client_id, client_secret


async def _validate_credentials(
    validator: ChannelCredentialValidator,
    *,
    channel: ExternalChannel,
    credentials: tuple[str, str | None],
) -> ValidatedBotIdentity:
    try:
        if channel == "discord":
            validated = await validator.validate_discord(credentials[0])
        else:
            assert credentials[1] is not None
            validated = await validator.validate_dingtalk(
                credentials[0],
                credentials[1],
            )
    except ChannelCredentialsInvalidError as exc:
        raise ConfigError(
            ErrorCode.CHANNEL_CREDENTIALS_INVALID,
            "Channel credentials are invalid",
        ) from exc
    except (ChannelCredentialsUnverifiedError, TimeoutError, OSError) as exc:
        raise ConfigError(
            ErrorCode.CHANNEL_CREDENTIALS_UNVERIFIED,
            "Channel credentials could not be verified",
        ) from exc
    if not validated.identity_id or not validated.bot_user_id:
        raise ConfigError(
            ErrorCode.CHANNEL_CREDENTIALS_UNVERIFIED,
            "Channel identity response is incomplete",
        )
    return validated


async def _ensure_identity_available(
    db: AsyncSession,
    *,
    channel: ExternalChannel,
    identity_id: str,
    user_id: UUID,
) -> None:
    if channel == "discord":
        owner = await db.scalar(
            select(DiscordConfig.user_id).where(
                DiscordConfig.application_id == identity_id,
                DiscordConfig.user_id != user_id,
            )
        )
    else:
        owner = await db.scalar(
            select(DingTalkConfig.user_id).where(
                DingTalkConfig.client_id == identity_id,
                DingTalkConfig.user_id != user_id,
            )
        )
    if owner is not None:
        raise ConfigError(
            ErrorCode.CHANNEL_BOT_ALREADY_BOUND,
            "Channel Bot is already bound to another user",
        )


def _validate_patch(channel: ExternalChannel, patch: ChannelConfigPatch) -> None:
    fields_present = any(
        value is not None
        for value in (
            patch.bot_token,
            patch.client_id,
            patch.client_secret,
            patch.allow_list,
        )
    )
    if not fields_present:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Channel patch must contain at least one field",
        )
    if channel == "discord" and (
        patch.client_id is not None or patch.client_secret is not None
    ):
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "DingTalk credentials are not valid for Discord",
        )
    if channel == "dingtalk" and patch.bot_token is not None:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Discord credentials are not valid for DingTalk",
        )
    if patch.allow_list is None:
        return
    if len(patch.allow_list) > 256 or len(set(patch.allow_list)) != len(
        patch.allow_list
    ):
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Channel allow_list is invalid",
        )
    if channel == "discord":
        valid = all(_DISCORD_USER_ID.fullmatch(value) for value in patch.allow_list)
    else:
        valid = all(
            1 <= len(value) <= 256
            and value.strip() == value
            and not any(unicodedata.category(char) == "Cc" for char in value)
            for value in patch.allow_list
        )
    if not valid:
        raise ConfigError(
            ErrorCode.CONFIG_VALIDATION_FAILED,
            "Channel allow_list is invalid",
        )


def _has_credential_patch(
    channel: ExternalChannel,
    patch: ChannelConfigPatch,
) -> bool:
    if channel == "discord":
        return patch.bot_token is not None
    return patch.client_id is not None or patch.client_secret is not None


def _update_discord_identity(
    row: DiscordConfig,
    identity: ValidatedBotIdentity,
) -> None:
    row.application_id = identity.identity_id
    row.bot_user_id = identity.bot_user_id
    row.bot_display_name = identity.display_name
    row.bot_avatar_url = identity.avatar_url


def _update_dingtalk_identity(
    row: DingTalkConfig,
    identity: ValidatedBotIdentity,
) -> None:
    row.client_id = identity.identity_id
    row.bot_user_id = identity.bot_user_id
    row.bot_display_name = identity.display_name
    row.bot_avatar_url = identity.avatar_url


def _replace_binding(row: DiscordConfig | DingTalkConfig, *, now: datetime) -> str:
    code, digest, expires_at = _new_pairing(now)
    row.binding_generation = uuid4()
    row.owner_platform_user_id = None
    row.owner_dm_chat_id = None
    row.paired_at = None
    row.pairing_code_hash = digest
    row.pairing_expires_at = expires_at
    return code


def _rotate_unpaired(
    row: DiscordConfig | DingTalkConfig,
    *,
    digest: bytes,
    expires_at: datetime,
    now: datetime,
) -> None:
    if row.owner_platform_user_id is not None:
        raise ConfigError(
            ErrorCode.CHANNEL_PAIRING_UNAVAILABLE,
            "Paired channel configuration must be deleted before rebinding",
        )
    row.pairing_code_hash = digest
    row.pairing_expires_at = expires_at
    row.revision += 1
    row.updated_at = now


def _config_not_found() -> ConfigError:
    return ConfigError(
        ErrorCode.CHANNEL_CONFIG_NOT_FOUND,
        "Channel configuration was not found",
    )


def _new_pairing(now: datetime) -> tuple[str, bytes, datetime]:
    code = secrets.token_urlsafe(9)
    return code, hashlib.sha256(code.encode("utf-8")).digest(), now + PAIRING_CODE_TTL


async def _apply_runtime(
    runtime: ChannelConfigRuntime,
    user_id: UUID,
    channel: ExternalChannel,
) -> ChannelRuntimeSnapshot:
    activation = asyncio.create_task(runtime.apply(user_id, channel))
    try:
        await await_future_cancellation_safe(activation)
        snapshot = runtime.status(user_id, channel)
    except Exception:
        snapshot = None
    return snapshot or _runtime_unavailable()


def _read_runtime_status(
    runtime: ChannelConfigRuntime,
    user_id: UUID,
    channel: ExternalChannel,
) -> ChannelRuntimeSnapshot | None:
    try:
        return runtime.status(user_id, channel)
    except Exception:
        return None


def _runtime_unavailable() -> ChannelRuntimeSnapshot:
    return ChannelRuntimeSnapshot(
        state="degraded",
        last_error=SanitizedChannelError(
            code="channel_runtime_unavailable",
            message="Channel runtime is unavailable",
            at=datetime.now(UTC),
        ),
    )


def _view(
    *,
    channel: ExternalChannel,
    stored: _StoredConfig | None,
    runtime_snapshot: ChannelRuntimeSnapshot | None,
    pairing_code: str | None = None,
) -> ChannelConfigView:
    if stored is None:
        return ChannelConfigView(
            channel=channel,
            configured=False,
            state="stopped",
            bot_id=None,
            bot_name=None,
            bot_avatar_url=None,
            owner_id=None,
            owner_dm_chat_id=None,
            allow_list=(),
            pairing_expires_at=None,
            pairing_code=None,
            last_error=None,
        )
    snapshot = runtime_snapshot or _runtime_unavailable()
    error = snapshot.last_error
    if error is not None:
        error = SanitizedChannelError(
            code=error.code,
            message=error.message[:512],
            at=error.at,
        )
    return ChannelConfigView(
        channel=channel,
        configured=True,
        state=snapshot.state,
        bot_id=stored.identity_id,
        bot_name=stored.bot_display_name,
        bot_avatar_url=stored.bot_avatar_url,
        owner_id=stored.owner_platform_user_id,
        owner_dm_chat_id=stored.owner_dm_chat_id,
        allow_list=stored.allow_list,
        pairing_expires_at=(
            stored.pairing_expires_at
            if stored.owner_platform_user_id is None
            else None
        ),
        pairing_code=pairing_code,
        last_error=error,
    )


def _stored_discord(row: DiscordConfig) -> _StoredConfig:
    return _StoredConfig(
        channel="discord",
        user_id=row.user_id,
        identity_id=row.application_id,
        credential=row.bot_token,
        secondary_credential=None,
        bot_user_id=row.bot_user_id,
        bot_display_name=row.bot_display_name,
        bot_avatar_url=row.bot_avatar_url,
        binding_generation=row.binding_generation,
        revision=row.revision,
        owner_platform_user_id=row.owner_platform_user_id,
        owner_dm_chat_id=row.owner_dm_chat_id,
        paired_at=row.paired_at,
        allow_list=tuple(str(value) for value in row.allow_list),
        pairing_code_hash=row.pairing_code_hash,
        pairing_expires_at=row.pairing_expires_at,
    )


def _stored_dingtalk(row: DingTalkConfig) -> _StoredConfig:
    return _StoredConfig(
        channel="dingtalk",
        user_id=row.user_id,
        identity_id=row.client_id,
        credential=row.client_id,
        secondary_credential=row.client_secret,
        bot_user_id=row.bot_user_id,
        bot_display_name=row.bot_display_name,
        bot_avatar_url=row.bot_avatar_url,
        binding_generation=row.binding_generation,
        revision=row.revision,
        owner_platform_user_id=row.owner_platform_user_id,
        owner_dm_chat_id=row.owner_dm_chat_id,
        paired_at=row.paired_at,
        allow_list=tuple(str(value) for value in row.allow_list),
        pairing_code_hash=row.pairing_code_hash,
        pairing_expires_at=row.pairing_expires_at,
    )


def _external_channel(channel: str) -> ExternalChannel:
    if channel == "discord":
        return "discord"
    if channel == "dingtalk":
        return "dingtalk"
    raise ConfigError(
        ErrorCode.CHANNEL_NOT_SUPPORTED,
        "Channel is not supported",
    )
