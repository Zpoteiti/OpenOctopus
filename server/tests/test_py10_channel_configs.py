from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from openctopus_server.api.channels import (
    get_channel_credential_validator,
    get_channel_runtime,
)
from openctopus_server.channels.adapters.dingtalk import DingTalkCredentialValidator
from openctopus_server.channels.types import ExternalChannel
from openctopus_server.db.models import DingTalkConfig, DiscordConfig, User
from openctopus_server.errors.exceptions import ConfigError
from openctopus_server.services import channels
from openctopus_server.services.channels import (
    ChannelConfigPatch,
    ChannelCredentialsInvalidError,
    ChannelCredentialsUnverifiedError,
    ChannelRuntimeSnapshot,
    SanitizedChannelError,
    ValidatedBotIdentity,
)


class _Validator:
    def __init__(self) -> None:
        self.discord: dict[str, ValidatedBotIdentity | Exception] = {}
        self.dingtalk: dict[tuple[str, str], ValidatedBotIdentity | Exception] = {}
        self.calls: list[tuple[object, ...]] = []
        self.transaction_probe: Any = None

    async def validate_discord(self, bot_token: str) -> ValidatedBotIdentity:
        self.calls.append(("discord", bot_token))
        if self.transaction_probe is not None:
            self.transaction_probe()
        result = self.discord[bot_token]
        if isinstance(result, Exception):
            raise result
        return result

    async def validate_dingtalk(
        self,
        client_id: str,
        client_secret: str,
    ) -> ValidatedBotIdentity:
        self.calls.append(("dingtalk", client_id, client_secret))
        if self.transaction_probe is not None:
            self.transaction_probe()
        result = self.dingtalk[(client_id, client_secret)]
        if isinstance(result, Exception):
            raise result
        return result


class _Runtime:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.apply_calls: list[tuple[UUID, ExternalChannel]] = []
        self.remove_calls: list[tuple[UUID, ExternalChannel]] = []
        self.statuses: dict[tuple[UUID, ExternalChannel], ChannelRuntimeSnapshot] = {}
        self.fail_apply = False
        self.fail_remove = False

    def status(
        self,
        user_id: UUID,
        channel: ExternalChannel,
    ) -> ChannelRuntimeSnapshot | None:
        return self.statuses.get((user_id, channel))

    async def apply(self, user_id: UUID, channel: ExternalChannel) -> None:
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            if channel == "discord":
                discord = await db.get(DiscordConfig, user_id)
                assert discord is not None, (
                    "runtime apply must observe the committed config"
                )
                owner_id = discord.owner_platform_user_id
            else:
                dingtalk = await db.get(DingTalkConfig, user_id)
                assert dingtalk is not None, (
                    "runtime apply must observe the committed config"
                )
                owner_id = dingtalk.owner_platform_user_id
        self.apply_calls.append((user_id, channel))
        if self.fail_apply:
            raise RuntimeError("raw secret must not escape")
        self.statuses[(user_id, channel)] = ChannelRuntimeSnapshot(
            state="ready" if owner_id is not None else "awaiting_pairing"
        )

    async def remove(self, user_id: UUID, channel: ExternalChannel) -> None:
        model = DiscordConfig if channel == "discord" else DingTalkConfig
        async with AsyncSession(self.engine, expire_on_commit=False) as db:
            row = await db.get(model, user_id)
        assert row is None, "runtime remove must run after the delete commit"
        self.remove_calls.append((user_id, channel))
        if self.fail_remove:
            raise RuntimeError("stop failed")
        self.statuses.pop((user_id, channel), None)


class _BlockingRuntime(_Runtime):
    def __init__(self, engine: AsyncEngine) -> None:
        super().__init__(engine)
        self.apply_entered = asyncio.Event()
        self.release_apply = asyncio.Event()
        self.apply_completed = asyncio.Event()
        self.remove_entered = asyncio.Event()
        self.release_remove = asyncio.Event()
        self.remove_completed = asyncio.Event()

    async def apply(self, user_id: UUID, channel: ExternalChannel) -> None:
        self.apply_entered.set()
        await self.release_apply.wait()
        await super().apply(user_id, channel)
        self.apply_completed.set()

    async def remove(self, user_id: UUID, channel: ExternalChannel) -> None:
        self.remove_entered.set()
        await self.release_remove.wait()
        await super().remove(user_id, channel)
        self.remove_completed.set()


class _ConcurrentDingTalkValidator:
    def __init__(self, engine: AsyncEngine, user_id: UUID) -> None:
        self.engine = engine
        self.user_id = user_id
        self.calls: list[tuple[str, str]] = []
        self.mutated = False

    async def validate_discord(self, bot_token: str) -> ValidatedBotIdentity:
        raise AssertionError(f"unexpected Discord validation: {bot_token}")

    async def validate_dingtalk(
        self,
        client_id: str,
        client_secret: str,
    ) -> ValidatedBotIdentity:
        self.calls.append((client_id, client_secret))
        if not self.mutated:
            self.mutated = True
            async with AsyncSession(self.engine) as db:
                row = await db.get(DingTalkConfig, self.user_id)
                assert row is not None
                row.client_secret = "secret-2"
                row.revision += 1
                await db.commit()
        return _identity(client_id)


def _identity(identity_id: str, *, name: str = "Bob") -> ValidatedBotIdentity:
    return ValidatedBotIdentity(
        identity_id=identity_id,
        bot_user_id=f"user-{identity_id}",
        display_name=name,
        avatar_url=f"https://cdn.example/{identity_id}.png",
    )


def _install(test_app: FastAPI, validator: _Validator, runtime: _Runtime) -> None:
    test_app.dependency_overrides[get_channel_credential_validator] = lambda: validator
    test_app.dependency_overrides[get_channel_runtime] = lambda: runtime


async def _current_user_id(engine: AsyncEngine) -> UUID:
    async with AsyncSession(engine) as db:
        user_id = await db.scalar(select(User.id).where(User.email == "user@test.com"))
    assert isinstance(user_id, UUID)
    return user_id


async def test_get_is_fixed_shape_and_patch_is_strict(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    _install(test_app, _Validator(), _Runtime(pg_engine))

    response = await user_client.get("/api/channels")

    assert response.status_code == 200
    assert response.json() == [
        {
            "channel": "discord",
            "configured": False,
            "state": "stopped",
            "bot": None,
            "owner": None,
            "allow_list": [],
            "credential_hint": None,
            "pairing": None,
            "last_error": None,
        },
        {
            "channel": "dingtalk",
            "configured": False,
            "state": "stopped",
            "bot": None,
            "owner": None,
            "allow_list": [],
            "credential_hint": None,
            "pairing": None,
            "last_error": None,
        },
    ]
    assert (
        await user_client.patch(
            "/api/channels/discord",
            json={"bot_token": "token", "unknown": True},
        )
    ).status_code == 400
    assert (
        await user_client.patch(
            "/api/channels/discord",
            json={"allow_list": []},
        )
    ).status_code == 400
    assert (
        await user_client.patch(
            "/api/channels/dingtalk",
            json={"client_id": "client-only"},
        )
    ).status_code == 400


async def test_discord_create_returns_pairing_code_once_and_never_returns_secret(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.discord["super-secret-token"] = _identity("application-1")
    runtime = _Runtime(pg_engine)
    _install(test_app, validator, runtime)

    created = await user_client.patch(
        "/api/channels/discord",
        json={
            "bot_token": "super-secret-token",
            "allow_list": ["123", "456"],
        },
    )

    assert created.status_code == 200
    body = created.json()
    code = body["pairing"]["code"]
    assert isinstance(code, str) and code
    assert body["state"] == "awaiting_pairing"
    assert body["bot"] == {
        "id": "application-1",
        "name": "Bob",
        "avatar_url": "https://cdn.example/application-1.png",
    }
    assert body["credential_hint"] == "Configured"
    assert "super-secret-token" not in created.text
    assert "suffix" not in created.text
    assert "hash" not in created.text

    user_id = await _current_user_id(pg_engine)
    async with AsyncSession(pg_engine) as db:
        row = await db.get(DiscordConfig, user_id)
    assert row is not None
    assert row.bot_token == "super-secret-token"
    assert row.pairing_code_hash == hashlib.sha256(code.encode()).digest()
    assert len(row.pairing_code_hash) == 32
    assert row.pairing_expires_at is not None
    assert timedelta(minutes=9, seconds=50) <= (
        row.pairing_expires_at - datetime.now(UTC)
    ) <= timedelta(minutes=10)
    assert runtime.apply_calls == [(user_id, "discord")]

    listed = await user_client.get("/api/channels")
    discord = listed.json()[0]
    assert discord["pairing"]["code"] is None
    assert "super-secret-token" not in listed.text
    assert "suffix" not in listed.text
    assert "hash" not in listed.text


async def test_validation_is_outside_transaction_and_failures_preserve_old_value(
    pg_engine: AsyncEngine,
) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        user = User(
            email="transaction@test.com",
            password_hash="hash",
            name="Transaction",
        )
        db.add(user)
        await db.commit()
        user_id = user.id

        validator = _Validator()
        validator.discord["old-token"] = _identity("application-1")
        validator.transaction_probe = lambda: _assert_no_transaction(db)
        runtime = _Runtime(pg_engine)
        await channels.patch_config(
            db,
            user_id=user_id,
            channel="discord",
            patch=ChannelConfigPatch(bot_token="old-token"),
            validator=validator,
            runtime=runtime,
        )

        validator.discord["bad-token"] = ChannelCredentialsInvalidError()
        try:
            await channels.patch_config(
                db,
                user_id=user_id,
                channel="discord",
                patch=ChannelConfigPatch(bot_token="bad-token"),
                validator=validator,
                runtime=runtime,
            )
        except ConfigError as exc:
            assert exc.code.value == "channel_credentials_invalid"
        else:
            raise AssertionError("invalid credentials must fail")

        validator.discord["offline-token"] = ChannelCredentialsUnverifiedError()
        try:
            await channels.patch_config(
                db,
                user_id=user_id,
                channel="discord",
                patch=ChannelConfigPatch(bot_token="offline-token"),
                validator=validator,
                runtime=runtime,
            )
        except ConfigError as exc:
            assert exc.code.value == "channel_credentials_unverified"
        else:
            raise AssertionError("unverified credentials must fail")

    async with AsyncSession(pg_engine) as db:
        row = await db.get(DiscordConfig, user_id)
    assert row is not None
    assert row.bot_token == "old-token"
    assert len(runtime.apply_calls) == 1


async def test_credential_failures_have_stable_http_statuses(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.discord["bad-token"] = ChannelCredentialsInvalidError()
    validator.discord["offline-token"] = TimeoutError()
    _install(test_app, validator, _Runtime(pg_engine))

    invalid = await user_client.patch(
        "/api/channels/discord",
        json={"bot_token": "bad-token"},
    )
    unavailable = await user_client.patch(
        "/api/channels/discord",
        json={"bot_token": "offline-token"},
    )

    assert invalid.status_code == 400
    assert invalid.json()["code"] == "channel_credentials_invalid"
    assert unavailable.status_code == 503
    assert unavailable.json()["code"] == "channel_credentials_unverified"


def _assert_no_transaction(db: AsyncSession) -> None:
    assert db.in_transaction() is False


async def test_same_identity_rotation_preserves_pairing_and_replacement_resets_it(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.discord["token-1"] = _identity("application-1")
    validator.discord["token-2"] = _identity("application-1", name="Bob 2")
    validator.discord["token-new-bot"] = _identity("application-2")
    runtime = _Runtime(pg_engine)
    _install(test_app, validator, runtime)
    await user_client.patch(
        "/api/channels/discord",
        json={"bot_token": "token-1"},
    )
    user_id = await _current_user_id(pg_engine)
    paired_at = datetime.now(UTC)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        row = await db.get(DiscordConfig, user_id)
        assert row is not None
        old_generation = row.binding_generation
        row.owner_platform_user_id = "owner-1"
        row.owner_dm_chat_id = "dm-1"
        row.paired_at = paired_at
        row.pairing_code_hash = None
        row.pairing_expires_at = None
        await db.commit()

    rotated = await user_client.patch(
        "/api/channels/discord",
        json={"bot_token": "token-2"},
    )
    assert rotated.status_code == 200
    assert rotated.json()["owner"] == {"id": "owner-1", "dm_chat_id": "dm-1"}
    assert rotated.json()["pairing"] is None
    async with AsyncSession(pg_engine) as db:
        same = await db.get(DiscordConfig, user_id)
    assert same is not None
    assert same.binding_generation == old_generation
    assert same.owner_platform_user_id == "owner-1"
    assert same.paired_at == paired_at

    replaced = await user_client.patch(
        "/api/channels/discord",
        json={"bot_token": "token-new-bot"},
    )
    assert replaced.status_code == 200
    assert replaced.json()["owner"] is None
    replacement_code = replaced.json()["pairing"]["code"]
    assert replacement_code
    async with AsyncSession(pg_engine) as db:
        changed = await db.get(DiscordConfig, user_id)
    assert changed is not None
    assert changed.binding_generation != old_generation
    assert changed.owner_platform_user_id is None
    assert changed.owner_dm_chat_id is None
    assert changed.paired_at is None
    assert changed.pairing_code_hash == hashlib.sha256(replacement_code.encode()).digest()


async def test_pairing_rotation_is_atomic_and_paired_config_rejects_it(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.discord["token"] = _identity("application-1")
    runtime = _Runtime(pg_engine)
    _install(test_app, validator, runtime)
    created = await user_client.patch(
        "/api/channels/discord",
        json={"bot_token": "token"},
    )
    first_code = created.json()["pairing"]["code"]

    renewed = await user_client.post("/api/channels/discord/pairing")

    assert renewed.status_code == 200
    second_code = renewed.json()["pairing"]["code"]
    assert second_code != first_code
    user_id = await _current_user_id(pg_engine)
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        row = await db.get(DiscordConfig, user_id)
        assert row is not None
        assert row.pairing_code_hash == hashlib.sha256(second_code.encode()).digest()
        row.owner_platform_user_id = "owner-1"
        row.owner_dm_chat_id = "dm-1"
        row.paired_at = datetime.now(UTC)
        row.pairing_code_hash = None
        row.pairing_expires_at = None
        await db.commit()

    unavailable = await user_client.post("/api/channels/discord/pairing")
    assert unavailable.status_code == 409
    assert unavailable.json()["code"] == "channel_pairing_unavailable"


async def test_allow_lists_are_exact_ordered_platform_ids(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.discord["discord-token"] = _identity("application-1")
    validator.dingtalk[("client-1", "secret-1")] = _identity("client-1")
    _install(test_app, validator, _Runtime(pg_engine))

    invalid_discord = (
        [""],
        ["abc"],
        ["1" * 21],
        ["123", "123"],
        [str(index) for index in range(257)],
    )
    for allow_list in invalid_discord:
        response = await user_client.patch(
            "/api/channels/discord",
            json={"bot_token": "discord-token", "allow_list": allow_list},
        )
        assert response.status_code == 400

    invalid_dingtalk = (
        [""],
        [" leading"],
        ["trailing "],
        ["control\nchar"],
        ["x" * 257],
        ["Exact", "Exact"],
    )
    for allow_list in invalid_dingtalk:
        response = await user_client.patch(
            "/api/channels/dingtalk",
            json={
                "client_id": "client-1",
                "client_secret": "secret-1",
                "allow_list": allow_list,
            },
        )
        assert response.status_code == 400


async def test_dingtalk_partial_rotation_and_allow_list_replacement(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.dingtalk[("client-1", "secret-1")] = _identity("client-1")
    validator.dingtalk[("client-1", "secret-2")] = _identity("client-1")
    runtime = _Runtime(pg_engine)
    _install(test_app, validator, runtime)
    await user_client.patch(
        "/api/channels/dingtalk",
        json={
            "client_id": "client-1",
            "client_secret": "secret-1",
            "allow_list": ["Alice", "alice"],
        },
    )

    rotated = await user_client.patch(
        "/api/channels/dingtalk",
        json={"client_secret": "secret-2"},
    )
    replaced = await user_client.patch(
        "/api/channels/dingtalk",
        json={"allow_list": ["only-this-id"]},
    )

    assert rotated.status_code == 200
    assert replaced.status_code == 200
    assert replaced.json()["allow_list"] == ["only-this-id"]
    assert validator.calls == [
        ("dingtalk", "client-1", "secret-1"),
        ("dingtalk", "client-1", "secret-2"),
    ]


async def test_dingtalk_validator_profile_without_name_is_saved_and_returned(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"accessToken": "temporary", "expireIn": 7200},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        validator = DingTalkCredentialValidator(http_client=client)
        test_app.dependency_overrides[get_channel_credential_validator] = (
            lambda: validator
        )
        test_app.dependency_overrides[get_channel_runtime] = lambda: _Runtime(
            pg_engine
        )

        created = await user_client.patch(
            "/api/channels/dingtalk",
            json={"client_id": "client-1", "client_secret": "secret-1"},
        )

    assert created.status_code == 200
    assert created.json()["bot"] == {
        "id": "client-1",
        "name": None,
        "avatar_url": None,
    }
    user_id = await _current_user_id(pg_engine)
    async with AsyncSession(pg_engine) as db:
        row = await db.get(DingTalkConfig, user_id)
    assert row is not None
    assert row.client_id == "client-1"
    assert row.bot_user_id == "client-1"
    assert row.bot_display_name is None


async def test_partial_credential_update_revalidates_after_revision_change(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    initial_validator = _Validator()
    initial_validator.dingtalk[("client-1", "secret-1")] = _identity("client-1")
    runtime = _Runtime(pg_engine)
    _install(test_app, initial_validator, runtime)
    created = await user_client.patch(
        "/api/channels/dingtalk",
        json={"client_id": "client-1", "client_secret": "secret-1"},
    )
    assert created.status_code == 200
    user_id = await _current_user_id(pg_engine)
    validator = _ConcurrentDingTalkValidator(pg_engine, user_id)

    async with AsyncSession(pg_engine) as db:
        await channels.patch_config(
            db,
            user_id=user_id,
            channel="dingtalk",
            patch=ChannelConfigPatch(client_id="client-2"),
            validator=validator,
            runtime=runtime,
        )

    async with AsyncSession(pg_engine) as db:
        row = await db.get(DingTalkConfig, user_id)
    assert row is not None
    assert (row.client_id, row.client_secret) == ("client-2", "secret-2")
    assert validator.calls == [
        ("client-2", "secret-1"),
        ("client-2", "secret-2"),
    ]


async def test_global_identity_conflict_is_409(
    async_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.discord["token-a"] = _identity("shared-application")
    validator.discord["token-b"] = _identity("shared-application")
    _install(test_app, validator, _Runtime(pg_engine))
    await async_client.post(
        "/api/auth/register",
        json={"email": "one@test.com", "password": "testpassword", "name": "One"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "one@test.com", "password": "testpassword"},
    )
    assert (
        await async_client.patch(
            "/api/channels/discord",
            json={"bot_token": "token-a"},
        )
    ).status_code == 200
    await async_client.post("/api/auth/logout")
    await async_client.post(
        "/api/auth/register",
        json={"email": "two@test.com", "password": "testpassword", "name": "Two"},
    )
    await async_client.post(
        "/api/auth/login",
        json={"email": "two@test.com", "password": "testpassword"},
    )

    conflict = await async_client.patch(
        "/api/channels/discord",
        json={"bot_token": "token-b"},
    )

    assert conflict.status_code == 409
    assert conflict.json()["code"] == "channel_bot_already_bound"


async def test_runtime_failure_is_degraded_and_delete_still_commits(
    user_client: AsyncClient,
    test_app: FastAPI,
    pg_engine: AsyncEngine,
) -> None:
    validator = _Validator()
    validator.discord["token"] = _identity("application-1")
    runtime = _Runtime(pg_engine)
    runtime.fail_apply = True
    _install(test_app, validator, runtime)

    response = await user_client.patch(
        "/api/channels/discord",
        json={"bot_token": "token"},
    )

    assert response.status_code == 200
    assert response.json()["state"] == "degraded"
    assert response.json()["last_error"]["code"] == "channel_runtime_unavailable"
    assert "raw secret" not in response.text
    user_id = await _current_user_id(pg_engine)
    async with AsyncSession(pg_engine) as db:
        assert await db.get(DiscordConfig, user_id) is not None

    runtime.fail_remove = True
    deleted = await user_client.delete("/api/channels/discord")
    assert deleted.status_code == 204
    async with AsyncSession(pg_engine) as db:
        assert await db.get(DiscordConfig, user_id) is None
    assert runtime.remove_calls == [(user_id, "discord")]


async def test_committed_patch_finishes_runtime_apply_before_cancellation(
    pg_engine: AsyncEngine,
    user_client: AsyncClient,
) -> None:
    del user_client
    user_id = await _current_user_id(pg_engine)
    validator = _Validator()
    validator.discord["token"] = _identity("application-1")
    runtime = _BlockingRuntime(pg_engine)
    async with AsyncSession(pg_engine) as db:
        task = asyncio.create_task(
            channels.patch_config(
                db,
                user_id=user_id,
                channel="discord",
                patch=ChannelConfigPatch(bot_token="token"),
                validator=validator,
                runtime=runtime,
            )
        )
        await runtime.apply_entered.wait()
        async with AsyncSession(pg_engine) as probe:
            assert await probe.get(DiscordConfig, user_id) is not None

        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        runtime.release_apply.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert runtime.apply_completed.is_set()
    assert runtime.apply_calls == [(user_id, "discord")]


async def test_committed_delete_finishes_runtime_remove_before_cancellation(
    pg_engine: AsyncEngine,
    user_client: AsyncClient,
) -> None:
    del user_client
    user_id = await _current_user_id(pg_engine)
    validator = _Validator()
    validator.discord["token"] = _identity("application-1")
    setup_runtime = _Runtime(pg_engine)
    async with AsyncSession(pg_engine) as db:
        await channels.patch_config(
            db,
            user_id=user_id,
            channel="discord",
            patch=ChannelConfigPatch(bot_token="token"),
            validator=validator,
            runtime=setup_runtime,
        )

    runtime = _BlockingRuntime(pg_engine)
    async with AsyncSession(pg_engine) as db:
        task = asyncio.create_task(
            channels.delete_config(
                db,
                user_id=user_id,
                channel="discord",
                runtime=runtime,
            )
        )
        await runtime.remove_entered.wait()
        async with AsyncSession(pg_engine) as probe:
            assert await probe.get(DiscordConfig, user_id) is None

        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        runtime.release_remove.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert runtime.remove_completed.is_set()
    assert runtime.remove_calls == [(user_id, "discord")]


def test_runtime_error_contract_is_bounded() -> None:
    error = SanitizedChannelError(
        code="channel_error",
        message="safe",
        at=datetime.now(UTC),
    )
    snapshot = ChannelRuntimeSnapshot(state="degraded", last_error=error)
    assert snapshot.last_error == error
