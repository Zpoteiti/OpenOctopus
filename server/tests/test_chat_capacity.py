import asyncio
import json
from collections import defaultdict
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.admission import (
    AdmissionTimeoutError,
    KeyedAdmission,
    KeyedDirectionalAdmission,
)
from openctopus_server.chat.runner import ChatRuntime
from openctopus_server.db.models import Message, PendingMessage, SystemConfig, TurnRun
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError
from openctopus_server.provider.anthropic import (
    DeltaCallback,
    ProviderResult,
    provider_fingerprint,
)
from openctopus_server.provider.config import ProviderConfig
from openctopus_server.provider.limiter import ProviderLimiter
from openctopus_server.provider.wire_types import Effort
from openctopus_server.tools.registry import ToolRegistry
from openctopus_server.workspace.file_content import DocumentParser

_SESSION_COUNT = 500


class CapacityProvider:
    def __init__(self, expected_calls: int) -> None:
        self.expected_calls = expected_calls
        self.started = 0
        self.active = 0
        self.max_active = 0
        self.started_users: set[str] = set()
        self.all_started = asyncio.Event()
        self.release = asyncio.Event()

    async def stream_turn(
        self,
        *,
        config: ProviderConfig,
        system: str,
        messages: list[dict[str, Any]],
        effort: Effort | None,
        limiter: ProviderLimiter,
        on_delta: DeltaCallback,
        tools: list[dict[str, Any]] | None = None,
    ) -> ProviderResult:
        del system, effort, tools
        prompt = str(messages[-1]["content"][-1]["text"])
        reply = prompt.replace("session-", "reply-", 1)
        await limiter.configure(config.max_concurrent_requests)
        async with limiter.slot():
            self.started_users.add("-".join(prompt.split("-", 2)[:2]))
            self.started += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == self.expected_calls:
                self.all_started.set()
            try:
                await self.release.wait()
                await on_delta("text", reply)
                return ProviderResult(
                    content=[{"type": "text", "text": reply}],
                    fingerprint=provider_fingerprint(config),
                )
            finally:
                self.active -= 1

    async def close(self) -> None:
        return None


async def _configure_provider(pg_engine) -> None:
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        db.add_all(
            [
                SystemConfig(key="llm_endpoint", value="http://capacity.test"),
                SystemConfig(key="llm_api_key", value="fake-key"),
                SystemConfig(key="llm_model", value="fake-model"),
                SystemConfig(key="llm_max_concurrent_requests", value=2),
            ]
        )
        await db.commit()


async def _wait_for_no_runtime_states(runtime: ChatRuntime) -> None:
    for _ in range(200):
        async with runtime._states_lock:
            if not runtime._states:
                return
        await asyncio.sleep(0.01)
    raise AssertionError("completed session states were not evicted")


async def _wait_for_runtime_state_count(runtime: ChatRuntime, expected: int) -> None:
    for _ in range(6_000):
        async with runtime._states_lock:
            if len(runtime._states) == expected:
                return
        await asyncio.sleep(0.01)
    raise AssertionError(f"runtime did not retain {expected} concurrent session states")


async def _hold_transfer(
    admission: KeyedDirectionalAdmission,
    user_id: UUID,
    direction: str,
    entered: asyncio.Event,
    release: asyncio.Event,
) -> None:
    async with admission.slot(user_id, direction):
        entered.set()
        await release.wait()


async def _expect_transfer_timeout(
    admission: KeyedDirectionalAdmission,
    user_id: UUID,
) -> str:
    try:
        async with admission.slot(user_id, "upload"):
            raise AssertionError("saturated transfer admission unexpectedly entered")
    except AdmissionTimeoutError:
        return "workspace_transfer_busy"


async def _hold_conversion(
    parser: DocumentParser,
    user_id: UUID,
    entered: asyncio.Event,
    release: asyncio.Event,
) -> None:
    async with parser.admit(user_id):
        entered.set()
        await release.wait()


async def _expect_conversion_timeout(parser: DocumentParser, user_id: UUID) -> ErrorCode:
    try:
        async with parser.admit(user_id):
            raise AssertionError("saturated conversion admission unexpectedly entered")
    except ToolError as exc:
        return exc.code


async def test_500_concurrent_sessions_complete_without_cross_talk(
    async_client,
    test_app,
    pg_engine,
) -> None:
    users: list[tuple[str, str, UUID]] = []
    for label in ("user-a", "user-b"):
        response = await async_client.post(
            "/api/auth/register",
            json={
                "email": f"{label}@test.com",
                "password": "testpassword",
                "name": label,
            },
        )
        assert response.status_code == 201
        users.append(
            (
                label,
                str(response.json()["jwt"]),
                UUID(response.json()["user"]["id"]),
            )
        )
    async_client.cookies.clear()

    release_features = asyncio.Event()
    transfer_admission = KeyedDirectionalAdmission(
        direction_limits={"upload": 2, "download": 1},
        per_key_limit=1,
        timeout_seconds=0.1,
    )
    transfer_holders: list[asyncio.Task[None]] = []
    first_upload_entered = asyncio.Event()
    transfer_holders.append(
        asyncio.create_task(
            _hold_transfer(
                transfer_admission,
                users[0][2],
                "upload",
                first_upload_entered,
                release_features,
            )
        )
    )
    await first_upload_entered.wait()
    same_user_transfer_waiters = [
        asyncio.create_task(_expect_transfer_timeout(transfer_admission, users[0][2]))
        for _ in range(20)
    ]
    second_upload_entered = asyncio.Event()
    transfer_holders.append(
        asyncio.create_task(
            _hold_transfer(
                transfer_admission,
                users[1][2],
                "upload",
                second_upload_entered,
                release_features,
            )
        )
    )
    await asyncio.wait_for(second_upload_entered.wait(), timeout=0.5)
    independent_download_user = uuid5(NAMESPACE_URL, "capacity-download-user")
    download_entered = asyncio.Event()
    transfer_holders.append(
        asyncio.create_task(
            _hold_transfer(
                transfer_admission,
                independent_download_user,
                "download",
                download_entered,
                release_features,
            )
        )
    )
    await asyncio.wait_for(download_entered.wait(), timeout=0.5)
    assert (
        await _expect_transfer_timeout(
            transfer_admission,
            uuid5(NAMESPACE_URL, "capacity-blocked-upload-user"),
        )
        == "workspace_transfer_busy"
    )
    assert await asyncio.gather(*same_user_transfer_waiters) == ["workspace_transfer_busy"] * len(
        same_user_transfer_waiters
    )

    conversion_admission = KeyedAdmission(
        global_limit=2,
        per_key_limit=1,
        timeout_seconds=0.1,
    )
    parser = DocumentParser(
        admission=conversion_admission,
        memory_mb=1024,
        timeout_seconds=20,
    )
    conversion_holders: list[asyncio.Task[None]] = []
    first_conversion_entered = asyncio.Event()
    conversion_holders.append(
        asyncio.create_task(
            _hold_conversion(
                parser,
                users[0][2],
                first_conversion_entered,
                release_features,
            )
        )
    )
    await first_conversion_entered.wait()
    same_user_conversion_waiters = [
        asyncio.create_task(_expect_conversion_timeout(parser, users[0][2])) for _ in range(20)
    ]
    second_conversion_entered = asyncio.Event()
    conversion_holders.append(
        asyncio.create_task(
            _hold_conversion(
                parser,
                users[1][2],
                second_conversion_entered,
                release_features,
            )
        )
    )
    await asyncio.wait_for(second_conversion_entered.wait(), timeout=0.5)
    assert (
        await _expect_conversion_timeout(
            parser,
            uuid5(NAMESPACE_URL, "capacity-blocked-conversion-user"),
        )
        is ErrorCode.TOOL_CONTENT_CONVERSION_BUSY
    )
    assert await asyncio.gather(*same_user_conversion_waiters) == [
        ErrorCode.TOOL_CONTENT_CONVERSION_BUSY
    ] * len(same_user_conversion_waiters)
    assert transfer_admission.entry_count == 3
    assert conversion_admission.entry_count == 2

    await _configure_provider(pg_engine)
    provider = CapacityProvider(2)
    context_admission = KeyedAdmission(
        global_limit=2,
        per_key_limit=1,
        timeout_seconds=60,
    )
    runtime = ChatRuntime(
        pg_engine,
        provider_factory=lambda config: provider,
        tool_registry=ToolRegistry(()),
        context_admission=context_admission,
    )
    test_app.state.chat_runtime = runtime
    session_ids = [
        uuid5(NAMESPACE_URL, f"openoctopus-capacity-{index}") for index in range(_SESSION_COUNT)
    ]
    request_users = [users[index % len(users)] for index in range(_SESSION_COUNT)]
    requests = [
        asyncio.create_task(
            async_client.post(
                f"/api/sessions/{session_id}/messages",
                json={
                    "content": [
                        {
                            "type": "text",
                            "text": f"{request_users[index][0]}-session-{index}",
                        }
                    ],
                    "attachments": [],
                },
                headers={"Authorization": f"Bearer {request_users[index][1]}"},
            )
        )
        for index, session_id in enumerate(session_ids)
    ]

    try:
        await asyncio.wait_for(provider.all_started.wait(), timeout=60)
        assert provider.started_users == {"user-a", "user-b"}
        await _wait_for_runtime_state_count(runtime, _SESSION_COUNT)
        async with runtime._states_lock:
            assert len(runtime._states) == _SESSION_COUNT
    except BaseException:
        provider.release.set()
        release_features.set()
        await asyncio.gather(*requests, return_exceptions=True)
        await asyncio.gather(*transfer_holders, *conversion_holders, return_exceptions=True)
        await runtime.close()
        raise

    provider.release.set()
    release_features.set()
    await asyncio.gather(*transfer_holders, *conversion_holders)
    responses = await asyncio.wait_for(asyncio.gather(*requests), timeout=60)
    for index, response in enumerate(responses):
        assert response.status_code == 200
        events = [json.loads(line) for line in response.text.splitlines()]
        deltas = [event for event in events if event["type"] == "token_delta"]
        assert [event["text"] for event in deltas] == [f"{request_users[index][0]}-reply-{index}"]

    await _wait_for_no_runtime_states(runtime)
    assert provider.started == _SESSION_COUNT
    assert provider.max_active == 2
    assert context_admission.entry_count == 0
    assert transfer_admission.entry_count == 0
    assert conversion_admission.entry_count == 0
    async with AsyncSession(pg_engine, expire_on_commit=False) as db:
        messages = list(
            (
                await db.execute(select(Message.session_id, Message.message_kind, Message.content))
            ).all()
        )
        pending_ids = list((await db.execute(select(PendingMessage.id))).scalars().all())
        run_statuses = list((await db.execute(select(TurnRun.status))).scalars().all())

    by_session: dict[UUID, list[tuple[str, list[dict[str, Any]]]]] = defaultdict(list)
    for session_id, message_kind, content in messages:
        by_session[session_id].append((message_kind, content))
    assert len(by_session) == _SESSION_COUNT
    for index, session_id in enumerate(session_ids):
        session_messages = dict(by_session[session_id])
        assert set(session_messages) == {"human", "assistant"}
        assert session_messages["human"][-1] == {
            "type": "text",
            "text": f"{request_users[index][0]}-session-{index}",
        }
        assert session_messages["assistant"] == [
            {"type": "text", "text": f"{request_users[index][0]}-reply-{index}"}
        ]
    assert pending_ids == []
    assert len(run_statuses) == _SESSION_COUNT
    assert set(run_statuses) == {"completed"}
    await runtime.close()
