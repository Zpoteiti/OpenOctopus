from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any, cast
from uuid import UUID

import pytest
from pydantic import SecretStr

from openoctopus_client.mcp.models import (
    McpServerConfig,
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
    SourceMcpCatalog,
    SourceMcpServerCatalog,
    SourceMcpTool,
    StdioMcpServerConfig,
    StreamableHttpMcpServerConfig,
)
from openoctopus_client.mcp.runtime import (
    CandidateValidation,
    McpRuntimeEvent,
    McpRuntimeState,
    McpServerRuntime,
    McpValidationFailure,
)
from openoctopus_client.mcp.supervisor import McpSupervisor
from openoctopus_client.protocol import (
    AcceptedMcpRegistration,
    ConfigValidate,
    DeviceConfig,
    McpRoute,
    ProtocolError,
    RegisterMcpAck,
    RejectedMcpRegistration,
    ToolCall,
    new_uuid7,
)
from openoctopus_client.tools import ToolOutput


def _config(name: str, *, command: str = "mcp") -> StdioMcpServerConfig:
    return StdioMcpServerConfig(
        name=name,
        transport="stdio",
        command=command,
        args=[],
        cwd=None,
        env={},
    )


def _source(name: str) -> SourceMcpServerCatalog:
    return SourceMcpServerCatalog(
        name=name,
        tools=[
            SourceMcpTool(
                raw_name="echo",
                description="Echo",
                input_schema={"type": "object", "properties": {}},
            )
        ],
    )


def _persisted(name: str, entry_id: UUID) -> PersistedMcpServerCatalog:
    return PersistedMcpServerCatalog(
        name=name,
        entries=[
            PersistedMcpCatalogEntry(
                entry_id=entry_id,
                server=name,
                surface="tool",
                raw_name="echo",
                invocation_identity="echo",
                final_name=f"mcp_{name}_echo",
                provider_description="Echo",
                input_schema={"type": "object", "properties": {}},
                enabled=True,
            )
        ],
    )


def _catalog(*servers: PersistedMcpServerCatalog) -> PersistedMcpCatalog:
    return PersistedMcpCatalog(
        version=1,
        digest="a" * 64,
        servers=list(servers),
    )


def _device_config(
    *servers: McpServerConfig,
    workspace_path: str = "/tmp/work",
) -> DeviceConfig:
    return DeviceConfig(
        workspace_path=workspace_path,
        restrict_to_workspace=True,
        ssrf_denylist=[],
        shell_timeout_max=600,
        env_allowlist=[],
        mcp_servers=list(servers),
    )


class _FakeRuntime:
    def __init__(self, config: McpServerConfig) -> None:
        self.config = config
        self.generation = new_uuid7()
        self.state = McpRuntimeState.STARTING
        self.code: str | None = "mcp_starting"
        self.permanent_failure = False
        self.source_catalog: SourceMcpServerCatalog | None = None
        self.routes: Mapping[UUID, object] = {}
        self.started = asyncio.Event()
        self.allow_start = asyncio.Event()
        self.closed = False
        self.invocations: list[tuple[UUID, Mapping[str, Any], UUID]] = []
        self.invoked = asyncio.Event()
        self.allow_invoke = asyncio.Event()
        self.allow_invoke.set()
        self.refresh_calls = 0
        self.events: asyncio.Queue[McpRuntimeEvent] = asyncio.Queue()
        self.retry_delay: float | None = None
        self.backoff_calls = 0

    async def start(self) -> SourceMcpServerCatalog:
        self.started.set()
        await self.allow_start.wait()
        self.source_catalog = _source(self.config.name)
        self.state = McpRuntimeState.AWAITING_ACK
        self.code = None
        return self.source_catalog

    def bind_persisted(self, persisted: PersistedMcpServerCatalog) -> Mapping[UUID, object]:
        assert self.source_catalog is not None
        self.routes = {entry.entry_id: object() for entry in persisted.entries}
        return self.routes

    def mark_ready(self, generation: UUID) -> None:
        assert generation == self.generation
        assert self.state is McpRuntimeState.AWAITING_ACK
        self.state = McpRuntimeState.READY

    async def invoke(
        self,
        entry_id: UUID,
        arguments: Mapping[str, Any],
        *,
        runtime_generation: UUID,
    ) -> ToolOutput:
        self.invocations.append((entry_id, arguments, runtime_generation))
        self.invoked.set()
        await self.allow_invoke.wait()
        return ToolOutput("ok")

    async def refresh(self) -> bool:
        self.refresh_calls += 1
        return True

    async def next_event(self) -> McpRuntimeEvent:
        return await self.events.get()

    def emit(self, kind: str) -> None:
        self.events.put_nowait(McpRuntimeEvent(kind=cast(Any, kind)))

    async def mark_transport_unavailable(self) -> None:
        self.state = McpRuntimeState.UNAVAILABLE
        self.code = "tool_mcp_unavailable"

    def enter_backoff(self, *, jitter: float) -> float | None:
        del jitter
        self.backoff_calls += 1
        self.state = McpRuntimeState.BACKOFF
        return self.retry_delay

    def begin_retry(self) -> None:
        raise AssertionError("retry is not expected")

    async def close(self) -> None:
        self.closed = True
        self.state = McpRuntimeState.ABSENT


class _RuntimeFactory:
    def __init__(self) -> None:
        self.created: list[_FakeRuntime] = []

    def __call__(self, config: McpServerConfig) -> McpServerRuntime:
        runtime = _FakeRuntime(config)
        self.created.append(runtime)
        return cast(McpServerRuntime, runtime)


async def _ready_registration(
    supervisor: McpSupervisor,
    runtime: _FakeRuntime,
) -> None:
    await runtime.started.wait()
    runtime.allow_start.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    registration = supervisor.next_registration()
    assert registration is not None
    snapshot = registration.servers[0]
    assert snapshot.state == "ready"
    supervisor.accept_registration(
        RegisterMcpAck(
            id=registration.id,
            config_revision=registration.config_revision,
            catalog_digest=registration.catalog_digest,
            results=[
                AcceptedMcpRegistration(
                    name=snapshot.name,
                    runtime_generation=snapshot.runtime_generation,
                    accepted=True,
                    code=None,
                )
            ],
        )
    )


def test_registration_is_single_flight_and_stale_ack_cannot_reopen_runtime() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        entry_id = new_uuid7()
        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(_config("corp")),
            catalog=_catalog(_persisted("corp", entry_id)),
        )

        starting = supervisor.next_registration()
        assert starting is not None
        assert starting.servers[0].state == "unavailable"
        assert supervisor.next_registration() is None

        runtime = factory.created[0]
        await runtime.started.wait()
        runtime.allow_start.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        supervisor.accept_registration(
            RegisterMcpAck(
                id=starting.id,
                config_revision=1,
                catalog_digest="a" * 64,
                results=[
                    RejectedMcpRegistration(
                        name="corp",
                        runtime_generation=starting.servers[0].runtime_generation,
                        accepted=False,
                        code="mcp_starting",
                    )
                ],
            )
        )
        latest = supervisor.next_registration()
        assert latest is not None
        assert latest.id != starting.id
        assert latest.servers[0].state == "ready"

        supervisor.accept_registration(
            RegisterMcpAck(
                id=latest.id,
                config_revision=1,
                catalog_digest="a" * 64,
                results=[
                    AcceptedMcpRegistration(
                        name="corp",
                        runtime_generation=runtime.generation,
                        accepted=True,
                        code=None,
                    )
                ],
            )
        )
        assert runtime.state.value == McpRuntimeState.READY.value
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_previous_revision_ack_cannot_open_current_authoritative_route() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        entry_id = new_uuid7()
        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        config = _device_config(_config("corp"))
        catalog = _catalog(_persisted("corp", entry_id))
        await supervisor.activate_authoritative(revision=1, config=config, catalog=catalog)
        runtime = factory.created[0]
        await runtime.started.wait()
        runtime.allow_start.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        revision_one = supervisor.next_registration()
        assert revision_one is not None
        assert revision_one.servers[0].state == "ready"

        await supervisor.activate_authoritative(
            revision=2,
            config=_device_config(
                _config("corp"),
                workspace_path="/tmp/revision-two",
            ),
            catalog=catalog,
        )
        supervisor.accept_registration(
            RegisterMcpAck(
                id=revision_one.id,
                config_revision=1,
                catalog_digest="a" * 64,
                results=[
                    AcceptedMcpRegistration(
                        name="corp",
                        runtime_generation=runtime.generation,
                        accepted=True,
                        code=None,
                    )
                ],
            )
        )

        assert runtime.state is McpRuntimeState.AWAITING_ACK
        revision_two = supervisor.next_registration()
        assert revision_two is not None
        assert revision_two.config_revision == 2
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_disconnect_retains_runtime_but_requires_fresh_registration() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        entry_id = new_uuid7()
        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        config = _device_config(_config("corp"))
        catalog = _catalog(_persisted("corp", entry_id))
        await supervisor.activate_authoritative(revision=1, config=config, catalog=catalog)
        runtime = factory.created[0]
        first = supervisor.next_registration()
        assert first is not None
        await runtime.started.wait()
        runtime.allow_start.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        supervisor.accept_registration(
            RegisterMcpAck(
                id=first.id,
                config_revision=1,
                catalog_digest="a" * 64,
                results=[
                    RejectedMcpRegistration(
                        name="corp",
                        runtime_generation=runtime.generation,
                        accepted=False,
                        code="mcp_starting",
                    )
                ],
            )
        )
        ready = supervisor.next_registration()
        assert ready is not None
        supervisor.accept_registration(
            RegisterMcpAck(
                id=ready.id,
                config_revision=1,
                catalog_digest="a" * 64,
                results=[
                    AcceptedMcpRegistration(
                        name="corp",
                        runtime_generation=runtime.generation,
                        accepted=True,
                        code=None,
                    )
                ],
            )
        )

        supervisor.detach_connection()
        assert not runtime.closed
        supervisor.attach_connection()
        await supervisor.activate_authoritative(revision=2, config=config, catalog=catalog)
        again = supervisor.next_registration()
        assert again is not None
        assert again.config_revision == 2
        assert again.servers[0].runtime_generation == runtime.generation
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_candidate_is_promoted_only_by_matching_update_during_lease() -> None:
    async def exercise() -> None:
        active_factory = _RuntimeFactory()
        candidate_runtime = _FakeRuntime(_config("corp", command="new"))
        candidate_runtime.source_catalog = _source("corp")
        candidate_runtime.state = McpRuntimeState.AWAITING_ACK
        candidate_runtime.allow_start.set()

        async def validator(
            configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            assert [config.name for config in configs] == ["corp"]
            return CandidateValidation(
                source_catalog=SourceMcpCatalog(version=1, servers=[_source("corp")]),
                failures=(),
                runtimes={"corp": cast(McpServerRuntime, candidate_runtime)},
            )

        supervisor = McpSupervisor(
            runtime_factory=active_factory,
            candidate_validator=cast(Callable[..., Awaitable[CandidateValidation]], validator),
            candidate_lease_seconds=60,
        )
        supervisor.attach_connection()
        validation_id = new_uuid7()
        candidate = _device_config(_config("corp", command="new"))
        validation = ConfigValidate(
            id=validation_id,
            base_config_revision=1,
            candidate_config=candidate,
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        result = await supervisor.validate(validation)
        assert result is not None and result.ok
        assert supervisor.candidate_count == 1

        await supervisor.activate_authoritative(
            revision=2,
            config=candidate,
            catalog=_catalog(_persisted("corp", new_uuid7())),
            validation_id=validation_id,
        )
        assert supervisor.candidate_count == 0
        assert active_factory.created == []
        assert not candidate_runtime.closed
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_cancelled_validation_closes_candidate_and_creates_tombstone() -> None:
    async def exercise() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        candidate_runtime = _FakeRuntime(_config("corp"))

        async def validator(
            _configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            started.set()
            await release.wait()
            return CandidateValidation(
                source_catalog=SourceMcpCatalog(version=1, servers=[_source("corp")]),
                failures=(),
                runtimes={"corp": cast(McpServerRuntime, candidate_runtime)},
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(Callable[..., Awaitable[CandidateValidation]], validator)
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        task = supervisor.begin_validation(frame)
        await started.wait()
        await supervisor.cancel_validation(frame.id)
        assert await task is None
        assert supervisor.is_validation_tombstone(frame.id)
        await supervisor.cancel_validation(frame.id)
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_cancel_after_completed_validation_failure_is_recently_idempotent() -> None:
    async def exercise() -> None:
        async def validator(
            configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            return CandidateValidation(
                source_catalog=None,
                failures=(
                    McpValidationFailure(
                        server=configs[0].name,
                        stage="candidate",
                        code="config_validation_failed",
                        message="expected validation failure",
                    ),
                ),
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            )
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )

        result = await supervisor.validate(frame)
        assert result is not None and not result.ok
        await supervisor.cancel_validation(frame.id)
        await supervisor.cancel_validation(frame.id)
        with pytest.raises(ProtocolError, match="Unknown MCP validation"):
            await supervisor.cancel_validation(new_uuid7())
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_candidate_cancel_publishes_tombstone_before_cleanup_await() -> None:
    async def exercise() -> None:
        class BlockingRuntime(_FakeRuntime):
            def __init__(self, config: McpServerConfig) -> None:
                super().__init__(config)
                self.close_started = asyncio.Event()
                self.allow_close = asyncio.Event()

            async def close(self) -> None:
                self.close_started.set()
                await self.allow_close.wait()
                await super().close()

        candidate_runtime = BlockingRuntime(_config("corp"))

        async def validator(
            _configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            return CandidateValidation(
                source_catalog=SourceMcpCatalog(version=1, servers=[_source("corp")]),
                failures=(),
                runtimes={"corp": cast(McpServerRuntime, candidate_runtime)},
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            )
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        result = await supervisor.validate(frame)
        assert result is not None and result.ok

        cancel_task = asyncio.create_task(supervisor.cancel_validation(frame.id))
        await candidate_runtime.close_started.wait()
        assert supervisor.is_validation_tombstone(frame.id)
        await asyncio.wait_for(supervisor.cancel_validation(frame.id), timeout=0.1)

        candidate_runtime.allow_close.set()
        await cancel_task
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_active_validation_cancel_releases_slot_before_cleanup_finishes() -> None:
    async def exercise() -> None:
        first_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        allow_cleanup = asyncio.Event()
        first_runtime = _FakeRuntime(_config("corp"))
        calls = 0

        async def validator(
            _configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    cleanup_started.set()
                    await allow_cleanup.wait()
                return CandidateValidation(
                    source_catalog=SourceMcpCatalog(
                        version=1,
                        servers=[_source("corp")],
                    ),
                    failures=(),
                    runtimes={"corp": cast(McpServerRuntime, first_runtime)},
                )
            return CandidateValidation(
                source_catalog=None,
                failures=(
                    McpValidationFailure(
                        server="corp",
                        stage="candidate",
                        code="config_validation_failed",
                        message="expected retry failure",
                    ),
                ),
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            )
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        first = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        first_task = supervisor.begin_validation(first)
        await first_started.wait()
        cancel_task = asyncio.create_task(supervisor.cancel_validation(first.id))
        await cleanup_started.wait()

        assert supervisor.is_validation_tombstone(first.id)
        second = await supervisor.validate(first.model_copy(update={"id": new_uuid7()}))
        assert second is not None and not second.ok
        assert second.failures[0].code == "config_validation_failed"

        allow_cleanup.set()
        await cancel_task
        assert await first_task is None
        assert first_runtime.closed
        assert supervisor.candidate_count == 0
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_candidate_expiry_tombstone_overflow_retires_ws_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        monkeypatch.setattr(
            "openoctopus_client.mcp.supervisor._VALIDATION_TOMBSTONE_MAX",
            1,
        )

        async def validator(
            configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            runtime = _FakeRuntime(configs[0])
            return CandidateValidation(
                source_catalog=SourceMcpCatalog(
                    version=1,
                    servers=[_source(configs[0].name)],
                ),
                failures=(),
                runtimes={configs[0].name: cast(McpServerRuntime, runtime)},
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            ),
            candidate_lease_seconds=0.01,
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        registration = supervisor.next_registration()
        assert registration is not None
        supervisor.accept_registration(
            RegisterMcpAck(
                id=registration.id,
                config_revision=1,
                catalog_digest="a" * 64,
                results=[],
            )
        )

        for _index in range(2):
            frame = ConfigValidate(
                id=new_uuid7(),
                base_config_revision=1,
                candidate_config=_device_config(_config("corp")),
                validate_servers=["corp"],
                deadline_ms=300000,
            )
            result = await supervisor.validate(frame)
            assert result is not None and result.ok
            for _attempt in range(20):
                if supervisor.candidate_count == 0:
                    break
                await asyncio.sleep(0.005)
            assert supervisor.candidate_count == 0

        await asyncio.wait_for(supervisor.registration_changed.wait(), timeout=1)
        with pytest.raises(ProtocolError, match="tombstone capacity"):
            supervisor.next_registration()
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_disconnect_discards_generation_scoped_candidate() -> None:
    async def exercise() -> None:
        candidate_runtime = _FakeRuntime(_config("corp"))
        candidate_runtime.source_catalog = _source("corp")
        candidate_runtime.state = McpRuntimeState.AWAITING_ACK

        async def validator(
            _configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            return CandidateValidation(
                source_catalog=SourceMcpCatalog(version=1, servers=[_source("corp")]),
                failures=(),
                runtimes={"corp": cast(McpServerRuntime, candidate_runtime)},
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(Callable[..., Awaitable[CandidateValidation]], validator)
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        result = await supervisor.validate(frame)
        assert result is not None and result.ok

        supervisor.detach_connection()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert supervisor.candidate_count == 0
        assert candidate_runtime.closed
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_reconnect_can_validate_while_old_cancel_cleanup_finishes() -> None:
    async def exercise() -> None:
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        first_runtime = _FakeRuntime(_config("corp"))
        calls = 0

        async def validator(
            _configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    await release_first.wait()
                return CandidateValidation(
                    source_catalog=SourceMcpCatalog(
                        version=1,
                        servers=[_source("corp")],
                    ),
                    failures=(),
                    runtimes={"corp": cast(McpServerRuntime, first_runtime)},
                )
            return CandidateValidation(
                source_catalog=None,
                failures=(
                    McpValidationFailure(
                        server="corp",
                        stage="candidate",
                        code="config_validation_failed",
                        message="expected retry failure",
                    ),
                ),
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            )
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        first_frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        first_task = supervisor.begin_validation(first_frame)
        await first_started.wait()

        supervisor.detach_connection()
        supervisor.attach_connection()
        second = await supervisor.validate(
            first_frame.model_copy(update={"id": new_uuid7()})
        )
        assert second is not None and not second.ok
        assert second.failures[0].code == "config_validation_failed"

        release_first.set()
        assert await first_task is None
        assert first_runtime.closed
        assert supervisor.candidate_count == 0
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_expiry_cleanup_cannot_publish_tombstone_into_reconnected_generation() -> None:
    async def exercise() -> None:
        class BlockingRuntime(_FakeRuntime):
            def __init__(self, config: McpServerConfig) -> None:
                super().__init__(config)
                self.close_started = asyncio.Event()
                self.allow_close = asyncio.Event()

            async def close(self) -> None:
                self.close_started.set()
                await self.allow_close.wait()
                await super().close()

        candidate_runtime = BlockingRuntime(_config("corp"))

        async def validator(
            _configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            return CandidateValidation(
                source_catalog=SourceMcpCatalog(version=1, servers=[_source("corp")]),
                failures=(),
                runtimes={"corp": cast(McpServerRuntime, candidate_runtime)},
            )

        supervisor = McpSupervisor(
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            ),
            candidate_lease_seconds=0.01,
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        result = await supervisor.validate(frame)
        assert result is not None and result.ok
        await asyncio.wait_for(candidate_runtime.close_started.wait(), timeout=1)

        supervisor.detach_connection()
        supervisor.attach_connection()
        candidate_runtime.allow_close.set()
        for _attempt in range(20):
            if candidate_runtime.closed:
                break
            await asyncio.sleep(0)

        assert candidate_runtime.closed
        for _attempt in range(20):
            await asyncio.sleep(0)
        assert not supervisor.is_validation_tombstone(frame.id)
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_dispatch_requires_exact_accepted_route_identity() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        entry_id = new_uuid7()
        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(_config("corp")),
            catalog=_catalog(_persisted("corp", entry_id)),
        )
        runtime = factory.created[0]
        await _ready_registration(supervisor, runtime)

        call = ToolCall(
            id=new_uuid7(),
            name="mcp_corp_echo",
            args={"value": "hello"},
            max_result_bytes=4096,
            mcp_route=McpRoute(
                entry_id=entry_id,
                config_revision=1,
                catalog_digest="a" * 64,
                runtime_generation=runtime.generation,
            ),
        )
        assert (await supervisor.invoke(call)).content == "ok"

        stale = call.model_copy(
            update={
                "mcp_route": call.mcp_route.model_copy(update={"config_revision": 2})
                if call.mcp_route is not None
                else None
            }
        )
        rejected = await supervisor.invoke(stale)
        assert rejected.is_error
        assert rejected.code == "tool_mcp_unavailable"
        assert len(runtime.invocations) == 1
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_insecure_server_origin_rejects_secret_activation_and_validation() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        supervisor = McpSupervisor(
            runtime_factory=factory,
            secret_transport_safe=False,
        )
        supervisor.attach_connection()
        secret = StdioMcpServerConfig(
            name="corp",
            transport="stdio",
            command="mcp",
            args=[],
            cwd=None,
            env={"API_KEY": SecretStr("secret")},
        )
        try:
            await supervisor.activate_authoritative(
                revision=1,
                config=_device_config(secret),
                catalog=_catalog(_persisted("corp", new_uuid7())),
            )
        except ValueError as exc:
            assert "HTTPS" in str(exc)
        else:
            raise AssertionError("secret activation over ws must fail closed")
        assert factory.created == []

        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(secret),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        result = await supervisor.validate(frame)
        assert result is not None and not result.ok
        assert result.failures[0].code == "mcp_secret_transport_insecure"

        public = _config("public")
        full_candidate = frame.model_copy(
            update={
                "id": new_uuid7(),
                "candidate_config": _device_config(secret, public),
                "validate_servers": ["public"],
            }
        )
        result = await supervisor.validate(full_candidate)
        assert result is not None and not result.ok
        assert [failure.name for failure in result.failures] == ["public"]
        assert result.failures[0].code == "mcp_secret_transport_insecure"
        assert factory.created == []
        await supervisor.shutdown()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "server",
    [
        StdioMcpServerConfig(
            name="corp",
            transport="stdio",
            command="mcp",
            args=[],
            cwd=None,
            env={"OPTIONAL_TOKEN": SecretStr("")},
        ),
        StreamableHttpMcpServerConfig(
            name="corp",
            transport="streamable_http",
            url="https://mcp.invalid/mcp",
            headers={"x-optional-token": SecretStr("")},
        ),
    ],
    ids=["empty-env", "empty-header"],
)
def test_insecure_server_origin_allows_empty_secret_values(
    server: McpServerConfig,
) -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        supervisor = McpSupervisor(
            runtime_factory=factory,
            secret_transport_safe=False,
        )
        supervisor.attach_connection()

        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(server),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )

        assert len(factory.created) == 1
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_insecure_server_origin_rejects_nonempty_remote_header() -> None:
    async def exercise() -> None:
        supervisor = McpSupervisor(secret_transport_safe=False)
        supervisor.attach_connection()
        server = StreamableHttpMcpServerConfig(
            name="corp",
            transport="streamable_http",
            url="https://mcp.invalid/mcp",
            headers={"authorization": SecretStr("secret")},
        )

        with pytest.raises(ProtocolError, match="HTTPS"):
            await supervisor.activate_authoritative(
                revision=1,
                config=_device_config(server),
                catalog=_catalog(_persisted("corp", new_uuid7())),
            )
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_repeated_replacement_closes_every_draining_generation() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        validation_calls = 0

        async def validator(
            configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            nonlocal validation_calls
            validation_calls += 1
            return CandidateValidation(
                source_catalog=None,
                failures=(
                    McpValidationFailure(
                        server=configs[0].name,
                        stage="candidate",
                        code="config_validation_failed",
                        message="expected test failure",
                    ),
                ),
            )

        entry_id = new_uuid7()
        catalog = _catalog(_persisted("corp", entry_id))
        supervisor = McpSupervisor(
            runtime_factory=factory,
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            ),
            drain_seconds=60,
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(_config("corp", command="one")),
            catalog=catalog,
        )
        first = factory.created[0]
        await _ready_registration(supervisor, first)
        first.allow_invoke.clear()
        call = ToolCall(
            id=new_uuid7(),
            name="mcp_corp_echo",
            args={},
            max_result_bytes=4096,
            mcp_route=McpRoute(
                entry_id=entry_id,
                config_revision=1,
                catalog_digest="a" * 64,
                runtime_generation=first.generation,
            ),
        )
        invocation = asyncio.create_task(supervisor.invoke(call))
        await first.invoked.wait()

        await supervisor.activate_authoritative(
            revision=2,
            config=_device_config(_config("corp", command="two")),
            catalog=catalog,
        )
        second = factory.created[1]
        conflict = await supervisor.validate(
            ConfigValidate(
                id=new_uuid7(),
                base_config_revision=2,
                candidate_config=_device_config(_config("corp", command="three")),
                validate_servers=["corp"],
                deadline_ms=300000,
            )
        )
        assert conflict is not None and not conflict.ok
        assert conflict.failures[0].code == "device_config_conflict"
        assert supervisor.draining_count == 1
        assert validation_calls == 0
        await supervisor.activate_authoritative(
            revision=3,
            config=_device_config(
                _config("corp", command="two"),
                workspace_path="/tmp/renamed-workspace",
            ),
            catalog=catalog,
        )
        await asyncio.sleep(0)
        assert not first.closed
        assert supervisor.draining_count == 1
        await supervisor.activate_authoritative(
            revision=4,
            config=_device_config(_config("corp", command="three")),
            catalog=catalog,
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert first.closed
        assert second.closed
        assert supervisor.draining_count <= 1
        first.allow_invoke.set()
        await invocation
        for _attempt in range(10):
            if supervisor.draining_count == 0:
                break
            await asyncio.sleep(0)
        retry = await supervisor.validate(
            ConfigValidate(
                id=new_uuid7(),
                base_config_revision=4,
                candidate_config=_device_config(_config("corp", command="four")),
                validate_servers=["corp"],
                deadline_ms=300000,
            )
        )
        assert retry is not None and not retry.ok
        assert retry.failures[0].code == "config_validation_failed"
        assert validation_calls == 1
        await supervisor.shutdown()
        assert all(runtime.closed for runtime in factory.created)

    asyncio.run(exercise())


def test_authoritative_startup_never_exceeds_four_runtimes() -> None:
    async def exercise() -> None:
        release = asyncio.Event()
        current = 0
        peak = 0
        started = 0

        class ConcurrentRuntime(_FakeRuntime):
            async def start(self) -> SourceMcpServerCatalog:
                nonlocal current, peak, started
                current += 1
                started += 1
                peak = max(peak, current)
                self.started.set()
                try:
                    await release.wait()
                finally:
                    current -= 1
                self.source_catalog = _source(self.config.name)
                self.state = McpRuntimeState.AWAITING_ACK
                self.code = None
                return self.source_catalog

        runtimes: list[ConcurrentRuntime] = []

        def factory(config: McpServerConfig) -> McpServerRuntime:
            runtime = ConcurrentRuntime(config)
            runtimes.append(runtime)
            return cast(McpServerRuntime, runtime)

        configs = tuple(_config(f"server_{index}") for index in range(6))
        catalog = _catalog(
            *(
                _persisted(config.name, new_uuid7())
                for config in configs
            )
        )
        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(*configs),
            catalog=catalog,
        )
        for _attempt in range(50):
            if started >= 4:
                break
            await asyncio.sleep(0)

        assert started == 4
        assert peak == 4
        registration = supervisor.next_registration()
        assert registration is not None
        assert len(registration.servers) == 6
        assert {snapshot.code for snapshot in registration.servers} == {"mcp_starting"}

        release.set()
        for _attempt in range(50):
            if started == 6 and current == 0:
                break
            await asyncio.sleep(0)
        assert started == 6
        assert peak == 4
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_active_cleanup_blocker_is_retained_and_retried_at_shutdown() -> None:
    async def exercise() -> None:
        class CleanupBlockedRuntime(_FakeRuntime):
            def __init__(self, config: McpServerConfig) -> None:
                super().__init__(config)
                self.close_calls = 0

            async def close(self) -> None:
                self.close_calls += 1
                if self.close_calls == 1:
                    self.state = McpRuntimeState.CLEANUP_BLOCKED
                    self.code = "mcp_cleanup_incomplete"
                    return
                await super().close()

        created: list[_FakeRuntime] = []
        blocked = CleanupBlockedRuntime(_config("corp"))

        def factory(config: McpServerConfig) -> McpServerRuntime:
            runtime: _FakeRuntime
            if not created:
                runtime = blocked
            else:
                runtime = _FakeRuntime(config)
            created.append(runtime)
            return cast(McpServerRuntime, runtime)

        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(_config("corp")),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )
        await blocked.started.wait()
        blocked.allow_start.set()
        await asyncio.sleep(0)
        await supervisor.activate_authoritative(
            revision=2,
            config=_device_config(),
            catalog=_catalog(),
        )
        for _attempt in range(20):
            if blocked.close_calls == 1:
                break
            await asyncio.sleep(0)
        assert blocked.state.value == "cleanup_blocked"

        await supervisor.activate_authoritative(
            revision=3,
            config=_device_config(_config("corp")),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )
        await asyncio.sleep(0)
        assert created == [blocked]
        snapshot = supervisor.next_registration()
        assert snapshot is not None
        assert snapshot.servers[0].code == "mcp_cleanup_incomplete"

        await supervisor.shutdown()
        assert blocked.close_calls == 2
        assert blocked.state is McpRuntimeState.ABSENT

    asyncio.run(exercise())


def test_cleanup_blocker_allows_same_name_with_different_sink() -> None:
    async def exercise() -> None:
        class CleanupBlockedRuntime(_FakeRuntime):
            def __init__(self, config: McpServerConfig) -> None:
                super().__init__(config)
                self.close_calls = 0

            async def close(self) -> None:
                self.close_calls += 1
                if self.close_calls == 1:
                    self.state = McpRuntimeState.CLEANUP_BLOCKED
                    self.code = "mcp_cleanup_incomplete"
                    return
                await super().close()

        blocked = CleanupBlockedRuntime(_config("corp", command="old"))
        created: list[_FakeRuntime] = []

        def factory(config: McpServerConfig) -> McpServerRuntime:
            runtime: _FakeRuntime = blocked if not created else _FakeRuntime(config)
            created.append(runtime)
            return cast(McpServerRuntime, runtime)

        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(_config("corp", command="old")),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )
        await blocked.started.wait()
        blocked.allow_start.set()
        await asyncio.sleep(0)
        await supervisor.activate_authoritative(
            revision=2,
            config=_device_config(),
            catalog=_catalog(),
        )
        for _attempt in range(20):
            if blocked.close_calls == 1:
                break
            await asyncio.sleep(0)

        await supervisor.activate_authoritative(
            revision=3,
            config=_device_config(_config("corp", command="replacement")),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )
        replacement = created[1]
        await replacement.started.wait()
        assert isinstance(replacement.config, StdioMcpServerConfig)
        assert replacement.config.command == "replacement"

        await supervisor.shutdown()
        assert blocked.close_calls == 2
        assert replacement.closed

    asyncio.run(exercise())


def test_failed_candidate_cleanup_blocker_prevents_same_alias_spawn() -> None:
    async def exercise() -> None:
        class CleanupBlockedRuntime(_FakeRuntime):
            def __init__(self, config: McpServerConfig) -> None:
                super().__init__(config)
                self.close_calls = 1
                self.state = McpRuntimeState.CLEANUP_BLOCKED
                self.code = "mcp_cleanup_incomplete"

            async def close(self) -> None:
                self.close_calls += 1
                await super().close()

        blocked = CleanupBlockedRuntime(_config("corp"))

        async def validator(
            _configs: Sequence[McpServerConfig], **_kwargs: object
        ) -> CandidateValidation:
            return CandidateValidation(
                source_catalog=None,
                failures=(
                    McpValidationFailure(
                        server="corp",
                        stage="cleanup",
                        code="mcp_cleanup_incomplete",
                        message="candidate cleanup remained incomplete",
                    ),
                ),
                runtimes={"corp": cast(McpServerRuntime, blocked)},
            )

        def unexpected_factory(_config: McpServerConfig) -> McpServerRuntime:
            raise AssertionError("cleanup-blocked alias must not spawn again")

        supervisor = McpSupervisor(
            runtime_factory=unexpected_factory,
            candidate_validator=cast(
                Callable[..., Awaitable[CandidateValidation]], validator
            ),
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(),
            catalog=_catalog(),
        )
        frame = ConfigValidate(
            id=new_uuid7(),
            base_config_revision=1,
            candidate_config=_device_config(_config("corp")),
            validate_servers=["corp"],
            deadline_ms=300000,
        )
        result = await supervisor.validate(frame)
        assert result is not None and not result.ok

        await supervisor.activate_authoritative(
            revision=2,
            config=_device_config(_config("corp")),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )
        snapshot = supervisor.next_registration()
        assert snapshot is not None
        assert snapshot.servers[0].code == "mcp_cleanup_incomplete"
        await supervisor.shutdown()
        assert blocked.close_calls == 2

    asyncio.run(exercise())


def test_list_changed_is_coalesced_and_requires_fresh_ack() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        supervisor = McpSupervisor(
            runtime_factory=factory,
            list_changed_debounce_seconds=0,
        )
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(_config("corp")),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )
        runtime = factory.created[0]
        await _ready_registration(supervisor, runtime)

        runtime.emit("tools_changed")
        runtime.emit("resources_changed")
        runtime.emit("prompts_changed")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert runtime.refresh_calls == 1
        assert runtime.state is McpRuntimeState.AWAITING_ACK
        registration = supervisor.next_registration()
        assert registration is not None
        supervisor.accept_registration(
            RegisterMcpAck(
                id=registration.id,
                config_revision=1,
                catalog_digest="a" * 64,
                results=[
                    AcceptedMcpRegistration(
                        name="corp",
                        runtime_generation=runtime.generation,
                        accepted=True,
                        code=None,
                    )
                ],
            )
        )
        assert runtime.state.value == McpRuntimeState.READY.value
        await supervisor.shutdown()

    asyncio.run(exercise())


def test_idle_transport_failure_enters_one_background_backoff() -> None:
    async def exercise() -> None:
        factory = _RuntimeFactory()
        supervisor = McpSupervisor(runtime_factory=factory)
        supervisor.attach_connection()
        await supervisor.activate_authoritative(
            revision=1,
            config=_device_config(_config("corp")),
            catalog=_catalog(_persisted("corp", new_uuid7())),
        )
        runtime = factory.created[0]
        await _ready_registration(supervisor, runtime)
        runtime.retry_delay = 60

        runtime.emit("transport_failed")
        runtime.emit("transport_failed")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert runtime.state is McpRuntimeState.BACKOFF
        assert runtime.backoff_calls == 1
        registration = supervisor.next_registration()
        assert registration is not None
        assert registration.servers[0].state == "unavailable"
        assert registration.servers[0].code == "tool_mcp_unavailable"
        await supervisor.shutdown()

    asyncio.run(exercise())
