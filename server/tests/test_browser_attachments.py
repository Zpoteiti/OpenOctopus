import base64
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.chat import attachments as attachment_module
from openctopus_server.chat.attachments import (
    build_device_attachment_targets,
    expand_server_workspace_attachments,
    fence_owner_device_targets,
)
from openctopus_server.chat.device_snapshot import OwnerDeviceSnapshot
from openctopus_server.chat.runner import _build_owner_tool_state
from openctopus_server.devices.mcp_catalog import with_catalog_digest
from openctopus_server.devices.mcp_models import (
    PersistedMcpCatalog,
    PersistedMcpCatalogEntry,
    PersistedMcpServerCatalog,
)
from openctopus_server.devices.protocol import new_uuid7
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError, WorkspaceError
from openctopus_server.tools.registry import build_py3_registry
from openctopus_server.workspace.service import WorkspaceService
from openctopus_server.workspace.storage import StoredObject


@dataclass(frozen=True, slots=True)
class _Attachment:
    openoctopus_device: str
    path: str


@dataclass(slots=True)
class _StoredAttachments:
    attachment_refs: list[dict[str, object]]


def test_device_attachment_targets_fence_a_same_name_replacement() -> None:
    attached_id = uuid4()
    replacement_id = uuid4()
    rows = [
        _StoredAttachments(
            attachment_refs=[
                {
                    "openoctopus_device": "laptop-cn",
                    "device_id": str(attached_id),
                    "path": "documents/report.pdf",
                },
                {"openoctopus_device": "server", "path": "shared/report.pdf"},
            ]
        )
    ]

    attachment_targets = build_device_attachment_targets(rows)
    targets, sites = fence_owner_device_targets(
        {"laptop-cn": replacement_id},
        attachment_targets,
    )

    assert attachment_targets == {"laptop-cn": attached_id}
    assert targets == {"laptop-cn": attached_id}
    assert sites == ("laptop-cn",)


def test_device_attachment_targets_block_conflicting_same_name_generations() -> None:
    rows = [
        _StoredAttachments(
            attachment_refs=[
                {
                    "openoctopus_device": "laptop-cn",
                    "device_id": str(uuid4()),
                    "path": "old.txt",
                },
                {
                    "openoctopus_device": "laptop-cn",
                    "device_id": str(uuid4()),
                    "path": "new.txt",
                },
            ]
        )
    ]

    targets, sites = fence_owner_device_targets(
        {"laptop-cn": uuid4()},
        build_device_attachment_targets(rows),
    )

    assert targets == {}
    assert sites == ("laptop-cn",)


@pytest.mark.parametrize(
    ("attachment_target", "expected_targets", "mcp_visible"),
    [
        ("current", "current", True),
        ("stale", "stale", False),
        (None, None, False),
    ],
)
def test_owner_tool_state_keeps_fenced_attachment_name_in_schema(
    attachment_target: str | None,
    expected_targets: str | None,
    mcp_visible: bool,
) -> None:
    replacement_id = uuid4()
    stale_id = uuid4()
    device = OwnerDeviceSnapshot(
        id=replacement_id,
        name="laptop-cn",
        workspace_path="~/workspace",
        restrict_to_workspace=True,
        shell_timeout_max=600,
        config_revision=1,
        mcp_catalog=with_catalog_digest(
            PersistedMcpCatalog(
                version=1,
                digest="0" * 64,
                servers=[
                    PersistedMcpServerCatalog(
                        name="demo",
                        entries=[
                            PersistedMcpCatalogEntry(
                                entry_id=new_uuid7(),
                                server="demo",
                                surface="tool",
                                raw_name="search",
                                invocation_identity="search",
                                final_name="mcp_demo_search",
                                provider_description="Search with demo.",
                                input_schema={
                                    "type": "object",
                                    "properties": {"query": {"type": "string"}},
                                    "required": ["query"],
                                    "additionalProperties": False,
                                },
                                enabled=True,
                            )
                        ],
                    )
                ],
            )
        ),
    )
    target = {
        "current": replacement_id,
        "stale": stale_id,
        None: None,
    }[attachment_target]

    targets, _mcp_snapshot, schemas = _build_owner_tool_state(
        [device],
        tool_registry=build_py3_registry(),
        attachment_targets={"laptop-cn": target},
    )

    assert targets == {
        "current": {"laptop-cn": replacement_id},
        "stale": {"laptop-cn": stale_id},
        None: {},
    }[expected_targets]
    assert schemas[0]["input_schema"]["properties"]["openoctopus_device"]["enum"] == [
        "server",
        "laptop-cn",
    ]
    assert ("mcp_demo_search" in {schema["name"] for schema in schemas}) is mcp_visible


def _workspace_service() -> AsyncMock:
    return AsyncMock(spec=WorkspaceService)


def _image_block(data: bytes, media_type: str = "image/png") -> dict[str, Any]:
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": media_type,
            "data": base64.b64encode(data).decode("ascii"),
        },
    }


async def test_no_attachments_preserves_content_without_workspace_io() -> None:
    service = _workspace_service()
    content = [{"type": "text", "text": "hello"}]

    expanded = await expand_server_workspace_attachments(
        AsyncMock(spec=AsyncSession),
        workspace_service=service,
        user_id=uuid4(),
        content=content,
        attachments=[],
    )

    assert expanded == content
    assert expanded is not content
    service.read.assert_not_awaited()
    service.read_with_metadata.assert_not_awaited()


async def test_non_image_reads_only_signature_probe_and_adds_marker() -> None:
    service = _workspace_service()
    service.read.return_value = b"%PDF-1.7"
    db = AsyncMock(spec=AsyncSession)
    user_id = uuid4()

    expanded = await expand_server_workspace_attachments(
        db,
        workspace_service=service,
        user_id=user_id,
        content=[{"type": "text", "text": "summarize it"}],
        attachments=[_Attachment("server", "reports/report.pdf")],
    )

    assert expanded == [
        {
            "type": "text",
            "text": "User uploaded file to device='server', path=\"reports/report.pdf\"",
        },
        {"type": "text", "text": "summarize it"},
    ]
    service.read.assert_awaited_once_with(
        db,
        user_id=user_id,
        path="reports/report.pdf",
        length=12,
    )
    service.read_with_metadata.assert_not_awaited()


@pytest.mark.parametrize(
    ("data", "media_type"),
    [
        (b"\x89PNG\r\n\x1a\nimage", "image/png"),
        (b"\xff\xd8\xffimage", "image/jpeg"),
        (b"GIF87aimage", "image/gif"),
        (b"RIFF\x00\x00\x00\x00WEBPimage", "image/webp"),
    ],
)
async def test_image_attachment_adds_marker_and_full_snapshot(
    data: bytes,
    media_type: str,
) -> None:
    service = _workspace_service()
    service.read.return_value = data[:12]
    service.read_with_metadata.return_value = StoredObject(
        data=data,
        etag="etag-1",
        truncated=False,
    )

    expanded = await expand_server_workspace_attachments(
        AsyncMock(spec=AsyncSession),
        workspace_service=service,
        user_id=uuid4(),
        content=[],
        attachments=[_Attachment("server", "upload.bin")],
    )

    assert expanded == [
        {
            "type": "text",
            "text": "User uploaded file to device='server', path=\"upload.bin\"",
        },
        _image_block(data, media_type),
    ]


async def test_fake_image_extension_remains_a_path_marker_only() -> None:
    service = _workspace_service()
    service.read.return_value = b"not an image"

    expanded = await expand_server_workspace_attachments(
        AsyncMock(spec=AsyncSession),
        workspace_service=service,
        user_id=uuid4(),
        content=[],
        attachments=[_Attachment("server", "fake.png")],
    )

    assert expanded == [
        {
            "type": "text",
            "text": "User uploaded file to device='server', path=\"fake.png\"",
        }
    ]
    service.read_with_metadata.assert_not_awaited()


async def test_direct_image_dedup_inserts_ordered_markers_before_first_match() -> None:
    image = b"\x89PNG\r\n\x1a\nsame-image"
    service = _workspace_service()
    service.read.side_effect = [image[:12], image[:12]]
    service.read_with_metadata.side_effect = [
        StoredObject(data=image, etag="one", truncated=False),
        StoredObject(data=image, etag="two", truncated=False),
    ]
    direct = _image_block(image)

    expanded = await expand_server_workspace_attachments(
        AsyncMock(spec=AsyncSession),
        workspace_service=service,
        user_id=uuid4(),
        content=[{"type": "text", "text": "compare"}, direct, direct],
        attachments=[
            _Attachment("server", "first.png"),
            _Attachment("server", "second.png"),
        ],
    )

    assert expanded == [
        {"type": "text", "text": "compare"},
        {
            "type": "text",
            "text": "User uploaded file to device='server', path=\"first.png\"",
        },
        {
            "type": "text",
            "text": "User uploaded file to device='server', path=\"second.png\"",
        },
        direct,
        direct,
    ]
    assert sum(block.get("type") == "image" for block in expanded) == 2


async def test_image_that_changes_to_non_image_uses_the_full_read_snapshot() -> None:
    service = _workspace_service()
    service.read.return_value = b"\x89PNG\r\n\x1a\nold"
    service.read_with_metadata.return_value = StoredObject(
        data=b"now plain text",
        etag="new",
        truncated=False,
    )

    expanded = await expand_server_workspace_attachments(
        AsyncMock(spec=AsyncSession),
        workspace_service=service,
        user_id=uuid4(),
        content=[],
        attachments=[_Attachment("server", "changed")],
    )

    assert expanded == [
        {
            "type": "text",
            "text": "User uploaded file to device='server', path=\"changed\"",
        }
    ]


async def test_truncated_image_is_rejected() -> None:
    image = b"\x89PNG\r\n\x1a\nimage"
    service = _workspace_service()
    service.read.return_value = image[:12]
    service.read_with_metadata.return_value = StoredObject(
        data=image,
        etag="etag-1",
        truncated=True,
    )

    with pytest.raises(ChatError) as caught:
        await expand_server_workspace_attachments(
            AsyncMock(spec=AsyncSession),
            workspace_service=service,
            user_id=uuid4(),
            content=[],
            attachments=[_Attachment("server", "large.png")],
        )

    assert caught.value.code is ErrorCode.INVALID_MESSAGE_CONTENT


async def test_aggregate_image_limit_counts_duplicate_snapshots(monkeypatch) -> None:
    image = b"\x89PNG\r\n\x1a\n1234"
    monkeypatch.setattr(attachment_module, "MAX_ATTACHMENT_IMAGE_BYTES", len(image) * 2 - 1)
    service = _workspace_service()
    service.read.side_effect = [image[:12], image[:12]]
    service.read_with_metadata.side_effect = [
        StoredObject(data=image, etag="one", truncated=False),
        StoredObject(data=image, etag="two", truncated=False),
    ]

    with pytest.raises(ChatError) as caught:
        await expand_server_workspace_attachments(
            AsyncMock(spec=AsyncSession),
            workspace_service=service,
            user_id=uuid4(),
            content=[_image_block(image)],
            attachments=[
                _Attachment("server", "one.png"),
                _Attachment("server", "two.png"),
            ],
        )

    assert caught.value.code is ErrorCode.INVALID_MESSAGE_CONTENT


@pytest.mark.parametrize(
    "attachments",
    [
        [_Attachment("server", "")],
        [_Attachment("server", "x" * 4097)],
        [_Attachment("server", f"{index}.png") for index in range(11)],
    ],
)
async def test_attachment_contract_rejects_invalid_refs_before_io(
    attachments: list[_Attachment],
) -> None:
    service = _workspace_service()

    with pytest.raises(ChatError) as caught:
        await expand_server_workspace_attachments(
            AsyncMock(spec=AsyncSession),
            workspace_service=service,
            user_id=uuid4(),
            content=[],
            attachments=attachments,
        )

    assert caught.value.code is ErrorCode.INVALID_MESSAGE_CONTENT
    service.read.assert_not_awaited()
    service.read_with_metadata.assert_not_awaited()


async def test_marker_json_escapes_path_without_changing_normal_paths() -> None:
    service = _workspace_service()
    service.read.return_value = b"plain"

    expanded = await expand_server_workspace_attachments(
        AsyncMock(spec=AsyncSession),
        workspace_service=service,
        user_id=uuid4(),
        content=[],
        attachments=[_Attachment("server", 'folder/quote"\nline.txt')],
    )

    assert expanded == [
        {
            "type": "text",
            "text": (
                "User uploaded file to device='server', "
                'path="folder/quote\\"\\nline.txt"'
            ),
        }
    ]


async def test_any_workspace_failure_aborts_the_whole_expansion() -> None:
    service = _workspace_service()
    service.read.side_effect = [
        b"plain",
        WorkspaceError(ErrorCode.WORKSPACE_NOT_FOUND, "Workspace was not found"),
    ]

    with pytest.raises(WorkspaceError) as caught:
        await expand_server_workspace_attachments(
            AsyncMock(spec=AsyncSession),
            workspace_service=service,
            user_id=uuid4(),
            content=[],
            attachments=[
                _Attachment("server", "exists.txt"),
                _Attachment("server", "missing.txt"),
            ],
        )

    assert caught.value.code is ErrorCode.WORKSPACE_NOT_FOUND
    assert service.read.await_count == 2
    service.read_with_metadata.assert_not_awaited()
