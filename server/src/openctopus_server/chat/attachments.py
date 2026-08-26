from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ChatError
from openctopus_server.workspace.file_content import image_media_type
from openctopus_server.workspace.fs import MAX_READ_BYTES
from openctopus_server.workspace.service import WorkspaceService

MAX_BROWSER_ATTACHMENTS = 10
MAX_ATTACHMENT_IMAGE_BYTES = MAX_READ_BYTES
_IMAGE_SIGNATURE_BYTES = 12


class BrowserAttachmentRef(Protocol):
    @property
    def openoctopus_device(self) -> str: ...

    @property
    def path(self) -> str: ...


async def expand_server_workspace_attachments(
    db: AsyncSession,
    *,
    workspace_service: WorkspaceService,
    user_id: UUID,
    content: Sequence[Mapping[str, Any]],
    attachments: Sequence[BrowserAttachmentRef],
) -> list[dict[str, Any]]:
    """Expand authorized server file refs into provider-shaped user blocks."""
    _validate_attachments(attachments)
    copied_content = [dict(block) for block in content]
    if not attachments:
        return copied_content

    direct_images = await _direct_image_positions(copied_content)
    prefix: list[dict[str, Any]] = []
    markers_before_direct: dict[int, list[dict[str, Any]]] = {}
    image_bytes = 0

    for attachment in attachments:
        marker = _attachment_marker(attachment.path)
        header = await workspace_service.read(
            db,
            user_id=user_id,
            path=attachment.path,
            length=_IMAGE_SIGNATURE_BYTES,
        )
        if image_media_type(header) is None:
            prefix.append(marker)
            continue

        snapshot = await workspace_service.read_with_metadata(
            db,
            user_id=user_id,
            path=attachment.path,
        )
        media_type = image_media_type(snapshot.data)
        if media_type is None:
            prefix.append(marker)
            continue
        if snapshot.truncated:
            raise _invalid("Browser attachment image exceeds the 8 MiB limit")

        image_bytes += len(snapshot.data)
        if image_bytes > MAX_ATTACHMENT_IMAGE_BYTES:
            raise _invalid("Browser attachment images exceed the 8 MiB aggregate limit")

        digest = await asyncio.to_thread(_sha256, snapshot.data)
        matching_index = direct_images.get(digest)
        if matching_index is not None:
            markers_before_direct.setdefault(matching_index, []).append(marker)
            continue

        encoded = await asyncio.to_thread(_base64, snapshot.data)
        prefix.extend(
            [
                marker,
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": encoded,
                    },
                },
            ]
        )

    expanded = prefix
    for index, block in enumerate(copied_content):
        expanded.extend(markers_before_direct.get(index, ()))
        expanded.append(block)
    return expanded


def _validate_attachments(attachments: Sequence[BrowserAttachmentRef]) -> None:
    if len(attachments) > MAX_BROWSER_ATTACHMENTS:
        raise _invalid(f"Browser messages accept at most {MAX_BROWSER_ATTACHMENTS} attachments")
    for attachment in attachments:
        if attachment.openoctopus_device != "server":
            raise _invalid("Browser attachments must target openoctopus_device='server'")
        if not isinstance(attachment.path, str) or not 1 <= len(attachment.path) <= 4096:
            raise _invalid("Browser attachment paths must contain 1 to 4096 characters")


async def _direct_image_positions(
    content: Sequence[Mapping[str, Any]],
) -> dict[bytes, int]:
    try:
        return await asyncio.to_thread(_collect_direct_image_positions, content)
    except (KeyError, TypeError, ValueError, binascii.Error) as exc:
        raise _invalid("Direct image content is malformed") from exc


def _collect_direct_image_positions(
    content: Sequence[Mapping[str, Any]],
) -> dict[bytes, int]:
    positions: dict[bytes, int] = {}
    for index, block in enumerate(content):
        if block.get("type") != "image":
            continue
        source = block["source"]
        if not isinstance(source, Mapping):
            raise TypeError("image source must be an object")
        encoded = source["data"]
        if not isinstance(encoded, str):
            raise TypeError("image data must be a string")
        decoded = base64.b64decode(encoded, validate=True)
        positions.setdefault(_sha256(decoded), index)
    return positions


def _attachment_marker(path: str) -> dict[str, str]:
    encoded_path = json.dumps(path, ensure_ascii=False)
    return {
        "type": "text",
        "text": f"User uploaded file to device='server', path={encoded_path}",
    }


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _base64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _invalid(message: str) -> ChatError:
    return ChatError(ErrorCode.INVALID_MESSAGE_CONTENT, message)
