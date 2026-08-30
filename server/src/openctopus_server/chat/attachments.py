from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from openctopus_server.db.models import Device
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


class StoredAttachmentRefs(Protocol):
    @property
    def attachment_refs(self) -> Sequence[Mapping[str, Any]]: ...


async def normalize_browser_attachment_refs(
    db: AsyncSession,
    *,
    user_id: UUID,
    attachments: Sequence[BrowserAttachmentRef],
) -> list[dict[str, Any]]:
    """Validate attachment authority and return canonical JSON-safe refs."""
    _validate_attachments(attachments)
    client_refs = [
        attachment
        for attachment in attachments
        if attachment.openoctopus_device != "server"
    ]
    if client_refs:
        names = {attachment.openoctopus_device for attachment in client_refs}
        owned = {
            name: device_id
            for name, device_id in (
                await db.execute(
                    select(Device.name, Device.id).where(
                        Device.user_id == user_id,
                        Device.name.in_(names),
                    )
                )
            ).all()
        }
        if any(
            owned.get(attachment.openoctopus_device)
            != getattr(attachment, "device_id", None)
            for attachment in client_refs
        ):
            raise ChatError(
                ErrorCode.TOOL_DEVICE_UNREACHABLE,
                "Attachment device is unavailable",
            )

    normalized: list[dict[str, Any]] = []
    for attachment in attachments:
        ref: dict[str, Any] = {
            "openoctopus_device": attachment.openoctopus_device,
            "path": attachment.path,
        }
        device_id = getattr(attachment, "device_id", None)
        if device_id is not None:
            ref["device_id"] = str(device_id)
        normalized.append(ref)
    return normalized


def build_device_attachment_targets(
    rows: Sequence[StoredAttachmentRefs],
) -> dict[str, UUID | None]:
    """Collect name-level UUID fences from provider-visible attachment refs."""
    targets: dict[str, UUID | None] = {}
    for row in rows:
        for ref in row.attachment_refs or ():
            name = ref.get("openoctopus_device")
            if not isinstance(name, str) or name == "server":
                continue
            raw_device_id = ref.get("device_id")
            try:
                device_id = UUID(str(raw_device_id))
            except (TypeError, ValueError) as exc:
                raise RuntimeError("Stored Client attachment ref is invalid") from exc
            previous = targets.get(name)
            if name in targets and previous != device_id:
                targets[name] = None
            else:
                targets[name] = device_id
    return targets


def fence_owner_device_targets(
    current_targets: Mapping[str, UUID],
    attachment_targets: Mapping[str, UUID | None],
) -> tuple[dict[str, UUID], tuple[str, ...]]:
    """Prefer accepted attachment generations and retain blocked schema sites."""
    sites = tuple(dict.fromkeys((*current_targets, *attachment_targets)))
    fenced = dict(current_targets)
    for name, device_id in attachment_targets.items():
        if device_id is None:
            fenced.pop(name, None)
        else:
            fenced[name] = device_id
    return fenced, sites


def strip_provider_attachment_markers(
    content: Sequence[Mapping[str, Any]],
    attachment_refs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Remove exact provider-only marker blocks from the public projection."""
    marker_counts: dict[str, int] = {}
    for ref in attachment_refs:
        device = ref.get("openoctopus_device")
        path = ref.get("path")
        if not isinstance(device, str) or not isinstance(path, str):
            continue
        marker = (
            _attachment_marker(path)
            if device == "server"
            else _device_attachment_marker(device, path)
        )["text"]
        marker_counts[marker] = marker_counts.get(marker, 0) + 1

    projected: list[dict[str, Any]] = []
    for block in content:
        text = block.get("text") if block.get("type") == "text" else None
        if isinstance(text, str) and marker_counts.get(text, 0):
            marker_counts[text] -= 1
            continue
        projected.append(dict(block))
    return projected


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
        if attachment.openoctopus_device != "server":
            prefix.append(_device_attachment_marker(attachment.openoctopus_device, attachment.path))
            continue
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


def _device_attachment_marker(device: str, path: str) -> dict[str, str]:
    encoded_path = json.dumps(path, ensure_ascii=False)
    return {
        "type": "text",
        "text": (
            f"User attached existing file from device='{device}', path={encoded_path}. "
            "Use read_file on that device to inspect it."
        ),
    }


def _sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _base64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _invalid(message: str) -> ChatError:
    return ChatError(ErrorCode.INVALID_MESSAGE_CONTENT, message)
