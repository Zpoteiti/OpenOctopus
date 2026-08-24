from __future__ import annotations

import hashlib
import json
import math
import struct
import unicodedata
from collections.abc import Sequence
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_DIRECTORY_ENTRIES = 10_000
MAX_DIRECTORY_MANIFEST_BYTES = 5 * 1024 * 1024
MAX_DIRECTORY_PAGE_ITEMS = 256
MAX_DIRECTORY_PAGE_BYTES = 256 * 1024
MAX_DIRECTORY_INTEGER = (1 << 63) - 1

_MANIFEST_PREFIX = b"openoctopus-directory-manifest-v1\0"
_CONTENT_PREFIX = b"openoctopus-directory-content-v1\0"


class DirectoryContractError(ValueError):
    pass


def _normalized_json(value: object) -> object:
    if isinstance(value, BaseModel):
        return _normalized_json(value.model_dump(mode="python"))
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DirectoryContractError("canonical JSON contains a non-finite number")
        return value
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            raise DirectoryContractError("canonical JSON object keys must be strings")
        return {key: _normalized_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalized_json(child) for child in value]
    raise DirectoryContractError("value is not canonical JSON")


def canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            _normalized_json(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise DirectoryContractError("value cannot be encoded as canonical JSON") from exc


def _validate_relative_path(value: str) -> str:
    if "\x00" in value or value.startswith("/"):
        raise ValueError("relative_path must be a canonical relative path")
    components = value.split("/")
    if any(component in {"", ".", ".."} for component in components):
        raise ValueError("relative_path must be a canonical relative path")
    try:
        if value.encode("utf-8").decode("utf-8") != value:
            raise ValueError("relative_path must round-trip through UTF-8")
    except UnicodeError as exc:
        raise ValueError("relative_path must be valid UTF-8") from exc
    return value


def _validate_visible_ascii(value: str) -> str:
    if not value.isascii() or any(not 0x21 <= ord(character) <= 0x7E for character in value):
        raise ValueError("value must contain visible ASCII only")
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class DirectoryManifestEntry(_StrictModel):
    relative_path: Annotated[str, Field(min_length=1, max_length=4096)]
    size: Annotated[int, Field(ge=0, le=MAX_DIRECTORY_INTEGER)]
    fingerprint: Annotated[str, Field(min_length=1, max_length=512)]

    _path = field_validator("relative_path")(_validate_relative_path)
    _fingerprint = field_validator("fingerprint")(_validate_visible_ascii)


class DirectoryManifestDirectory(_StrictModel):
    relative_path: Annotated[str, Field(min_length=1, max_length=4096)]
    identity: Annotated[str, Field(min_length=1, max_length=512)] | None

    _path = field_validator("relative_path")(_validate_relative_path)
    _identity = field_validator("identity")(
        lambda value: None if value is None else _validate_visible_ascii(value)
    )


class DirectoryContentEntry(_StrictModel):
    relative_path: Annotated[str, Field(min_length=1, max_length=4096)]
    size: Annotated[int, Field(ge=0, le=MAX_DIRECTORY_INTEGER)]
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]

    _path = field_validator("relative_path")(_validate_relative_path)


def _encode_optional_identity(identity: str | None) -> bytes:
    if identity is None:
        return struct.pack(">H", 0)
    encoded = identity.encode("ascii")
    return struct.pack(">H", len(encoded) + 1) + encoded


def directory_manifest_sha256(
    root_identity: str | None,
    directories: Sequence[DirectoryManifestDirectory],
    entries: Sequence[DirectoryManifestEntry],
) -> str:
    merged: list[tuple[bytes, bytes]] = []
    for directory in directories:
        path = directory.relative_path.encode("utf-8")
        item = b"D" + struct.pack(">I", len(path)) + path
        item += _encode_optional_identity(directory.identity)
        merged.append((path, item))
    for entry in entries:
        path = entry.relative_path.encode("utf-8")
        fingerprint = entry.fingerprint.encode("ascii")
        item = b"F" + struct.pack(">I", len(path)) + path
        item += struct.pack(">Q", entry.size)
        item += struct.pack(">H", len(fingerprint)) + fingerprint
        merged.append((path, item))
    digest = hashlib.sha256(_MANIFEST_PREFIX)
    digest.update(b"R")
    digest.update(_encode_optional_identity(root_identity))
    for _, encoded_item in sorted(merged, key=lambda item: item[0]):
        digest.update(encoded_item)
    return digest.hexdigest()


def directory_content_sha256(entries: Sequence[DirectoryContentEntry]) -> str:
    digest = hashlib.sha256(_CONTENT_PREFIX)
    for entry in sorted(entries, key=lambda item: item.relative_path.encode("utf-8")):
        path = entry.relative_path.encode("utf-8")
        digest.update(struct.pack(">I", len(path)))
        digest.update(path)
        digest.update(struct.pack(">Q", entry.size))
        digest.update(bytes.fromhex(entry.sha256))
    return digest.hexdigest()


class DirectoryManifest(_StrictModel):
    version: Literal[1] = 1
    root_identity: Annotated[str, Field(min_length=1, max_length=512)] | None
    scanned_entries: Annotated[int, Field(ge=1, le=MAX_DIRECTORY_ENTRIES)]
    total_bytes: Annotated[int, Field(ge=0, le=MAX_DIRECTORY_INTEGER)]
    directories: Annotated[
        tuple[DirectoryManifestDirectory, ...], Field(max_length=MAX_DIRECTORY_ENTRIES)
    ]
    entries: Annotated[
        tuple[DirectoryManifestEntry, ...], Field(min_length=1, max_length=MAX_DIRECTORY_ENTRIES)
    ]
    manifest_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]

    _root_identity = field_validator("root_identity")(
        lambda value: None if value is None else _validate_visible_ascii(value)
    )

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        if self.scanned_entries != len(self.directories) + len(self.entries):
            raise ValueError("scanned_entries does not match manifest entries")

        directory_paths = [item.relative_path.encode("utf-8") for item in self.directories]
        entry_paths = [item.relative_path.encode("utf-8") for item in self.entries]
        if directory_paths != sorted(directory_paths) or entry_paths != sorted(entry_paths):
            raise ValueError("manifest paths must be sorted by UTF-8 bytes")
        if len(set(directory_paths)) != len(directory_paths) or len(set(entry_paths)) != len(
            entry_paths
        ):
            raise ValueError("manifest paths must be unique")
        if set(directory_paths) & set(entry_paths):
            raise ValueError("directory and file path conflict")

        directory_names = {item.relative_path for item in self.directories}
        all_paths = [item.relative_path for item in self.directories]
        all_paths.extend(item.relative_path for item in self.entries)
        for relative_path in all_paths:
            components = relative_path.split("/")
            for end in range(1, len(components)):
                if "/".join(components[:end]) not in directory_names:
                    raise ValueError("manifest path is missing a directory parent")

        identities_are_null = self.root_identity is None
        if any((item.identity is None) != identities_are_null for item in self.directories):
            raise ValueError("root and directory identity mode must match")

        total_bytes = sum(item.size for item in self.entries)
        if total_bytes > MAX_DIRECTORY_INTEGER or self.total_bytes != total_bytes:
            raise ValueError("total_bytes does not match the checked file-size sum")
        expected_digest = directory_manifest_sha256(
            self.root_identity, self.directories, self.entries
        )
        if self.manifest_sha256 != expected_digest:
            raise ValueError("manifest_sha256 does not match the manifest")
        if len(canonical_json_bytes(self)) > MAX_DIRECTORY_MANIFEST_BYTES:
            raise ValueError("canonical directory manifest exceeds 5 MiB")
        return self


class DirectoryManifestDirectoryItem(DirectoryManifestDirectory):
    kind: Literal["directory"] = "directory"


class DirectoryManifestFileItem(DirectoryManifestEntry):
    kind: Literal["file"] = "file"


type DirectoryManifestItem = Annotated[
    DirectoryManifestDirectoryItem | DirectoryManifestFileItem,
    Field(discriminator="kind"),
]


class DirectoryManifestPage(_StrictModel):
    offset: Annotated[int, Field(ge=0, le=MAX_DIRECTORY_ENTRIES)]
    next_offset: Annotated[int, Field(ge=1, le=MAX_DIRECTORY_ENTRIES)] | None
    items: Annotated[
        tuple[DirectoryManifestItem, ...],
        Field(min_length=1, max_length=MAX_DIRECTORY_PAGE_ITEMS),
    ]

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        if self.offset + len(self.items) > MAX_DIRECTORY_ENTRIES:
            raise ValueError("page range exceeds the directory entry bound")
        if self.next_offset is not None and self.next_offset != self.offset + len(self.items):
            raise ValueError("next_offset does not follow the page items")
        paths = [item.relative_path.encode("utf-8") for item in self.items]
        if paths != sorted(paths) or len(set(paths)) != len(paths):
            raise ValueError("page paths must be sorted and unique")
        if len(canonical_json_bytes(self)) > MAX_DIRECTORY_PAGE_BYTES:
            raise ValueError("canonical directory manifest page exceeds 256 KiB")
        return self


def create_directory_manifest(
    *,
    root_identity: str | None,
    directories: Sequence[DirectoryManifestDirectory],
    entries: Sequence[DirectoryManifestEntry],
) -> DirectoryManifest:
    sorted_directories = tuple(
        sorted(directories, key=lambda item: item.relative_path.encode("utf-8"))
    )
    sorted_entries = tuple(sorted(entries, key=lambda item: item.relative_path.encode("utf-8")))
    return DirectoryManifest(
        root_identity=root_identity,
        scanned_entries=len(sorted_directories) + len(sorted_entries),
        total_bytes=sum(item.size for item in sorted_entries),
        directories=sorted_directories,
        entries=sorted_entries,
        manifest_sha256=directory_manifest_sha256(
            root_identity, sorted_directories, sorted_entries
        ),
    )


def _page_encoded_size(item_bytes: Sequence[bytes], *, offset: int, next_offset: int | None) -> int:
    encoded_next = b"null" if next_offset is None else str(next_offset).encode("ascii")
    return (
        len(b'{"items":[')
        + sum(len(item) for item in item_bytes)
        + max(0, len(item_bytes) - 1)
        + len(b'],"next_offset":')
        + len(encoded_next)
        + len(b',"offset":')
        + len(str(offset).encode("ascii"))
        + len(b"}")
    )


def split_manifest_pages(manifest: DirectoryManifest) -> tuple[DirectoryManifestPage, ...]:
    items: list[DirectoryManifestItem] = [
        DirectoryManifestDirectoryItem(**item.model_dump()) for item in manifest.directories
    ]
    items.extend(DirectoryManifestFileItem(**item.model_dump()) for item in manifest.entries)
    items.sort(key=lambda item: item.relative_path.encode("utf-8"))

    pages: list[DirectoryManifestPage] = []
    offset = 0
    while offset < len(items):
        page_items: list[DirectoryManifestItem] = []
        page_item_bytes: list[bytes] = []
        while offset + len(page_items) < len(items) and len(page_items) < MAX_DIRECTORY_PAGE_ITEMS:
            candidate = items[offset + len(page_items)]
            candidate_bytes = canonical_json_bytes(candidate)
            candidate_count = len(page_items) + 1
            candidate_end = offset + candidate_count
            candidate_next = candidate_end if candidate_end < len(items) else None
            if (
                _page_encoded_size(
                    (*page_item_bytes, candidate_bytes),
                    offset=offset,
                    next_offset=candidate_next,
                )
                > MAX_DIRECTORY_PAGE_BYTES
            ):
                break
            page_items.append(candidate)
            page_item_bytes.append(candidate_bytes)
        if not page_items:
            raise DirectoryContractError("one directory manifest item cannot fit in a page")
        next_offset = offset + len(page_items)
        pages.append(
            DirectoryManifestPage(
                offset=offset,
                next_offset=next_offset if next_offset < len(items) else None,
                items=tuple(page_items),
            )
        )
        offset = next_offset
    return tuple(pages)


type DestinationPlatform = Literal["linux", "macos", "windows"]
type DestinationCollisionKey = tuple[bytes, ...]


def _destination_key(path: str, platform: DestinationPlatform) -> DestinationCollisionKey:
    components = path.split("/")
    if platform in {"macos", "windows"}:
        components = [
            unicodedata.normalize("NFC", component).casefold() for component in components
        ]
    return tuple(component.encode("utf-8") for component in components)


def destination_collision_keys(
    manifest: DirectoryManifest, *, platform: DestinationPlatform
) -> tuple[DestinationCollisionKey, ...]:
    if platform not in {"linux", "macos", "windows"}:
        raise DirectoryContractError("unsupported destination platform")
    file_keys: list[DestinationCollisionKey] = []
    derived_parent_keys: set[DestinationCollisionKey] = set()
    derived_parent_sources: dict[DestinationCollisionKey, str] = {}
    for entry in manifest.entries:
        key = _destination_key(entry.relative_path, platform)
        file_keys.append(key)
        components = entry.relative_path.split("/")
        for end in range(1, len(key)):
            parent_key = key[:end]
            parent_source = "/".join(components[:end])
            previous_source = derived_parent_sources.setdefault(
                parent_key, parent_source
            )
            if previous_source != parent_source:
                raise DirectoryContractError("destination parent collision")
            derived_parent_keys.add(parent_key)
    if len(set(file_keys)) != len(file_keys):
        raise DirectoryContractError("destination path collision")
    if set(file_keys) & derived_parent_keys:
        raise DirectoryContractError("destination file/parent collision")
    return tuple(file_keys)
