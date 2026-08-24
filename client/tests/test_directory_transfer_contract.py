from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

import openoctopus_client.tools.directory_contract as contract

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "fixtures"
    / "directory_transfer_v1"
    / "vectors.json"
)


def _vectors() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(_FIXTURE_PATH.read_text(encoding="utf-8")))


def _entry(path: str, *, size: int = 0, fingerprint: str = "f") -> contract.DirectoryManifestEntry:
    return contract.DirectoryManifestEntry(
        relative_path=path,
        size=size,
        fingerprint=fingerprint,
    )


def _directory(
    path: str, *, identity: str | None = "directory-id"
) -> contract.DirectoryManifestDirectory:
    return contract.DirectoryManifestDirectory(relative_path=path, identity=identity)


def test_shared_manifest_and_content_digest_vectors() -> None:
    vectors = _vectors()
    assert vectors["version"] == 1
    for vector in vectors["manifest_vectors"]:
        raw = vector["manifest"]
        manifest = contract.DirectoryManifest.model_validate_json(
            json.dumps(raw, ensure_ascii=False, separators=(",", ":")),
            strict=True,
        )
        encoded = contract.canonical_json_bytes(manifest)
        assert (
            contract.directory_manifest_sha256(
                manifest.root_identity,
                manifest.directories,
                manifest.entries,
            )
            == raw["manifest_sha256"]
        )
        assert len(encoded) == vector["canonical_bytes"]
        assert hashlib.sha256(encoded).hexdigest() == vector["canonical_sha256"]

    content = vectors["content_vector"]
    entries = tuple(
        contract.DirectoryContentEntry.model_validate(item, strict=True)
        for item in content["entries"]
    )
    assert contract.directory_content_sha256(entries) == content["sha256"]


def test_canonical_json_preserves_unicode_and_uses_exact_escaping() -> None:
    encoded = contract.canonical_json_bytes({"z": "cafe\u0301", "a": '\u4e2d"\\\n\u0001/end'})
    assert encoded == b'{"a":"\xe4\xb8\xad\\"\\\\\\n\\u0001/end","z":"cafe\xcc\x81"}'
    assert not encoded.startswith(b"\xef\xbb\xbf")


@pytest.mark.parametrize(
    "path",
    ["", "/absolute", ".", "..", "a//b", "a/./b", "a/../b", "nul\x00name", "bad\ud800"],
)
def test_manifest_path_is_canonical_utf8(path: str) -> None:
    with pytest.raises(ValidationError):
        _entry(path)


def test_models_are_strict_bounded_and_forbid_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        contract.DirectoryManifestEntry.model_validate(
            {"relative_path": "a", "size": True, "fingerprint": "f"}, strict=True
        )
    with pytest.raises(ValidationError):
        _entry("a", fingerprint="not visible ascii ")
    with pytest.raises(ValidationError):
        _directory("a", identity="bad\nidentity")
    with pytest.raises(ValidationError):
        contract.DirectoryManifestEntry.model_validate(
            {"relative_path": "a", "size": 0, "fingerprint": "f", "extra": 1},
            strict=True,
        )
    with pytest.raises(ValidationError):
        _entry("a" * 4097)
    with pytest.raises(ValidationError):
        _entry("a", fingerprint="f" * 513)


def test_manifest_rejects_unsorted_duplicates_conflicts_and_missing_parents() -> None:
    manifest = contract.create_directory_manifest(
        root_identity="root",
        directories=(_directory("a"),),
        entries=(_entry("a/one"), _entry("z")),
    )
    raw = manifest.model_dump()

    reversed_entries = dict(raw)
    reversed_entries["entries"] = tuple(reversed(raw["entries"]))
    with pytest.raises(ValidationError, match="sorted"):
        contract.DirectoryManifest.model_validate(reversed_entries, strict=True)

    duplicate = dict(raw)
    duplicate["entries"] = (raw["entries"][0], raw["entries"][0])
    duplicate["scanned_entries"] = 3
    with pytest.raises(ValidationError, match="unique"):
        contract.DirectoryManifest.model_validate(duplicate, strict=True)

    cross_kind = dict(raw)
    cross_kind["directories"] = (_directory("z").model_dump(),)
    with pytest.raises(ValidationError, match="path"):
        contract.DirectoryManifest.model_validate(cross_kind, strict=True)

    missing_parent = dict(raw)
    missing_parent["directories"] = ()
    missing_parent["scanned_entries"] = 2
    with pytest.raises(ValidationError, match="parent"):
        contract.DirectoryManifest.model_validate(missing_parent, strict=True)


def test_manifest_rejects_count_sum_digest_identity_and_integer_mismatch() -> None:
    manifest = contract.create_directory_manifest(
        root_identity="root",
        directories=(),
        entries=(_entry("one", size=1),),
    )
    for field, value in (
        ("scanned_entries", 2),
        ("total_bytes", 2),
        ("manifest_sha256", "0" * 64),
    ):
        raw = manifest.model_dump()
        raw[field] = value
        with pytest.raises(ValidationError):
            contract.DirectoryManifest.model_validate(raw, strict=True)

    with pytest.raises(ValidationError, match="identity"):
        contract.create_directory_manifest(
            root_identity="root",
            directories=(_directory("empty", identity=None),),
            entries=(_entry("one"),),
        )
    with pytest.raises(ValidationError):
        _entry("one", size=contract.MAX_DIRECTORY_INTEGER + 1)
    with pytest.raises(ValidationError, match="total_bytes"):
        contract.create_directory_manifest(
            root_identity="root",
            directories=(),
            entries=(
                _entry("one", size=contract.MAX_DIRECTORY_INTEGER),
                _entry("two", size=1),
            ),
        )


def test_manifest_entry_count_exact_boundary() -> None:
    entries = tuple(_entry(f"{index:05d}") for index in range(10_000))
    manifest = contract.create_directory_manifest(
        root_identity="root", directories=(), entries=entries
    )
    assert manifest.scanned_entries == contract.MAX_DIRECTORY_ENTRIES == 10_000

    raw = manifest.model_dump()
    raw["entries"] = (*raw["entries"], _entry("10000").model_dump())
    raw["scanned_entries"] = 10_001
    with pytest.raises(ValidationError):
        contract.DirectoryManifest.model_validate(raw, strict=True)


def _manifest_payload_of_exact_size(target: int) -> dict[str, Any]:
    entries = [
        {
            "relative_path": f"{index:04d}-" + "p" * (4096 - 5),
            "size": 0,
            "fingerprint": "f" * 512,
        }
        for index in range(1200)
    ]

    def payload() -> dict[str, Any]:
        values = tuple(contract.DirectoryManifestEntry.model_validate(item) for item in entries)
        return {
            "version": 1,
            "root_identity": "root",
            "scanned_entries": len(entries),
            "total_bytes": 0,
            "directories": (),
            "entries": tuple(item.model_dump() for item in values),
            "manifest_sha256": contract.directory_manifest_sha256("root", (), values),
        }

    raw = payload()
    excess = len(contract.canonical_json_bytes(raw)) - target
    assert excess >= 0
    for item in reversed(entries):
        for field, minimum in (("fingerprint", 1), ("relative_path", 5)):
            available = len(item[field]) - minimum
            removed = min(excess, available)
            item[field] = item[field][:-removed] if removed else item[field]
            excess -= removed
            if excess == 0:
                result = payload()
                assert len(contract.canonical_json_bytes(result)) == target
                return result
    raise AssertionError("could not construct exact-size manifest")


def test_manifest_canonical_five_mib_exact_and_plus_one() -> None:
    assert contract.MAX_DIRECTORY_MANIFEST_BYTES == 5 * 1024 * 1024
    exact = _manifest_payload_of_exact_size(contract.MAX_DIRECTORY_MANIFEST_BYTES)
    manifest = contract.DirectoryManifest.model_validate(exact, strict=True)
    assert len(contract.canonical_json_bytes(manifest)) == contract.MAX_DIRECTORY_MANIFEST_BYTES

    too_large = manifest.model_dump()
    entries = list(too_large["entries"])
    entries[-1]["relative_path"] += "x"
    values = tuple(contract.DirectoryManifestEntry.model_validate(item) for item in entries)
    too_large["entries"] = tuple(entries)
    too_large["manifest_sha256"] = contract.directory_manifest_sha256(
        manifest.root_identity, manifest.directories, values
    )
    with pytest.raises(ValidationError, match="5 MiB"):
        contract.DirectoryManifest.model_validate(too_large, strict=True)


def test_manifest_pages_split_on_item_and_encoded_byte_bounds() -> None:
    manifest = contract.create_directory_manifest(
        root_identity="root",
        directories=(),
        entries=tuple(_entry(f"{index:04d}") for index in range(257)),
    )
    pages = contract.split_manifest_pages(manifest)
    assert [len(page.items) for page in pages] == [256, 1]
    assert [page.offset for page in pages] == [0, 256]
    assert [page.next_offset for page in pages] == [256, None]

    large = contract.create_directory_manifest(
        root_identity="root",
        directories=(),
        entries=tuple(
            _entry(f"{index:04d}-" + "x" * 4080, fingerprint="f" * 512) for index in range(80)
        ),
    )
    byte_pages = contract.split_manifest_pages(large)
    assert len(byte_pages) > 1
    assert sum(len(page.items) for page in byte_pages) == 80
    assert all(
        len(contract.canonical_json_bytes(page)) <= contract.MAX_DIRECTORY_PAGE_BYTES
        for page in byte_pages
    )


def test_page_canonical_256_kib_exact_and_plus_one() -> None:
    assert contract.MAX_DIRECTORY_PAGE_BYTES == 256 * 1024
    items = [
        contract.DirectoryManifestFileItem(
            relative_path=f"{index:04d}-" + "x" * 4080,
            size=0,
            fingerprint="f" * 512,
        )
        for index in range(70)
    ]
    raw: dict[str, Any] = {"offset": 0, "next_offset": None, "items": items}
    excess = len(contract.canonical_json_bytes(raw)) - contract.MAX_DIRECTORY_PAGE_BYTES
    assert excess >= 0
    for item in reversed(items):
        for field, minimum in (("fingerprint", 1), ("relative_path", 5)):
            value = getattr(item, field)
            available = len(value) - minimum
            removed = min(excess, available)
            if removed:
                object.__setattr__(item, field, value[:-removed])
            excess -= removed
            if excess == 0:
                break
        if excess == 0:
            break
    assert excess == 0
    exact = contract.DirectoryManifestPage(offset=0, next_offset=None, items=tuple(items))
    assert len(contract.canonical_json_bytes(exact)) == contract.MAX_DIRECTORY_PAGE_BYTES

    expanded = list(exact.items)
    last = expanded[-1]
    expanded[-1] = last.model_copy(update={"relative_path": last.relative_path + "x"})
    with pytest.raises(ValidationError, match="256 KiB"):
        contract.DirectoryManifestPage(offset=0, next_offset=None, items=tuple(expanded))


def test_destination_collision_keys_are_platform_deterministic() -> None:
    case_manifest = contract.create_directory_manifest(
        root_identity="root",
        directories=(),
        entries=(_entry("A"), _entry("a")),
    )
    assert len(contract.destination_collision_keys(case_manifest, platform="linux")) == 2
    with pytest.raises(contract.DirectoryContractError, match="collision"):
        contract.destination_collision_keys(case_manifest, platform="macos")
    with pytest.raises(contract.DirectoryContractError, match="collision"):
        contract.destination_collision_keys(case_manifest, platform="windows")

    unicode_manifest = contract.create_directory_manifest(
        root_identity="root",
        directories=(),
        entries=(_entry("cafe\u0301"), _entry("caf\u00e9")),
    )
    assert len(contract.destination_collision_keys(unicode_manifest, platform="linux")) == 2
    with pytest.raises(contract.DirectoryContractError, match="collision"):
        contract.destination_collision_keys(unicode_manifest, platform="macos")

    scan_only = contract.create_directory_manifest(
        root_identity="root",
        directories=(_directory("empty"),),
        entries=(_entry("EMPTY"),),
    )
    assert contract.destination_collision_keys(scan_only, platform="macos")

    parent_collision = contract.create_directory_manifest(
        root_identity="root",
        directories=(_directory("a"),),
        entries=(_entry("A"), _entry("a/child")),
    )
    with pytest.raises(contract.DirectoryContractError, match="parent"):
        contract.destination_collision_keys(parent_collision, platform="macos")

    merged_parent_collision = contract.create_directory_manifest(
        root_identity="root",
        directories=(_directory("A"), _directory("a")),
        entries=(_entry("A/x"), _entry("a/y")),
    )
    assert len(
        contract.destination_collision_keys(
            merged_parent_collision,
            platform="linux",
        )
    ) == 2
    with pytest.raises(contract.DirectoryContractError, match="parent"):
        contract.destination_collision_keys(
            merged_parent_collision,
            platform="macos",
        )
    with pytest.raises(contract.DirectoryContractError, match="parent"):
        contract.destination_collision_keys(
            merged_parent_collision,
            platform="windows",
        )
