from __future__ import annotations

import unicodedata

_FORBIDDEN_IDENTIFIER_CHARS = frozenset("/\\\0@:<>" + '"|?*')
_MAX_IDENTIFIER_LENGTH = 64


def validate_display_identifier_name(raw: str) -> str:
    """Normalize and validate a workspace or skill display name."""
    normalized = unicodedata.normalize("NFC", raw)
    if any(
        character in _FORBIDDEN_IDENTIFIER_CHARS or ord(character) <= 0x1F or ord(character) == 0x7F
        for character in normalized
    ):
        raise ValueError("identifier name contains a forbidden character")
    name = " ".join(normalized.split())
    if not name or name in {".", ".."}:
        raise ValueError("identifier name is empty or reserved")
    if len(name) > _MAX_IDENTIFIER_LENGTH:
        raise ValueError("identifier name is longer than 64 characters")
    return name
