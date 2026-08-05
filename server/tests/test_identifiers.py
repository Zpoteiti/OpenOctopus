import pytest

from openctopus_server.identifiers import validate_display_identifier_name


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("  Xmas   gift  ", "Xmas gift"),
        ("Cafe\u0301", "Caf\u00e9"),
        ("\u751f\u4ea7 \u90e8\u95e8", "\u751f\u4ea7 \u90e8\u95e8"),
        ("release_2026+(blue)", "release_2026+(blue)"),
    ],
)
def test_display_identifier_normalizes_safe_names(raw: str, expected: str) -> None:
    assert validate_display_identifier_name(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        ".",
        "..",
        "team/name",
        "team\\name",
        "team@name",
        "team:name",
        "team<name",
        'team"name',
        "team|name",
        "team?name",
        "team*name",
        "team\nname",
        "x" * 65,
    ],
)
def test_display_identifier_rejects_reserved_or_unsafe_names(raw: str) -> None:
    with pytest.raises(ValueError):
        validate_display_identifier_name(raw)
