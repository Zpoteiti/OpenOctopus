from __future__ import annotations

import pytest

import openctopus_server.workspace.text_edit as text_edit
from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError
from openctopus_server.workspace.text_edit import (
    MAX_FUZZY_MATCH_CANDIDATES,
    MatchLevel,
    apply_text_edit,
)


def test_exact_match_wins_before_fallback_levels() -> None:
    result = apply_text_edit(
        "exact\n  exact\n",
        old_text="exact",
        new_text="changed",
        occurrence=2,
    )

    assert result.text == "exact\n  changed\n"
    assert result.replacements == 1
    assert result.match_level is MatchLevel.EXACT


def test_line_trimmed_match_preserves_outer_indentation() -> None:
    result = apply_text_edit(
        "def run():\n    first()\n    second()\n",
        old_text="first()\nsecond()",
        new_text="first()\nchanged()",
    )

    assert result.text == "def run():\n    first()\n    changed()\n"
    assert result.match_level is MatchLevel.TRIMMED


def test_trimmed_replace_all_preserves_overlapping_reverse_edit_semantics() -> None:
    result = apply_text_edit(
        "  a\n  a\n  a\n",
        old_text="\ta\n\ta",
        new_text="\ta\n\tb",
        replace_all=True,
        expected_replacements=2,
    )

    assert result.text == "  a\n  b\n  b\n"
    assert result.match_level is MatchLevel.TRIMMED


def test_smart_quote_normalization_preserves_curly_quote_style() -> None:
    result = apply_text_edit(
        "title = “Old”\n",
        old_text='title = "Old"',
        new_text='title = "New"',
    )

    assert result.text == "title = “New”\n"
    assert result.match_level is MatchLevel.NORMALIZED


def test_no_match_is_a_stable_tool_error() -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit("alpha\n", old_text="beta", new_text="changed")

    assert caught.value.code is ErrorCode.TOOL_NO_MATCH


def test_unselected_multiple_matches_are_ambiguous() -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit("same\nsame\n", old_text="same", new_text="changed")

    assert caught.value.code is ErrorCode.TOOL_AMBIGUOUS_EDIT


def test_occurrence_selects_one_match() -> None:
    result = apply_text_edit(
        "same\nother\nsame\n",
        old_text="same",
        new_text="changed",
        occurrence=2,
    )

    assert result.text == "same\nother\nchanged\n"
    assert result.replacements == 1


def test_line_hint_selects_nearest_match() -> None:
    result = apply_text_edit(
        "same\none\ntwo\nthree\nsame\n",
        old_text="same",
        new_text="changed",
        line_hint=4,
    )

    assert result.text == "same\none\ntwo\nthree\nchanged\n"


def test_line_hint_tie_is_ambiguous() -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit(
            "same\none\nsame\n",
            old_text="same",
            new_text="changed",
            line_hint=2,
        )

    assert caught.value.code is ErrorCode.TOOL_AMBIGUOUS_EDIT


@pytest.mark.parametrize("selector", [{"occurrence": 50_000}, {"line_hint": 50_000}])
def test_exact_selectors_retain_bounded_match_state(monkeypatch, selector) -> None:
    def fail_match_construction(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("exact selectors must not construct Match objects")

    monkeypatch.setattr(text_edit, "_Match", fail_match_construction)

    result = apply_text_edit(
        "same\n" * 50_000,
        old_text="same",
        new_text="changed",
        **selector,
    )

    assert result.text.endswith("changed\n")
    assert result.replacements == 1


def test_replace_all_and_expected_replacements() -> None:
    result = apply_text_edit(
        "same\nsame\n",
        old_text="same",
        new_text="changed",
        replace_all=True,
        expected_replacements=2,
    )

    assert result.text == "changed\nchanged\n"
    assert result.replacements == 2


def test_exact_replace_all_does_not_materialize_a_match_per_occurrence(monkeypatch) -> None:
    def fail_match_construction(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("exact replace_all must not construct Match objects")

    monkeypatch.setattr(text_edit, "_Match", fail_match_construction)
    size = 8 * 1024 * 1024

    result = apply_text_edit(
        "a" * size,
        old_text="a",
        new_text="b",
        replace_all=True,
        expected_replacements=size,
    )

    assert len(result.text) == size
    assert result.text.count("b") == size
    assert result.replacements == size
    assert result.match_level is MatchLevel.EXACT


def test_pathological_normalized_matches_stop_at_the_fuzzy_candidate_limit() -> None:
    size = 8 * 1024 * 1024

    with pytest.raises(ToolError) as caught:
        apply_text_edit(
            "“" * size,
            old_text='"',
            new_text="changed",
            replace_all=True,
        )

    assert caught.value.code is ErrorCode.TOOL_AMBIGUOUS_EDIT
    assert str(MAX_FUZZY_MATCH_CANDIDATES) in caught.value.message


def test_exact_replace_all_rejects_projected_output_before_replace() -> None:
    class GuardedText(str):
        def replace(self, old: str, new: str, count: int = -1) -> str:
            if old in {"\r\n", "\r"}:
                return self
            raise AssertionError("replacement ran before the projected-size check")

    size = 8 * 1024 * 1024
    text = GuardedText("a" * size)

    with pytest.raises(ToolError) as caught:
        apply_text_edit(
            text,
            old_text="a",
            new_text="b" * (1024 * 1024),
            replace_all=True,
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT


def test_create_missing_enforces_the_utf8_output_budget() -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit(
            None,
            old_text="",
            new_text="😀" * (2 * 1024 * 1024 + 1),
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT


def test_trimmed_indentation_expansion_is_sized_before_rendering(monkeypatch) -> None:
    def fail_render(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("indentation rendered before the projected-size check")

    monkeypatch.setattr(text_edit, "_apply_target_indentation", fail_render)
    size = 8 * 1024 * 1024

    with pytest.raises(ToolError) as caught:
        apply_text_edit(
            f"{' ' * (size - 2)}x\n",
            old_text="\tx",
            new_text="\tx\n\ty",
        )

    assert caught.value.code is ErrorCode.WORKSPACE_FILE_TOO_LARGE_TO_EDIT


def test_trimmed_late_mismatch_uses_linear_token_comparisons(monkeypatch) -> None:
    comparisons = 0

    def counted_equal(left: str, right: str) -> bool:
        nonlocal comparisons
        comparisons += 1
        return left == right

    monkeypatch.setattr(text_edit, "_tokens_equal", counted_equal)
    pattern_lines = 2_000
    text_lines = 10_000

    with pytest.raises(ToolError) as caught:
        apply_text_edit(
            "a\n" * text_lines,
            old_text="\n".join([*["a"] * (pattern_lines - 1), "b"]),
            new_text="changed",
        )

    assert caught.value.code is ErrorCode.TOOL_NO_MATCH
    assert comparisons <= 4 * (pattern_lines + text_lines)


def test_expected_replacements_mismatch_does_not_return_an_edit() -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit(
            "same\nsame\n",
            old_text="same",
            new_text="changed",
            replace_all=True,
            expected_replacements=1,
        )

    assert caught.value.code is ErrorCode.TOOL_INVALID_ARGS


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"replace_all": True, "occurrence": 1}, "occurrence"),
        ({"replace_all": True, "line_hint": 1}, "line_hint"),
        ({"occurrence": 1, "line_hint": 1}, "line_hint"),
        ({"occurrence": 0}, "occurrence"),
        ({"line_hint": 0}, "line_hint"),
        ({"expected_replacements": 0}, "expected_replacements"),
    ],
)
def test_invalid_selectors_are_rejected(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit("same\n", old_text="same", new_text="changed", **kwargs)  # type: ignore[arg-type]

    assert caught.value.code is ErrorCode.TOOL_INVALID_ARGS
    assert message in caught.value.message


def test_empty_old_text_creates_only_a_missing_target() -> None:
    result = apply_text_edit(None, old_text="", new_text="created\n")

    assert result.text == "created\n"
    assert result.replacements == 0
    assert result.created is True
    assert result.match_level is None


def test_empty_old_text_does_not_overwrite_an_existing_target() -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit("", old_text="", new_text="created\n")

    assert caught.value.code is ErrorCode.TOOL_INVALID_ARGS


def test_nonempty_edit_does_not_create_a_missing_target() -> None:
    with pytest.raises(ToolError) as caught:
        apply_text_edit(None, old_text="old", new_text="new")

    assert caught.value.code is ErrorCode.TOOL_NO_MATCH


def test_crlf_line_endings_are_restored_after_a_trimmed_match() -> None:
    result = apply_text_edit(
        "def run():\r\n    first()\r\n    second()\r\n",
        old_text="first()\nsecond()",
        new_text="first()\nchanged()",
    )

    assert result.text == "def run():\r\n    first()\r\n    changed()\r\n"
