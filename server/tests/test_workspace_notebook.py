from __future__ import annotations

import json

import pytest

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError
from openctopus_server.workspace.notebook import edit_notebook


def _notebook() -> str:
    return json.dumps(
        {
            "cells": [
                {
                    "cell_type": "code",
                    "execution_count": 7,
                    "metadata": {"tag": "keep"},
                    "outputs": [{"output_type": "stream", "name": "stdout", "text": "old\n"}],
                    "source": ["print('old')\n"],
                },
                {"cell_type": "markdown", "metadata": {}, "source": ["old heading\n"]},
            ],
            "metadata": {"kernelspec": {"name": "python3"}},
            "nbformat": 4,
            "nbformat_minor": 5,
        }
    )


def test_replace_changes_only_the_selected_source() -> None:
    edited = json.loads(edit_notebook(_notebook(), cell_index=0, new_source="print('new')\n"))

    cell = edited["cells"][0]
    assert cell["source"] == "print('new')\n"
    assert cell["outputs"] == [{"output_type": "stream", "name": "stdout", "text": "old\n"}]
    assert cell["execution_count"] == 7
    assert cell["metadata"] == {"tag": "keep"}
    assert edited["cells"][1]["source"] == ["old heading\n"]


def test_insert_code_cell_after_index_has_safe_output_fields() -> None:
    edited = json.loads(
        edit_notebook(
            _notebook(),
            cell_index=0,
            new_source="value = 1\n",
            edit_mode="insert",
        )
    )

    assert edited["cells"][1] == {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": "value = 1\n",
    }


def test_insert_markdown_cell_has_no_code_output_fields() -> None:
    edited = json.loads(
        edit_notebook(
            _notebook(),
            cell_index=0,
            new_source="# Heading\n",
            cell_type="markdown",
            edit_mode="insert",
        )
    )

    assert edited["cells"][1] == {
        "cell_type": "markdown",
        "metadata": {},
        "source": "# Heading\n",
    }


def test_delete_removes_only_the_selected_cell() -> None:
    edited = json.loads(edit_notebook(_notebook(), cell_index=0, edit_mode="delete"))

    assert len(edited["cells"]) == 1
    assert edited["cells"][0]["cell_type"] == "markdown"


@pytest.mark.parametrize(
    "content",
    [
        "not JSON",
        "[]",
        '{"cells": {}}',
        '{"cells": [null]}',
        '{"cells": [{"cell_type": "code", "source": 42, "outputs": []}]}',
        '{"cells": [{"cell_type": "code", "source": "x", "outputs": {}}]}',
        '{"cells": [{"cell_type": "markdown", "source": "x", "metadata": []}]}',
    ],
)
def test_invalid_notebook_or_cell_shape_is_rejected(content: str) -> None:
    with pytest.raises(ToolError) as caught:
        edit_notebook(content, cell_index=0, new_source="new")

    assert caught.value.code is ErrorCode.TOOL_INVALID_NOTEBOOK


@pytest.mark.parametrize("cell_index", [-1, 2])
def test_cell_index_out_of_range_is_stable(cell_index: int) -> None:
    with pytest.raises(ToolError) as caught:
        edit_notebook(_notebook(), cell_index=cell_index, new_source="new")

    assert caught.value.code is ErrorCode.TOOL_CELL_INDEX_OUT_OF_RANGE


@pytest.mark.parametrize("edit_mode", ["replace", "insert"])
def test_source_is_required_for_source_modes(edit_mode: str) -> None:
    with pytest.raises(ToolError) as caught:
        edit_notebook(_notebook(), cell_index=0, edit_mode=edit_mode)  # type: ignore[arg-type]

    assert caught.value.code is ErrorCode.TOOL_INVALID_ARGS


def test_invalid_edit_mode_is_rejected() -> None:
    with pytest.raises(ToolError) as caught:
        edit_notebook(_notebook(), cell_index=0, edit_mode="move")  # type: ignore[arg-type]

    assert caught.value.code is ErrorCode.TOOL_INVALID_ARGS
