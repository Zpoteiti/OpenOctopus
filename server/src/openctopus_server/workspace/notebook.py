from __future__ import annotations

import json
from typing import Literal, cast

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.errors.exceptions import ToolError

CellType = Literal["code", "markdown"]
EditMode = Literal["replace", "insert", "delete"]


def edit_notebook(
    content: str,
    *,
    cell_index: int,
    new_source: str | None = None,
    cell_type: CellType = "code",
    edit_mode: EditMode = "replace",
) -> str:
    if edit_mode not in {"replace", "insert", "delete"}:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "edit_mode is invalid")
    if cell_type not in {"code", "markdown"}:
        raise ToolError(ErrorCode.TOOL_INVALID_ARGS, "cell_type is invalid")
    if edit_mode in {"replace", "insert"} and new_source is None:
        raise ToolError(
            ErrorCode.TOOL_INVALID_ARGS,
            f"new_source is required for {edit_mode} mode",
        )

    notebook = _parse_notebook(content)
    cells = cast(list[dict[str, object]], notebook["cells"])
    if cell_index < 0 or cell_index >= len(cells):
        raise ToolError(
            ErrorCode.TOOL_CELL_INDEX_OUT_OF_RANGE,
            "cell_index is outside the notebook",
        )

    if edit_mode == "delete":
        del cells[cell_index]
    elif edit_mode == "insert":
        cells.insert(cell_index + 1, _new_cell(cell_type, cast(str, new_source)))
    else:
        cells[cell_index]["source"] = cast(str, new_source)

    return json.dumps(notebook, ensure_ascii=False)


def _parse_notebook(content: str) -> dict[str, object]:
    try:
        value = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook is not valid JSON") from exc
    if not isinstance(value, dict) or not isinstance(value.get("cells"), list):
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook must contain a cells array")
    if "metadata" in value and not isinstance(value["metadata"], dict):
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook metadata must be an object")
    for cell in value["cells"]:
        _validate_cell(cell)
    return cast(dict[str, object], value)


def _validate_cell(value: object) -> None:
    if not isinstance(value, dict):
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook cells must be objects")
    if value.get("cell_type") not in {"code", "markdown", "raw"}:
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook cell_type is invalid")
    source = value.get("source")
    if not isinstance(source, str) and not (
        isinstance(source, list) and all(isinstance(part, str) for part in source)
    ):
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook cell source is invalid")
    if "metadata" in value and not isinstance(value["metadata"], dict):
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Notebook cell metadata must be an object")
    if value["cell_type"] == "code" and not isinstance(value.get("outputs"), list):
        raise ToolError(ErrorCode.TOOL_INVALID_NOTEBOOK, "Code cell outputs must be an array")


def _new_cell(cell_type: CellType, source: str) -> dict[str, object]:
    cell: dict[str, object] = {
        "cell_type": cell_type,
        "metadata": {},
        "source": source,
    }
    if cell_type == "code":
        cell["execution_count"] = None
        cell["outputs"] = []
    return cell
