import asyncio
from copy import deepcopy
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from openctopus_server.errors.codes import ErrorCode
from openctopus_server.tools.base import Tool, ToolContext, ToolResult

READ_FILE_MAX_OUTPUT_CHARS = 128_000

WORKSPACE_FILE_TOOL_TIMEOUT_SECONDS: dict[str, float] = {
    "read_file": 30,
    "write_file": 30,
    "edit_file": 30,
    "apply_patch": 30,
    "delete_file": 10,
    "delete_folder": 60,
    "list_dir": 10,
    "find_files": 30,
    "grep": 60,
    "notebook_edit": 30,
}


def _input_schema(
    properties: dict[str, Any],
    *,
    required: tuple[str, ...] = (),
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = list(required)
    return schema


WORKSPACE_FILE_TOOL_SCHEMAS: dict[str, dict[str, Any]] = {
    "read_file": {
        "name": "read_file",
        "description": (
            "Read a file (text, image, or document). Text output format: "
            "LINE_NUM|CONTENT. Images return visual content for analysis. Supports PDF, "
            "DOCX, XLSX, PPTX documents. Use find_files/list_dir first when the path is "
            "uncertain. Read the relevant range before editing so replacements or patches "
            "are based on current content. Use offset and limit for large text files. Use "
            "force=true to re-read content even if unchanged. Reads exceeding ~128K chars "
            "are truncated."
        ),
        "input_schema": _input_schema(
            {
                "path": {"type": "string", "description": "The file path to read"},
                "offset": {
                    "type": "integer",
                    "description": "Line number to start reading from (1-indexed, default 1)",
                    "minimum": 1,
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of lines to read (default 2000)",
                    "minimum": 1,
                },
                "pages": {
                    "type": "string",
                    "description": (
                        "Page range for PDF files, e.g. '1-5' (default: all, max 20 pages)"
                    ),
                },
                "force": {
                    "type": "boolean",
                    "description": (
                        "Bypass same-file read deduplication and return content again."
                    ),
                    "default": False,
                },
            },
            required=("path",),
        ),
    },
    "write_file": {
        "name": "write_file",
        "description": (
            "Create a new file or intentionally replace an entire file with the provided "
            "content. Overwrites existing files and creates parent directories as needed. "
            "For code changes or partial edits, prefer apply_patch; use edit_file only for "
            "small exact replacements."
        ),
        "input_schema": _input_schema(
            {
                "path": {"type": "string", "description": "The file path to write to"},
                "content": {"type": "string", "description": "The content to write"},
            },
            required=("path", "content"),
        ),
    },
    "edit_file": {
        "name": "edit_file",
        "description": (
            "Perform a small, exact replacement in one file by replacing old_text with "
            "new_text. Use this for narrow text substitutions with old_text copied from "
            "read_file. For multi-file, structural, or generated code edits, prefer "
            "apply_patch. If old_text matches multiple times, provide more context or set "
            "occurrence, line_hint, replace_all, and expected_replacements. Shows "
            "closest-match diagnostics on failure."
        ),
        "input_schema": _input_schema(
            {
                "path": {"type": "string", "description": "The file path to edit"},
                "old_text": {"type": "string", "description": "The text to find and replace"},
                "new_text": {"type": "string", "description": "The text to replace with"},
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace all occurrences (default false)",
                },
                "occurrence": {
                    "type": ["integer", "null"],
                    "description": (
                        "Optional 1-based occurrence to replace when old_text appears "
                        "multiple times."
                    ),
                    "minimum": 1,
                },
                "line_hint": {
                    "type": ["integer", "null"],
                    "description": ("Optional 1-based line hint used to choose the nearest match."),
                    "minimum": 1,
                },
                "expected_replacements": {
                    "type": ["integer", "null"],
                    "description": (
                        "Optional guard for the number of replacements that must be made."
                    ),
                    "minimum": 1,
                },
            },
            required=("path", "old_text", "new_text"),
        ),
    },
    "apply_patch": {
        "name": "apply_patch",
        "description": (
            "Default tool for code edits. Supports multi-file changes in a single call. "
            "Provide a list of structured edits, each specifying a file path, action "
            "(replace/add), and the exact text to change. Paths must be relative. Set "
            "dry_run=true to validate and preview without writing files. Use edit_file only "
            "for small exact replacements on a single file."
        ),
        "input_schema": _input_schema(
            {
                "edits": {
                    "type": "array",
                    "description": (
                        "List of edits to apply. Each edit specifies a file and the change to make."
                    ),
                    "minItems": 1,
                    "maxItems": 20,
                    "items": _input_schema(
                        {
                            "path": {
                                "type": "string",
                                "description": "Relative path to the file to edit.",
                            },
                            "action": {
                                "type": "string",
                                "enum": ["replace", "add"],
                                "description": "Operation type: replace or add.",
                            },
                            "old_text": {
                                "type": ["string", "null"],
                                "description": (
                                    "Exact text to search for in the file. Required for replace."
                                ),
                            },
                            "new_text": {
                                "type": ["string", "null"],
                                "description": (
                                    "Text to replace with or append. Required for replace and add."
                                ),
                            },
                        },
                        required=("path", "action"),
                    ),
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Validate and summarize the patch without writing files.",
                    "default": False,
                },
            },
            required=("edits",),
        ),
    },
    "delete_file": {
        "name": "delete_file",
        "description": "Delete a single file. Use delete_folder for directories.",
        "input_schema": _input_schema(
            {"path": {"type": "string", "description": "Absolute path to the file."}},
            required=("path",),
        ),
    },
    "delete_folder": {
        "name": "delete_folder",
        "description": (
            "Recursively delete a folder and all its contents. Always recursive — use "
            "delete_file for individual files."
        ),
        "input_schema": _input_schema(
            {"path": {"type": "string", "description": "Absolute path to the folder."}},
            required=("path",),
        ),
    },
    "list_dir": {
        "name": "list_dir",
        "description": (
            "List the contents of a directory. Set recursive=true to explore nested "
            "structure. Common noise directories (.git, node_modules, __pycache__, etc.) "
            "are auto-ignored."
        ),
        "input_schema": _input_schema(
            {
                "path": {"type": "string", "description": "The directory path to list"},
                "recursive": {
                    "type": "boolean",
                    "description": "Recursively list all files (default false)",
                },
                "max_entries": {
                    "type": "integer",
                    "description": "Maximum entries to return (default 200, max 1000)",
                    "minimum": 1,
                    "maximum": 1000,
                },
            },
            required=("path",),
        ),
    },
    "find_files": {
        "name": "find_files",
        "description": (
            "Find files by path fragment, glob, or file type. Use this before read_file "
            "when you need to locate files, and prefer it over shell find/ls for ordinary "
            "workspace discovery. Returns workspace-relative paths and skips common "
            "dependency/build directories."
        ),
        "input_schema": _input_schema(
            {
                "path": {
                    "type": "string",
                    "description": "Directory or file to search in (default '.')",
                },
                "query": {
                    "type": "string",
                    "description": (
                        "Optional case-insensitive path fragment search. Whitespace-separated "
                        "terms must all be present."
                    ),
                },
                "glob": {
                    "type": "string",
                    "description": "Optional file filter, e.g. '*.py' or 'tests/**/test_*.py'",
                },
                "type": {
                    "type": "string",
                    "description": ("Optional file type shorthand, e.g. 'py', 'ts', 'md', 'json'"),
                },
                "include_dirs": {
                    "type": "boolean",
                    "description": "Include matching directories as well as files (default false)",
                },
                "sort": {
                    "type": "string",
                    "enum": ["path", "modified"],
                    "description": "Sort by path or most recently modified first (default path)",
                },
                "head_limit": {
                    "type": "integer",
                    "description": (
                        "Maximum number of paths to return (default 200, 0 for all, max 1000)"
                    ),
                    "minimum": 0,
                    "maximum": 1000,
                },
                "offset": {
                    "type": "integer",
                    "description": "Skip the first N matching entries before returning results",
                    "minimum": 0,
                    "maximum": 100000,
                },
            }
        ),
    },
    "grep": {
        "name": "grep",
        "description": (
            "Search file contents with a regex pattern. Default output_mode is "
            "files_with_matches (file paths only); use content mode for matching lines with "
            "context. Skips binary and files >2 MB. Supports glob/type filtering."
        ),
        "input_schema": _input_schema(
            {
                "pattern": {
                    "type": "string",
                    "description": "Regex or plain text pattern to search for",
                    "minLength": 1,
                },
                "path": {
                    "type": "string",
                    "description": "File or directory to search in (default '.')",
                },
                "glob": {
                    "type": "string",
                    "description": "Optional file filter, e.g. '*.py' or 'tests/**/test_*.py'",
                },
                "type": {
                    "type": "string",
                    "description": ("Optional file type shorthand, e.g. 'py', 'ts', 'md', 'json'"),
                },
                "case_insensitive": {
                    "type": "boolean",
                    "description": "Case-insensitive search (default false)",
                },
                "fixed_strings": {
                    "type": "boolean",
                    "description": "Treat pattern as plain text instead of regex (default false)",
                },
                "output_mode": {
                    "type": "string",
                    "enum": ["content", "files_with_matches", "count"],
                    "description": (
                        "content: matching lines with optional context; files_with_matches: "
                        "only matching file paths; count: matching line counts per file. "
                        "Default: files_with_matches"
                    ),
                },
                "context_before": {
                    "type": "integer",
                    "description": "Number of lines of context before each match",
                    "minimum": 0,
                    "maximum": 20,
                },
                "context_after": {
                    "type": "integer",
                    "description": "Number of lines of context after each match",
                    "minimum": 0,
                    "maximum": 20,
                },
                "max_matches": {
                    "type": "integer",
                    "description": "Legacy alias for head_limit in content mode",
                    "minimum": 1,
                    "maximum": 1000,
                },
                "max_results": {
                    "type": "integer",
                    "description": (
                        "Legacy alias for head_limit in files_with_matches or count mode"
                    ),
                    "minimum": 1,
                    "maximum": 1000,
                },
                "head_limit": {
                    "type": "integer",
                    "description": (
                        "Maximum number of results to return. In content mode this limits "
                        "matching line blocks; in other modes it limits file entries. Default 250"
                    ),
                    "minimum": 0,
                    "maximum": 1000,
                },
                "offset": {
                    "type": "integer",
                    "description": "Skip the first N results before applying head_limit",
                    "minimum": 0,
                    "maximum": 100000,
                },
            },
            required=("pattern",),
        ),
    },
    "notebook_edit": {
        "name": "notebook_edit",
        "description": (
            "Edit a Jupyter notebook (.ipynb) cell. Modes: replace (default) replaces cell "
            "content, insert adds a new cell after the target index, delete removes the cell "
            "at the index. cell_index is 0-based."
        ),
        "input_schema": _input_schema(
            {
                "path": {
                    "type": "string",
                    "description": "Path to the .ipynb notebook file",
                },
                "cell_index": {
                    "type": "integer",
                    "description": "0-based index of the cell to edit",
                    "minimum": 0,
                },
                "new_source": {
                    "type": "string",
                    "description": "New source content for the cell",
                },
                "cell_type": {
                    "type": "string",
                    "description": "Cell type: 'code' or 'markdown' (default: code)",
                    "enum": ["code", "markdown"],
                },
                "edit_mode": {
                    "type": "string",
                    "description": (
                        "Mode: 'replace' (default), 'insert' (after target), or 'delete'"
                    ),
                    "enum": ["replace", "insert", "delete"],
                },
            },
            required=("path", "cell_index"),
        ),
    },
}


class WorkspaceToolBackend(Protocol):
    async def __call__(
        self,
        name: str,
        args: dict[str, Any],
        ctx: ToolContext,
    ) -> ToolResult: ...


class _Args(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _ReadFileArgs(_Args):
    path: str
    offset: int = Field(1, ge=1)
    limit: int = Field(2000, ge=1)
    pages: str | None = None
    force: bool = False


class _WriteFileArgs(_Args):
    path: str
    content: str


class _EditFileArgs(_Args):
    path: str
    old_text: str
    new_text: str
    replace_all: bool = False
    occurrence: int | None = Field(None, ge=1)
    line_hint: int | None = Field(None, ge=1)
    expected_replacements: int | None = Field(None, ge=1)

    @model_validator(mode="after")
    def validate_selectors(self) -> "_EditFileArgs":
        selectors = sum(value is not None for value in (self.occurrence, self.line_hint))
        if selectors > 1:
            raise ValueError("occurrence and line_hint are mutually exclusive")
        if self.replace_all and selectors:
            raise ValueError("occurrence and line_hint cannot be used with replace_all")
        return self


class _PatchEdit(_Args):
    path: str
    action: Literal["replace", "add"]
    old_text: str | None = None
    new_text: str | None = None

    @model_validator(mode="after")
    def validate_action_fields(self) -> "_PatchEdit":
        if self.action == "replace" and (self.old_text is None or self.new_text is None):
            raise ValueError("replace requires old_text and new_text")
        if self.action == "add" and self.new_text is None:
            raise ValueError("add requires new_text")
        return self


class _ApplyPatchArgs(_Args):
    edits: list[_PatchEdit] = Field(min_length=1, max_length=20)
    dry_run: bool = False


class _PathArgs(_Args):
    path: str


class _ListDirArgs(_Args):
    path: str
    recursive: bool = False
    max_entries: int = Field(200, ge=1, le=1000)


class _FindFilesArgs(_Args):
    path: str = "."
    query: str | None = None
    glob: str | None = None
    type: str | None = None
    include_dirs: bool = False
    sort: Literal["path", "modified"] = "path"
    head_limit: int = Field(200, ge=0, le=1000)
    offset: int = Field(0, ge=0, le=100000)


class _GrepArgs(_Args):
    pattern: str = Field(min_length=1)
    path: str = "."
    glob: str | None = None
    type: str | None = None
    case_insensitive: bool = False
    fixed_strings: bool = False
    output_mode: Literal["content", "files_with_matches", "count"] = "files_with_matches"
    context_before: int = Field(0, ge=0, le=20)
    context_after: int = Field(0, ge=0, le=20)
    max_matches: int | None = Field(None, ge=1, le=1000)
    max_results: int | None = Field(None, ge=1, le=1000)
    head_limit: int = Field(250, ge=0, le=1000)
    offset: int = Field(0, ge=0, le=100000)


class _NotebookEditArgs(_Args):
    path: str
    cell_index: int = Field(ge=0)
    new_source: str | None = None
    cell_type: Literal["code", "markdown"] = "code"
    edit_mode: Literal["replace", "insert", "delete"] = "replace"

    @model_validator(mode="after")
    def validate_source(self) -> "_NotebookEditArgs":
        if not self.path.lower().endswith(".ipynb"):
            raise ValueError("notebook_edit requires an .ipynb path")
        if self.edit_mode != "delete" and self.new_source is None:
            raise ValueError(f"{self.edit_mode} requires new_source")
        return self


type _ArgsModel = type[_Args]

_ARG_MODELS: dict[str, _ArgsModel] = {
    "read_file": _ReadFileArgs,
    "write_file": _WriteFileArgs,
    "edit_file": _EditFileArgs,
    "apply_patch": _ApplyPatchArgs,
    "delete_file": _PathArgs,
    "delete_folder": _PathArgs,
    "list_dir": _ListDirArgs,
    "find_files": _FindFilesArgs,
    "grep": _GrepArgs,
    "notebook_edit": _NotebookEditArgs,
}


class WorkspaceFileTool(Tool):
    def __init__(self, name: str, backend: WorkspaceToolBackend) -> None:
        self._name = name
        self._backend = backend

    def name(self) -> str:
        return self._name

    def schema(self) -> dict[str, Any]:
        return deepcopy(WORKSPACE_FILE_TOOL_SCHEMAS[self._name])

    def max_output_chars(self) -> int:
        if self._name == "read_file":
            return READ_FILE_MAX_OUTPUT_CHARS
        return super().max_output_chars()

    async def execute(self, args: dict[str, Any], ctx: ToolContext) -> ToolResult:
        try:
            parsed = _ARG_MODELS[self._name].model_validate(args)
        except ValidationError as exc:
            return ToolResult(
                content=f"[{ErrorCode.TOOL_INVALID_ARGS.value}] Invalid {self._name} arguments: {exc}",
                is_error=True,
                code=ErrorCode.TOOL_INVALID_ARGS,
            )
        timeout_seconds = WORKSPACE_FILE_TOOL_TIMEOUT_SECONDS[self._name]
        try:
            async with asyncio.timeout(timeout_seconds):
                return await self._backend(self._name, parsed.model_dump(), ctx)
        except TimeoutError:
            uncertainty = (
                " The operation may have committed; inspect the workspace before retrying."
                if self._name
                in {
                    "write_file",
                    "edit_file",
                    "apply_patch",
                    "delete_file",
                    "delete_folder",
                    "notebook_edit",
                }
                else ""
            )
            return ToolResult(
                content=(
                    f"[{ErrorCode.TOOL_EXEC_TIMEOUT.value}] {self._name} timed out "
                    f"after {timeout_seconds:g} seconds.{uncertainty}"
                ),
                is_error=True,
                code=ErrorCode.TOOL_EXEC_TIMEOUT,
            )


def build_workspace_file_tools(
    backend: WorkspaceToolBackend,
) -> tuple[WorkspaceFileTool, ...]:
    return tuple(WorkspaceFileTool(name, backend) for name in WORKSPACE_FILE_TOOL_SCHEMAS)
