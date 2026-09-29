from __future__ import annotations

import asyncio
import errno
import json
from pathlib import Path
from typing import Any

from openoctopus_client.tools.common import ToolFailure, ToolOutput, fail
from openoctopus_client.tools.locks import PathLocks
from openoctopus_client.tools.workspace_rest import (
    INTERNAL_WORKSPACE_ACTION,
)
from openoctopus_client.transfer_admission import (
    LocalTransferAdmission,
    LocalTransferDrainRegistry,
)

from .common import (
    _contains_nul,
)
from .file_tools import FileTools
from .web_fetch import web_fetch

_TOOL_ARGUMENTS: dict[str, frozenset[str]] = {
    "read_file": frozenset({"path", "offset", "limit", "pages"}),
    "write_file": frozenset({"path", "content"}),
    "edit_file": frozenset(
        {
            "path",
            "old_text",
            "new_text",
            "replace_all",
            "occurrence",
            "line_hint",
            "expected_replacements",
        }
    ),
    "apply_patch": frozenset({"edits", "dry_run"}),
    "delete_file": frozenset({"path"}),
    "delete_folder": frozenset({"path"}),
    "list_dir": frozenset({"path", "recursive", "max_entries"}),
    "find_files": frozenset(
        {"path", "query", "glob", "type", "include_dirs", "sort", "head_limit", "offset"}
    ),
    "grep": frozenset(
        {
            "pattern",
            "path",
            "glob",
            "type",
            "case_insensitive",
            "fixed_strings",
            "output_mode",
            "context_before",
            "context_after",
            "max_matches",
            "max_results",
            "head_limit",
            "offset",
        }
    ),
    "notebook_edit": frozenset({"path", "cell_index", "new_source", "cell_type", "edit_mode"}),
    "web_fetch": frozenset({"url", "extractMode", "maxChars"}),
}

_PATCH_ARGUMENTS = frozenset({"path", "action", "old_text", "new_text"})


class ClientToolDispatcher:
    """Validate, route, and bound calls to the client tool implementations."""

    def __init__(
        self,
        workspace: Path,
        *,
        restrict_to_workspace: bool,
        ssrf_denylist: list[str],
        path_locks: PathLocks | None = None,
        transfer_admission: LocalTransferAdmission | None = None,
        transfer_drains: LocalTransferDrainRegistry | None = None,
    ) -> None:
        self._files = FileTools(
            workspace,
            restrict_to_workspace=restrict_to_workspace,
            path_locks=path_locks,
            transfer_admission=transfer_admission,
            transfer_drains=transfer_drains,
        )
        self._denylist = tuple(ssrf_denylist)

    def has_pending_blocking(self) -> bool:
        return self._files.work.has_pending()

    async def wait_for_pending_blocking(self) -> None:
        await self._files.work.wait()

    async def execute(self, name: str, args: dict[str, Any]) -> ToolOutput:
        try:
            self._validate_args(name, args)
            operation = (
                web_fetch(args, self._denylist)
                if name == "web_fetch"
                else self._files.execute(name, args)
            )
            return await asyncio.wait_for(operation, timeout=_timeout_for(name))
        except TimeoutError:
            code = "network_timeout" if name == "web_fetch" else "tool_exec_timeout"
            message = "web_fetch timed out" if name == "web_fetch" else f"{name} timed out"
            return fail(code, message)
        except ToolFailure as exc:
            return fail(exc.code, exc.message)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EPERM}:
                return fail("workspace_permission_denied", "Workspace path is unavailable")
            return fail(
                "workspace_storage_unavailable",
                "Workspace filesystem operation failed",
            )
        except (ValueError, TypeError, json.JSONDecodeError):
            return fail("tool_invalid_args", "Tool arguments are invalid")

    @staticmethod
    def _validate_args(name: str, args: dict[str, Any]) -> None:
        if _contains_nul(args):
            raise ToolFailure("tool_invalid_args", "Tool arguments must not contain NUL")
        allowed = _TOOL_ARGUMENTS.get(name)
        if allowed is None:
            return
        unknown = set(args) - allowed
        if unknown:
            raise ToolFailure("tool_invalid_args", "Tool arguments contain unknown fields")
        if name == "apply_patch":
            edits = args.get("edits")
            if isinstance(edits, list):
                for edit in edits:
                    if not isinstance(edit, dict) or set(edit) - _PATCH_ARGUMENTS:
                        raise ToolFailure("tool_invalid_args", "Patch edit contains unknown fields")


def _timeout_for(name: str) -> float:
    return {
        "delete_file": 10.0,
        "delete_folder": 60.0,
        "web_fetch": 30.0,
        INTERNAL_WORKSPACE_ACTION: 60.0,
    }.get(name, 30.0)
