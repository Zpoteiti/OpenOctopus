"""Black-box smoke test for the frozen tray, core, and worker entry points."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypedDict

COMMAND_TIMEOUT_SECONDS = 30.0
POLL_SECONDS = 0.02
_WINPTY_NATIVE_FILES = frozenset(
    {
        "conpty.dll",
        "openconsole.exe",
        "winpty.dll",
        "winpty-agent.exe",
    }
)


class SmokeError(RuntimeError):
    pass


def _assert_winpty_native_bundle(binary: Path) -> None:
    names = {path.name.casefold() for path in binary.parent.rglob("*") if path.is_file()}
    missing = sorted(_WINPTY_NATIVE_FILES - names)
    if not any(name.startswith("_winpty.") and name.endswith(".pyd") for name in names):
        missing.append("_winpty.*.pyd")
    if missing:
        raise SmokeError("frozen bundle is missing pywinpty native files: " + ", ".join(missing))


@dataclass(frozen=True)
class _RunResult:
    completed: subprocess.CompletedProcess[str]
    seconds: float
    peak_rss_bytes: int
    peak_processes: int


def _psutil() -> Any:
    try:
        import psutil  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SmokeError("psutil is required for frozen runtime smoke") from exc
    return psutil


def _tree_metrics(process: Any, psutil: Any) -> tuple[int, int]:
    if process is None:
        return 0, 0
    try:
        members = [process, *process.children(recursive=True)]
    except (psutil.Error, OSError):
        return 0, 0
    rss = 0
    for member in members:
        try:
            rss += int(member.memory_info().rss)
        except (psutil.Error, OSError):
            continue
    return rss, len(members)


def _kill_tree(process: subprocess.Popen[bytes], monitored: Any, psutil: Any) -> None:
    if monitored is None:
        children = []
    else:
        try:
            children = monitored.children(recursive=True)
        except (psutil.Error, OSError):
            children = []
    for child in reversed(children):
        try:
            child.kill()
        except (psutil.Error, OSError):
            pass
    if process.poll() is None:
        process.kill()
    process.wait(timeout=5)


def _run(
    binary: Path,
    *arguments: str,
    env: Mapping[str, str] | None = None,
    psutil: Any,
) -> _RunResult:
    started = time.monotonic()
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                [str(binary), *arguments],
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
                env=dict(env) if env is not None else None,
            )
        except OSError as exc:
            raise SmokeError(f"artifact could not start: {exc}") from exc
        try:
            monitored = psutil.Process(process.pid)
        except psutil.Error:
            monitored = None
        peak_rss = 0
        peak_processes = 0
        try:
            while process.poll() is None:
                rss, count = _tree_metrics(monitored, psutil)
                peak_rss = max(peak_rss, rss)
                peak_processes = max(peak_processes, count)
                if time.monotonic() - started > COMMAND_TIMEOUT_SECONDS:
                    _kill_tree(process, monitored, psutil)
                    raise SmokeError(f"artifact timed out: {' '.join(arguments)}")
                time.sleep(POLL_SECONDS)
            process.wait(timeout=1)
        except BaseException:
            if process.poll() is None:
                _kill_tree(process, monitored, psutil)
            raise
        rss, count = _tree_metrics(monitored, psutil)
        peak_rss = max(peak_rss, rss)
        peak_processes = max(peak_processes, count)
        stdout_file.seek(0)
        stderr_file.seek(0)
        try:
            stdout = stdout_file.read().decode("utf-8")
            stderr = stderr_file.read().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SmokeError("artifact output was not valid UTF-8") from exc
    return _RunResult(
        completed=subprocess.CompletedProcess(
            [str(binary), *arguments], process.returncode, stdout, stderr
        ),
        seconds=time.monotonic() - started,
        peak_rss_bytes=peak_rss,
        peak_processes=peak_processes,
    )


class _PipeResult(TypedDict):
    return_code: int
    stdout: str
    stderr: str
    seconds: float


def _run_core_pipe(
    binary: Path,
    *,
    startup_lines: list[bytes],
    close_stdin_after: bool,
    psutil: Any,
) -> _PipeResult:
    started = time.monotonic()
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                [str(binary), "_core-run"],
                stdin=subprocess.PIPE,
                stdout=stdout_file,
                stderr=stderr_file,
            )
        except OSError as exc:
            raise SmokeError(f"artifact could not start: {exc}") from exc
        assert process.stdin is not None
        try:
            for line in startup_lines:
                process.stdin.write(line)
            process.stdin.flush()
            if close_stdin_after:
                process.stdin.close()
            # The core must emit its terminal exit event and exit promptly.
            return_code = process.wait(timeout=COMMAND_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise SmokeError("frozen core pipe did not terminate") from None
        except BrokenPipeError:
            return_code = process.wait(timeout=5)
        finally:
            if process.stdin is not None:
                try:
                    process.stdin.close()
                except (BrokenPipeError, OSError):
                    pass
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read().decode("utf-8", errors="replace")
        stderr = stderr_file.read().decode("utf-8", errors="replace")
    return {
        "return_code": return_code,
        "stdout": stdout,
        "stderr": stderr,
        "seconds": time.monotonic() - started,
    }


def _run_tray_single_instance_check(binary: Path, psutil: Any) -> dict[str, object]:
    """Start the frozen tray offscreen and verify the single-instance exit.

    The first process must stay resident; a second launch must wake the
    running program and exit with 0 without starting another core.
    """

    suffix = ".exe" if os.name == "nt" else ""
    binary = binary.with_name(f"openoctopus-client{suffix}")
    with tempfile.TemporaryDirectory() as temporary:
        environment = {
            **os.environ,
            "QT_QPA_PLATFORM": "offscreen",
            "HOME": temporary,
            "USERPROFILE": temporary,
            "APPDATA": str(Path(temporary) / "config"),
            "LOCALAPPDATA": str(Path(temporary) / "local"),
            "XDG_CONFIG_HOME": str(Path(temporary) / "config"),
            "XDG_CACHE_HOME": str(Path(temporary) / "cache"),
        }
        first = subprocess.Popen(
            [str(binary)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
        )
        second_return = -1
        second_seconds = 0.0
        try:
            time.sleep(3.0)
            if first.poll() is not None and first.returncode == 4:
                return {
                    "second_launch_seconds": 0.0,
                    "second_launch_return_code": 0,
                    "skipped": "tray-unavailable",
                }
            if first.poll() is not None:
                raise SmokeError(
                    f"tray exited early with {first.returncode}; the frozen GUI is broken"
                )
            second_started = time.monotonic()
            second = subprocess.Popen(
                [str(binary)],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=environment,
            )
            try:
                second_return = second.wait(timeout=15)
            except subprocess.TimeoutExpired:
                second.kill()
                second.wait(timeout=5)
                raise SmokeError(
                    "second tray launch did not exit; single-instance arbitration failed"
                ) from None
            second_seconds = time.monotonic() - second_started
            if first.poll() is not None:
                raise SmokeError("the resident tray died when the second launch arrived")
        finally:
            if first.poll() is None:
                first.terminate()
                try:
                    first.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    first.kill()
                    first.wait(timeout=5)
    if second_return != 0:
        raise SmokeError(
            f"second tray launch must exit 0 after waking the running program; got {second_return}"
        )
    return {
        "second_launch_seconds": round(second_seconds, 6),
        "second_launch_return_code": second_return,
    }


def _required_path(name: str, *, kind: str) -> Path:
    value = os.environ.get(name)
    if not value:
        raise SmokeError(f"{name} must name the {kind}")
    path = Path(value).expanduser().resolve()
    if kind == "frozen client binary" and (not path.is_file() or not os.access(path, os.X_OK)):
        raise SmokeError(f"{name} is not an executable file: {path}")
    if kind == "document corpus" and not path.is_dir():
        raise SmokeError(f"{name} is not a directory: {path}")
    return path


def _bundle_size(binary: Path) -> int:
    return sum(path.stat().st_size for path in binary.parent.rglob("*") if path.is_file())


def _runtime_smoke_payload(
    *,
    bundle: Path,
    version_seconds: float,
    version_peak_rss: int,
    version_peak_processes: int,
    core_pipe_seconds: float,
    exec_seconds: float,
    exec_peak_rss: int,
    exec_peak_processes: int,
    mcp_seconds: float,
    mcp_peak_rss: int,
    mcp_peak_processes: int,
    conversion_seconds: float,
    conversion_peak_rss: int,
    conversion_peak_processes: int,
    tray: dict[str, object],
) -> dict[str, object]:
    return {
        "bundle_bytes": _bundle_size(bundle),
        "version": {
            "seconds": round(version_seconds, 6),
            "sampled_process_tree_peak_processes": version_peak_processes,
            "sampled_process_tree_peak_rss_bytes": version_peak_rss,
        },
        "core_pipe": {
            "seconds": round(core_pipe_seconds, 6),
        },
        "exec_backends": {
            "seconds": round(exec_seconds, 6),
            "sampled_process_tree_peak_processes": exec_peak_processes,
            "sampled_process_tree_peak_rss_bytes": exec_peak_rss,
        },
        "mcp_stdio": {
            "seconds": round(mcp_seconds, 6),
            "sampled_process_tree_peak_processes": mcp_peak_processes,
            "sampled_process_tree_peak_rss_bytes": mcp_peak_rss,
        },
        "conversion_child": {
            "seconds": round(conversion_seconds, 6),
            "sampled_process_tree_peak_processes": conversion_peak_processes,
            "sampled_process_tree_peak_rss_bytes": conversion_peak_rss,
        },
        "tray_single_instance": tray,
    }


def main() -> int:
    try:
        binary = _required_path("OO_CLIENT_BIN", kind="frozen client binary")
        corpus = _required_path("OO_DOCUMENT_CORPUS", kind="document corpus")
        fixture = corpus / "sample.html"
        if not fixture.is_file():
            raise SmokeError(f"document corpus is missing {fixture.name}")
        if os.name == "nt":
            _assert_winpty_native_bundle(binary)
        psutil = _psutil()

        version = _run(binary, "_version", psutil=psutil)
        if (
            version.completed.returncode != 0
            or version.completed.stdout not in {"0.0.1\n", "0.0.1\r\n"}
        ):
            raise SmokeError(
                f"version failed: stdout={version.completed.stdout!r}; "
                f"stderr={version.completed.stderr!r}"
            )
        if version.completed.stderr:
            raise SmokeError(f"version wrote stderr: {version.completed.stderr!r}")

        # The private core pipe must exist, reject bad startup configs with a
        # structured exit event, and never echo the token.  The old
        # environment-variable ``run`` CLI contract is gone.
        pipe = _run_core_pipe(
            binary,
            startup_lines=[b'{"type":"startup-config","generation":1,'
            b'"server_url":"http://host/api","token":"openoctopus_dev_frozen_pipe_secret"}\n'],
            close_stdin_after=False,
            psutil=psutil,
        )
        pipe_stdout = pipe["stdout"]
        events = [json.loads(line) for line in pipe_stdout.splitlines() if line.strip()]
        if (
            pipe["return_code"] != 78
            or len(events) != 1
            or events[0].get("type") != "exit"
            or events[0].get("reason") != "startup_config_invalid"
            or "frozen_pipe_secret" in pipe_stdout
            or "frozen_pipe_secret" in pipe["stderr"]
        ):
            raise SmokeError(f"frozen core pipe rejected the config incorrectly: {pipe!r}")

        exec_backends = _run(binary, "_exec-backend-smoke", psutil=psutil)
        try:
            exec_payload = json.loads(exec_backends.completed.stdout)
        except json.JSONDecodeError as exc:
            raise SmokeError("exec backend smoke did not write JSON") from exc
        if (
            exec_backends.completed.returncode != 0
            or exec_backends.completed.stderr
            or not isinstance(exec_payload, dict)
            or exec_payload.get("ok") is not True
            or exec_payload.get("pipe") is not True
            or exec_payload.get("tty") is not True
            or "openoctopus-exec-smoke" in exec_backends.completed.stdout
        ):
            raise SmokeError("exec backend smoke failed")

        mcp_environment = dict(os.environ)
        mcp_environment["OPENOCTOPUS_DEVICE_TOKEN"] = "openoctopus_dev_mcp_smoke_secret"
        mcp_fixture = Path(__file__).parent / "fixtures" / "fake_mcp_stdio.py"
        mcp = _run(
            binary,
            "_mcp-stdio-smoke",
            sys.executable,
            str(mcp_fixture),
            env=mcp_environment,
            psutil=psutil,
        )
        try:
            mcp_payload = json.loads(mcp.completed.stdout)
        except json.JSONDecodeError as exc:
            raise SmokeError("MCP stdio smoke did not write JSON") from exc
        if (
            mcp.completed.returncode != 0
            or mcp.completed.stderr
            or mcp_payload != {"ok": True, "stdio_mcp": True}
            or "mcp_smoke_secret" in mcp.completed.stdout
        ):
            raise SmokeError("MCP stdio smoke failed")
        if mcp.peak_processes < 2:
            raise SmokeError("MCP stdio smoke did not start a child process")

        conversion = _run(binary, "_spike-convert", str(fixture), psutil=psutil)
        try:
            conversion_payload = json.loads(conversion.completed.stdout)
        except json.JSONDecodeError as exc:
            raise SmokeError(
                f"conversion child did not write JSON: {conversion.completed.stdout!r}"
            ) from exc
        if (
            conversion.completed.returncode != 0
            or conversion.completed.stderr
            or not isinstance(conversion_payload, dict)
            or conversion_payload.get("ok") is not True
            or not isinstance(conversion_payload.get("text"), str)
        ):
            raise SmokeError(
                f"conversion child failed: stdout={conversion.completed.stdout!r}; "
                f"stderr={conversion.completed.stderr!r}"
            )
        if conversion.peak_processes < 2:
            raise SmokeError(
                "conversion did not show a child process; frozen multiprocessing path was not used"
            )

        tray = _run_tray_single_instance_check(binary, psutil=psutil)

        print(
            json.dumps(
                _runtime_smoke_payload(
                    bundle=binary,
                    version_seconds=version.seconds,
                    version_peak_rss=version.peak_rss_bytes,
                    version_peak_processes=version.peak_processes,
                    core_pipe_seconds=pipe["seconds"],
                    exec_seconds=exec_backends.seconds,
                    exec_peak_rss=exec_backends.peak_rss_bytes,
                    exec_peak_processes=exec_backends.peak_processes,
                    mcp_seconds=mcp.seconds,
                    mcp_peak_rss=mcp.peak_rss_bytes,
                    mcp_peak_processes=mcp.peak_processes,
                    conversion_seconds=conversion.seconds,
                    conversion_peak_rss=conversion.peak_rss_bytes,
                    conversion_peak_processes=conversion.peak_processes,
                    tray=tray,
                ),
                sort_keys=True,
            )
        )
    except SmokeError as exc:
        print(f"frozen runtime smoke failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
