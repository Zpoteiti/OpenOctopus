"""Black-box conversion smoke test for a frozen OpenOctopus Client artifact."""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REQUIRED_FIXTURES = ("sample.pdf", "sample.docx", "sample.xlsx", "sample.pptx", "sample.html")
EXPECTED_TOKENS = {
    "sample.pdf": ("[PDF pages 1-1 of 1]", "中文段落", "名称", "章鱼"),
    "sample.docx": ("Workspace report 工作区报告", "中文段落", "名称", "章鱼", "|"),
    "sample.xlsx": ("Summary 汇总", "Details 明细", "章鱼", "就绪", "|"),
    "sample.pptx": ("Status 状态", "读取文件", "Presenter notes 演讲者备注", "|"),
    "sample.html": ("Workspace report 工作区报告", "English paragraph", "名称", "章鱼", "|"),
}
VERSION_RUNS = 5
COMMAND_TIMEOUT_SECONDS = 30.0
POLL_SECONDS = 0.02


class SmokeError(RuntimeError):
    pass


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


def _psutil() -> Any:
    try:
        import psutil  # type: ignore[import-untyped]
    except ImportError as exc:
        raise SmokeError(
            "psutil is required for frozen smoke metrics; install it in the CI test environment"
        ) from exc
    return psutil


def _aggregate_rss(process: Any, psutil: Any) -> int:
    if process is None:
        return 0
    try:
        members = [process, *process.children(recursive=True)]
    except (psutil.Error, OSError):
        return 0
    total = 0
    for member in members:
        try:
            total += int(member.memory_info().rss)
        except (psutil.Error, OSError):
            continue
    return total


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


def _bundle_size(binary: Path) -> int:
    return sum(path.stat().st_size for path in binary.parent.rglob("*") if path.is_file())


def _run(
    binary: Path, *arguments: str, psutil: Any
) -> tuple[subprocess.CompletedProcess[str], float, int]:
    started = time.monotonic()
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                [str(binary), *arguments],
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
            )
        except OSError as exc:
            raise SmokeError(f"artifact could not start: {exc}") from exc
        try:
            monitored = psutil.Process(process.pid)
        except psutil.Error:
            monitored = None
        peak_rss = 0
        try:
            while process.poll() is None:
                peak_rss = max(peak_rss, _aggregate_rss(monitored, psutil))
                if time.monotonic() - started > COMMAND_TIMEOUT_SECONDS:
                    _kill_tree(process, monitored, psutil)
                    raise SmokeError(f"artifact timed out: {' '.join(arguments)}")
                time.sleep(POLL_SECONDS)
            process.wait(timeout=1)
        except BaseException:
            if process.poll() is None:
                _kill_tree(process, monitored, psutil)
            raise
        peak_rss = max(peak_rss, _aggregate_rss(monitored, psutil))
        stdout_file.seek(0)
        stderr_file.seek(0)
        try:
            stdout = stdout_file.read().decode("utf-8")
            stderr = stderr_file.read().decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SmokeError("artifact output was not valid UTF-8") from exc
    return (
        subprocess.CompletedProcess([str(binary), *arguments], process.returncode, stdout, stderr),
        time.monotonic() - started,
        peak_rss,
    )


def _json_result(result: subprocess.CompletedProcess[str], description: str) -> dict[str, Any]:
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise SmokeError(f"{description} did not write a JSON result: {result.stdout!r}") from exc
    if not isinstance(payload, dict):
        raise SmokeError(f"{description} JSON result is not an object")
    return payload


def _convert(binary: Path, document: Path, psutil: Any) -> tuple[str, float, int]:
    result, elapsed, peak_rss = _run(binary, "_spike-convert", str(document), psutil=psutil)
    payload = _json_result(result, document.name)
    if (
        result.returncode != 0
        or payload.get("ok") is not True
        or not isinstance(payload.get("text"), str)
        or result.stderr
    ):
        raise SmokeError(f"{document.name} failed: {payload!r}; stderr={result.stderr!r}")
    return payload["text"], elapsed, peak_rss


def _assert_corrupt_document(binary: Path, psutil: Any) -> tuple[float, int]:
    with tempfile.TemporaryDirectory() as temporary:
        document = Path(temporary) / "broken.docx"
        document.write_bytes(b"not a zip")
        result, elapsed, peak_rss = _run(binary, "_spike-convert", str(document), psutil=psutil)
    payload = _json_result(result, "corrupt docx")
    if (
        result.returncode == 0
        or payload.get("ok") is not False
        or payload.get("code") != "tool_content_conversion_failed"
        or payload.get("message") != "Document is not a valid OOXML file"
        or result.stderr
    ):
        raise SmokeError(f"corrupt docx did not return stable failure JSON: {payload!r}")
    return elapsed, peak_rss


def _assert_oversized_document(binary: Path, psutil: Any) -> tuple[float, int]:
    with tempfile.TemporaryDirectory() as temporary:
        document = Path(temporary) / "oversized.html"
        document.write_bytes(b"x" * (8 * 1024 * 1024 + 1))
        result, elapsed, peak_rss = _run(binary, "_spike-convert", str(document), psutil=psutil)
    payload = _json_result(result, "oversized html")
    if (
        result.returncode == 0
        or payload.get("ok") is not False
        or payload.get("code") != "tool_content_conversion_failed"
        or payload.get("message") != "Document exceeds the 8 MiB input limit"
        or result.stderr
    ):
        raise SmokeError(f"oversized html did not return stable failure JSON: {payload!r}")
    return elapsed, peak_rss


def _assert_large_output(binary: Path, psutil: Any) -> tuple[float, int]:
    with tempfile.TemporaryDirectory() as temporary:
        document = Path(temporary) / "large-output.html"
        document.write_text(f"<p>{'A' * 200_000}</p>", encoding="utf-8")
        text, elapsed, peak_rss = _convert(binary, document, psutil)
    if len(text) != 128_000 or not text.endswith("\n[truncated]"):
        raise SmokeError("large HTML output was not capped at 128,000 characters")
    return elapsed, peak_rss


def main() -> int:
    try:
        binary = _required_path("OO_CLIENT_BIN", kind="frozen client binary")
        corpus = _required_path("OO_DOCUMENT_CORPUS", kind="document corpus")
        psutil = _psutil()
        missing = [name for name in REQUIRED_FIXTURES if not (corpus / name).is_file()]
        if missing:
            raise SmokeError(
                f"OO_DOCUMENT_CORPUS is missing required fixtures: {', '.join(missing)}"
            )

        version_durations: list[float] = []
        for _ in range(VERSION_RUNS):
            result, elapsed, _ = _run(binary, "version", psutil=psutil)
            if (
                result.returncode != 0
                or result.stdout not in {"0.0.1\n", "0.0.1\r\n"}
                or result.stderr
            ):
                raise SmokeError(
                    f"version failed: stdout={result.stdout!r}; stderr={result.stderr!r}"
                )
            version_durations.append(elapsed)

        conversions: dict[str, dict[str, float | int]] = {}
        for name in REQUIRED_FIXTURES:
            text, elapsed, peak_rss = _convert(binary, corpus / name, psutil)
            absent = [token for token in EXPECTED_TOKENS[name] if token not in text]
            if absent:
                raise SmokeError(f"{name} lost semantic content: {absent!r}")
            conversions[name] = {
                "seconds": round(elapsed, 6),
                "sampled_process_tree_peak_rss_bytes": peak_rss,
            }

        corrupt_seconds, corrupt_peak_rss = _assert_corrupt_document(binary, psutil)
        oversized_seconds, oversized_peak_rss = _assert_oversized_document(binary, psutil)
        large_output_seconds, large_output_peak_rss = _assert_large_output(binary, psutil)
        print(
            json.dumps(
                {
                    "bundle_bytes": _bundle_size(binary),
                    "conversion": conversions,
                    "corrupt_docx": {
                        "seconds": round(corrupt_seconds, 6),
                        "sampled_process_tree_peak_rss_bytes": corrupt_peak_rss,
                    },
                    "oversized_html": {
                        "seconds": round(oversized_seconds, 6),
                        "sampled_process_tree_peak_rss_bytes": oversized_peak_rss,
                    },
                    "large_output_html": {
                        "seconds": round(large_output_seconds, 6),
                        "sampled_process_tree_peak_rss_bytes": large_output_peak_rss,
                    },
                    "version_startup_median_seconds": round(
                        statistics.median(version_durations), 6
                    ),
                },
                sort_keys=True,
            )
        )
    except SmokeError as exc:
        print(f"frozen smoke failed: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
