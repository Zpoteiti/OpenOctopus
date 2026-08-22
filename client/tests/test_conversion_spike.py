from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import pytest

from openoctopus_client import cli, document_convert
from openoctopus_client.document_convert import ConversionError

CLIENT_ROOT = Path(__file__).parents[1]
FIXTURES = CLIENT_ROOT.parent / "server" / "tests" / "fixtures" / "documents"
MKFIFO = getattr(os, "mkfifo", None)


def _client(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "openoctopus_client", *arguments],
        check=False,
        capture_output=True,
        encoding="utf-8",
        env={**os.environ, "PYTHONPATH": str(CLIENT_ROOT / "src")},
        timeout=30,
    )


def test_version_is_stable() -> None:
    result = _client("version")
    assert result.returncode == 0
    assert result.stdout == "0.0.1\n"
    assert result.stderr == ""


@pytest.mark.parametrize(
    "filename", ["sample.pdf", "sample.docx", "sample.xlsx", "sample.pptx", "sample.html"]
)
def test_spike_convert_returns_json(filename: str) -> None:
    result = _client("_spike-convert", str(FIXTURES / filename))
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert payload["text"]
    assert set(payload) == {"ok", "text"}


def test_spike_convert_failure_is_stable_json() -> None:
    result = _client("_spike-convert", str(FIXTURES / "missing.pdf"))
    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "code": "tool_content_conversion_failed",
        "message": "Document does not exist",
        "ok": False,
    }
    assert result.stderr == ""


def test_spike_convert_rejects_more_than_eight_mebibytes(tmp_path: Path) -> None:
    document = tmp_path / "large.html"
    document.write_bytes(b"x" * (8 * 1024 * 1024 + 1))
    result = _client("_spike-convert", str(document))
    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "code": "tool_content_conversion_failed",
        "message": "Document exceeds the 8 MiB input limit",
        "ok": False,
    }


def test_spike_convert_caps_large_output(tmp_path: Path) -> None:
    document = tmp_path / "large-output.html"
    document.write_text(f"<p>{'A' * 200_000}</p>", encoding="utf-8")

    result = _client("_spike-convert", str(document))

    assert result.returncode == 0
    text = json.loads(result.stdout)["text"]
    assert len(text) == 128_000
    assert text.endswith("\n[truncated]")


def test_html_bytes_are_converted_without_a_local_staging_file() -> None:
    text = document_convert.convert_html_bytes(
        "<h1>内存文档</h1><table><tr><td>alpha</td></tr></table>".encode()
    )

    assert "内存文档" in text
    assert "alpha" in text


@pytest.mark.skipif(MKFIFO is None, reason="named pipes are POSIX-only")
def test_spike_convert_rejects_a_fifo_without_blocking(tmp_path: Path) -> None:
    document = tmp_path / "pipe.html"
    assert MKFIFO is not None
    MKFIFO(document)

    result = _client("_spike-convert", str(document))

    assert result.returncode == 1
    assert json.loads(result.stdout) == {
        "code": "tool_content_conversion_failed",
        "message": "Document is not a regular file",
        "ok": False,
    }


def test_conversion_worker_rejects_a_replaced_document(tmp_path: Path) -> None:
    document = tmp_path / "document.html"
    document.write_text("original", encoding="utf-8")
    identity = document_convert._document_identity(document)
    replacement = tmp_path / "replacement.html"
    replacement.write_text("replacement", encoding="utf-8")
    os.replace(replacement, document)

    with pytest.raises(ConversionError, match="changed"):
        document_convert._read_limited(document, expected_identity=identity)


def test_spike_convert_rejects_windows_drive_ooxml_member() -> None:
    document = BytesIO()
    with ZipFile(document, "w", ZIP_DEFLATED) as archive:
        archive.writestr("C:\\escape.xml", "unsafe")

    with pytest.raises(ConversionError, match="safety validation"):
        document_convert._preflight_ooxml(document.getvalue())


def test_pdf_pages_argument() -> None:
    result = _client("_spike-convert", str(FIXTURES / "sample.pdf"), "--pages", "1")

    assert result.returncode == 0
    assert json.loads(result.stdout)["text"].startswith("[PDF pages 1-1 of 1]")


@pytest.mark.parametrize(
    ("filename", "pages"),
    [("sample.pdf", "0"), ("sample.pdf", "1-21"), ("sample.html", "1")],
)
def test_invalid_pages_argument_returns_stable_json(filename: str, pages: str) -> None:
    result = _client("_spike-convert", str(FIXTURES / filename), "--pages", pages)

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["ok"] is False
    assert payload["code"] == "tool_invalid_args"


def test_cli_normalizes_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def fail(_path: Path, *, pages: str | None) -> str:
        raise OSError("synthetic failure")

    monkeypatch.setattr(cli, "convert_path", fail)
    monkeypatch.setattr(sys, "argv", ["openoctopus-client", "_spike-convert", "sample.pdf"])

    assert cli.main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "code": "tool_content_conversion_failed",
        "message": "Document conversion failed",
        "ok": False,
    }


def test_conversion_worker_environment_excludes_parent_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENOCTOPUS_DEVICE_TOKEN", "secret-device-token")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret-cloud-key")
    monkeypatch.setenv("HTTPS_PROXY", "http://secret-proxy.example")
    monkeypatch.setenv("LANG", "zh_CN.UTF-8")

    worker_environment = document_convert._minimal_worker_environment()

    assert worker_environment.get("LANG") == "zh_CN.UTF-8"
    assert "OPENOCTOPUS_DEVICE_TOKEN" not in worker_environment
    assert "AWS_SECRET_ACCESS_KEY" not in worker_environment
    assert "HTTPS_PROXY" not in worker_environment
    assert set(worker_environment) <= {
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SYSTEMROOT",
        "WINDIR",
    }


def test_conversion_timeout_reaps_the_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(document_convert, "TIMEOUT_SECONDS", 0.01)

    with pytest.raises(ConversionError, match="timed out") as raised:
        document_convert.convert_path(FIXTURES / "sample.pdf", pages=None)

    assert raised.value.code == "tool_exec_timeout"


def test_async_conversion_cancellation_kills_and_reaps_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> tuple[bool, bool]:
        started = asyncio.Event()
        reaped = asyncio.Event()

        class Process:
            returncode: int | None = None
            terminated = False
            killed = False
            waited = False

            async def communicate(self, data: bytes) -> tuple[bytes, bytes]:
                assert data
                started.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True
                self.returncode = -9
                reaped.set()

            async def wait(self) -> int:
                self.waited = True
                await reaped.wait()
                assert self.returncode is not None
                return self.returncode

        process = Process()

        async def create_process(*args: object, **kwargs: object) -> Process:
            assert args
            assert kwargs["stderr"] == asyncio.subprocess.DEVNULL
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
        conversion = asyncio.create_task(
            document_convert.convert_path_async(FIXTURES / "sample.pdf", pages=None)
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        conversion.cancel()
        with pytest.raises(asyncio.CancelledError):
            await conversion
        return process.terminated, process.killed and process.waited

    assert asyncio.run(exercise()) == (True, True)


def test_async_conversion_timeout_terminates_and_reaps_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> tuple[bool, bool, bool]:
        started = asyncio.Event()
        reaped = asyncio.Event()

        class Process:
            returncode: int | None = None
            terminated = False
            killed = False
            waited = False

            async def communicate(self, data: bytes) -> tuple[bytes, bytes]:
                assert data
                started.set()
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            def terminate(self) -> None:
                self.terminated = True
                self.returncode = -15
                reaped.set()

            def kill(self) -> None:
                self.killed = True
                self.returncode = -9
                reaped.set()

            async def wait(self) -> int:
                self.waited = True
                await reaped.wait()
                assert self.returncode is not None
                return self.returncode

        process = Process()

        async def create_process(*args: object, **kwargs: object) -> Process:
            assert args
            assert kwargs["stderr"] == asyncio.subprocess.DEVNULL
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", create_process)
        conversion = asyncio.create_task(
            document_convert.convert_path_async(FIXTURES / "sample.pdf", pages=None)
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        with pytest.raises(ConversionError) as raised:
            await conversion
        assert raised.value.code == "tool_exec_timeout"
        return process.terminated, process.killed, process.waited

    monkeypatch.setattr(document_convert, "TIMEOUT_SECONDS", 0.01)
    assert asyncio.run(exercise()) == (True, False, True)
