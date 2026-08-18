from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="native Windows contract")


def test_cli_ctrl_break_gracefully_shuts_down() -> None:
    async def run() -> None:
        connected = asyncio.Event()
        disconnected = asyncio.Event()

        async def accept_client(
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
        ) -> None:
            connected.set()
            try:
                with contextlib.suppress(OSError):
                    await reader.read()
            finally:
                writer.close()
                with contextlib.suppress(ConnectionError):
                    await writer.wait_closed()
                disconnected.set()

        server = await asyncio.start_server(accept_client, "127.0.0.1", 0)
        assert server.sockets
        port = server.sockets[0].getsockname()[1]
        environment = os.environ.copy()
        for key in (
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "WS_PROXY",
            "WSS_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "ws_proxy",
            "wss_proxy",
        ):
            environment.pop(key, None)
        environment["NO_PROXY"] = "127.0.0.1,localhost"
        environment["no_proxy"] = "127.0.0.1,localhost"
        environment["OPENOCTOPUS_SERVER_URL"] = f"http://127.0.0.1:{port}"
        environment["OPENOCTOPUS_DEVICE_TOKEN"] = "openoctopus_dev_native_shutdown"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "openoctopus_client",
            "run",
            env=environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        )
        try:
            await asyncio.wait_for(connected.wait(), timeout=10)
            process.send_signal(signal.CTRL_BREAK_EVENT)
            await asyncio.wait_for(process.communicate(), timeout=8)
            assert process.returncode == 0
            await asyncio.wait_for(disconnected.wait(), timeout=3)
        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.communicate(), timeout=3)
            server.close()
            await server.wait_closed()

    asyncio.run(asyncio.wait_for(run(), timeout=25))
