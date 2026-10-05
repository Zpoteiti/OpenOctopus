from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="native Windows contract")


def test_core_run_ctrl_break_gracefully_shuts_down(tmp_path: object) -> None:
    async def run(workspace: str) -> None:
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
        # The core takes its configuration on stdin only; the environment
        # must never carry credentials for it.
        for key in ("OPENOCTOPUS_SERVER_URL", "OPENOCTOPUS_DEVICE_TOKEN"):
            environment.pop(key, None)
        startup_config = json.dumps(
            {
                "type": "startup-config",
                "generation": 1,
                "server_url": f"http://127.0.0.1:{port}",
                "token": "openoctopus_dev_native_shutdown",
                "workspace_root": workspace,
            }
        )
        creationflags = int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200))
        ctrl_break = int(getattr(signal, "CTRL_BREAK_EVENT"))
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "openoctopus_client",
            "_core-run",
            env=environment,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            creationflags=creationflags,
        )
        try:
            assert process.stdin is not None
            process.stdin.write((startup_config + "\n").encode("utf-8"))
            await process.stdin.drain()
            await asyncio.wait_for(connected.wait(), timeout=10)
            process.send_signal(ctrl_break)
            await asyncio.wait_for(process.communicate(), timeout=8)
            assert process.returncode == 0
            await asyncio.wait_for(disconnected.wait(), timeout=3)
        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.communicate(), timeout=3)
            server.close()
            await server.wait_closed()

    asyncio.run(asyncio.wait_for(run(str(tmp_path)), timeout=25))
