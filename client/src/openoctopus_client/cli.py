from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from openoctopus_client import __version__
from openoctopus_client.config import ConfigurationError, load_config
from openoctopus_client.connection import ClientRuntime
from openoctopus_client.document_convert import (
    ConversionError,
    conversion_worker_main,
    convert_path,
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="openoctopus-client")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("version")
    commands.add_parser("run")
    spike = commands.add_parser("_spike-convert", help=argparse.SUPPRESS)
    spike.add_argument("path", type=Path)
    spike.add_argument("--pages")
    commands.add_parser("_conversion-worker", help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.command == "version":
        print(__version__)
        return 0
    if arguments.command == "_conversion-worker":
        return conversion_worker_main()
    if arguments.command in {None, "run"}:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
        try:
            config = load_config()
        except ConfigurationError as exc:
            print(f"configuration error: {exc}", file=sys.stderr)
            return 78
        try:
            return asyncio.run(ClientRuntime(config).run())
        except KeyboardInterrupt:
            return 0
    try:
        text = convert_path(arguments.path, pages=arguments.pages)
    except ConversionError as exc:
        print(json.dumps({"code": exc.code, "message": exc.message, "ok": False}, sort_keys=True))
        return 1
    except Exception:
        print(
            json.dumps(
                {
                    "code": "tool_content_conversion_failed",
                    "message": "Document conversion failed",
                    "ok": False,
                },
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps({"ok": True, "text": text}, ensure_ascii=False, sort_keys=True))
    return 0
