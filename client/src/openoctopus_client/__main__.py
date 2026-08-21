from __future__ import annotations

import sys

if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()

    if len(sys.argv) == 4 and sys.argv[1] == "_pty-worker":
        from openoctopus_client.pty_worker import run

        raise SystemExit(run(int(sys.argv[2]), int(sys.argv[3])))

    from openoctopus_client.cli import main

    raise SystemExit(main())
