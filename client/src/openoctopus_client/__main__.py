from __future__ import annotations

if __name__ == "__main__":
    import multiprocessing

    multiprocessing.freeze_support()

    from openoctopus_client.cli import main

    raise SystemExit(main())
