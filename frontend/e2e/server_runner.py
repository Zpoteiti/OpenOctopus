from pathlib import Path

import uvicorn

from openctopus_server.main import create_app


if __name__ == "__main__":
    frontend_dist = Path(__file__).resolve().parents[1] / "dist"
    uvicorn.run(create_app(frontend_dir=frontend_dist), host="127.0.0.1", port=8080)
