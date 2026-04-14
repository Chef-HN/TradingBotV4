"""
TradingBotV3 — Launcher
Starts the grid worker and API server as separate subprocesses.

Run:
    python -m scripts.run_bot
"""
from __future__ import annotations

import os
import subprocess
import sys


def main() -> None:
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    python = sys.executable
    src = os.path.join(base, "src")
    env = {**os.environ, "PYTHONPATH": src}

    worker = subprocess.Popen(
        [python, "-m", "scripts.run_worker"],
        cwd=base,
        env=env,
    )
    api = subprocess.Popen(
        [python, "-m", "scripts.run_api"],
        cwd=base,
        env=env,
    )

    try:
        worker.wait()
    except KeyboardInterrupt:
        worker.terminate()
        api.terminate()


if __name__ == "__main__":
    main()
