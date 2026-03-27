"""Build a single-file Windows executable with PyInstaller.

Run from the project root:  python build.py

Requires: icon.ico in the same directory as main.py (project root).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MAIN = ROOT / "main.py"
ICON = ROOT / "icon.ico"
APP_NAME = "LinkedInEngagementTracker"


def main() -> int:
    if not MAIN.is_file():
        print(f"ERROR: Missing entry script: {MAIN}", file=sys.stderr)
        return 1
    if not ICON.is_file():
        print(
            f"ERROR: Missing {ICON.name} — place it next to main.py (same folder as build.py).",
            file=sys.stderr,
        )
        return 1

    icon_str = str(ICON.resolve())
    # Bundle icon.ico into the onefile extract dir so app.ui _window_icon_path() finds it at sys._MEIPASS / "icon.ico"
    add_data = f"{icon_str};."

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--onefile",
        "--windowed",
        "--noconfirm",
        "--clean",
        "--name",
        APP_NAME,
        f"--icon={icon_str}",
        "--add-data",
        add_data,
        str(MAIN.resolve()),
    ]

    print("Running:", " ".join(cmd), flush=True)
    rc = subprocess.call(cmd, cwd=ROOT)
    if rc == 0:
        exe = ROOT / "dist" / f"{APP_NAME}.exe"
        print(f"Build OK: {exe}", flush=True)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
