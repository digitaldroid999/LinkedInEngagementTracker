"""Per-run scrape logging: scrape_YYYYMMDD_HHMMSS.log next to the EXE (frozen) or project root (dev)."""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

LOGGER_NAME = "linkedin_engagement"

_current_log_path: Path | None = None


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def log_directory() -> Path:
    """Directory for scrape log files: folder containing the .exe when frozen, else project root."""
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return project_root()


def begin_scrape_session() -> Path:
    """Create a timestamped log file and attach it to the app scrape logger (once per process)."""
    global _current_log_path
    if _current_log_path is not None:
        return _current_log_path

    root_dir = log_directory()
    path = root_dir / f"scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    root = logging.getLogger(LOGGER_NAME)
    root.setLevel(logging.DEBUG)
    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()

    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    root.addHandler(fh)
    root.propagate = False

    _current_log_path = path
    return path
