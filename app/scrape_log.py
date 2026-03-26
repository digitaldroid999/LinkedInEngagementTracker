"""Per-run scrape logging next to main.py: scrape_YYYYMMDD_HHMMSS.log."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

LOGGER_NAME = "linkedin_engagement"


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def begin_scrape_session() -> Path:
    """Create a timestamped log file beside main.py and attach it to the app scrape logger."""
    root_dir = project_root()
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

    return path
