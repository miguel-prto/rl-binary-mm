"""Normalise recorded Polymarket raw files into Parquet.

    python -m rl_binary_mm.store.normalize                    # every pending file
    python -m rl_binary_mm.store.normalize --date 2026-11-13  # one day
    python -m rl_binary_mm.store.normalize --force --jobs 4   # redo, 4 processes

The data directory is ``--data-dir``, else ``RL_DATA_DIR``, else ``./data``.
Only finished raw files are processed (see pipeline.py). Exit code 1 if any
file failed. See ``pipeline.py`` for the layout.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from rl_binary_mm.store.normalize.pipeline import run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rl_binary_mm.store.normalize", description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=Path(os.environ.get("RL_DATA_DIR", "data"))
    )
    parser.add_argument("--date", help="only this UTC date (YYYY-MM-DD)")
    parser.add_argument("--force", action="store_true", help="redo files already normalised")
    parser.add_argument("--jobs", type=int, default=1, help="worker processes (default 1)")
    parser.add_argument(
        "--min-age-min", type=float, default=10, help="skip raw files modified more recently"
    )
    parser.add_argument(
        "--hour-grace-min", type=float, default=30, help="wait this long after the hour ends"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    start = time.perf_counter()
    result = run(
        args.data_dir,
        date=args.date,
        force=args.force,
        jobs=args.jobs,
        min_age_s=args.min_age_min * 60,
        hour_grace_s=args.hour_grace_min * 60,
    )
    logging.info(
        "done: %d normalised, %d skipped, %d failed in %.0f s",
        len(result.done),
        result.skipped,
        len(result.failed),
        time.perf_counter() - start,
    )
    return 1 if result.failed else 0


if __name__ == "__main__":
    sys.exit(main())
