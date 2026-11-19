"""Deletion of raw ``pm_clob`` files once they are safely normalised.

Policy (decided 2026-11-19, when the recording server started to fill up):

- Only ``pm_clob`` raw files are ever deleted. ``pm_gamma`` is never deleted:
  it is about 2 MB a day and the normaliser needs all of it (see markets.py).
- A file is deleted only if its outputs are complete (``pipeline.is_done``)
  AND its ``normalize_stats`` row shows that nothing was dropped
  (``unknown_asset_events == 0``).
- The last ``keep_days`` UTC days before today are kept, so a bug found in
  the normaliser can still be fixed by re-normalising them.
- Day ``SAMPLE_DAY`` of every month is never deleted here: it is the monthly
  raw sample, moved to another machine with ``marketrec.pull`` (which verifies
  every file before deleting it on the server).

Dry run by default: ``python -m rl_binary_mm.store.retention`` lists what would
go; ``--yes`` deletes it.
"""

from __future__ import annotations

import argparse
import collections
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from rl_binary_mm.store.normalize.pipeline import (
    STATS_TABLE,
    RawFile,
    is_done,
    output_paths,
    raw_files,
)

log = logging.getLogger(__name__)

KEEP_DAYS = 2
SAMPLE_DAY = 15


@dataclass
class Plan:
    delete: list[RawFile] = field(default_factory=list)
    kept: collections.Counter[str] = field(default_factory=collections.Counter)  # reason -> files

    @property
    def delete_bytes(self) -> int:
        return sum(raw.path.stat().st_size for raw in self.delete)


def plan(
    data_dir: Path, today: date, *, keep_days: int = KEEP_DAYS, sample_day: int = SAMPLE_DAY
) -> Plan:
    """Decide, file by file, what the policy deletes and why the rest is kept."""
    if keep_days < 0:
        raise ValueError("keep_days must be >= 0")
    oldest_kept = today - timedelta(days=keep_days)
    result = Plan()
    for raw in raw_files(data_dir):
        day = date.fromisoformat(raw.date)
        if day.day == sample_day:
            result.kept["monthly sample"] += 1
        elif day >= oldest_kept:
            result.kept["recent"] += 1
        elif not is_done(data_dir, raw):
            result.kept["not normalised"] += 1
        elif _dropped_events(data_dir, raw):
            result.kept["normalised with dropped events"] += 1
        else:
            result.delete.append(raw)
    return result


def _dropped_events(data_dir: Path, raw: RawFile) -> bool:
    stats = pq.read_table(output_paths(data_dir, raw)[STATS_TABLE]).to_pylist()
    return len(stats) != 1 or stats[0]["unknown_asset_events"] != 0


def execute(result: Plan) -> int:
    """Delete the planned files. Returns the bytes freed."""
    freed = 0
    for raw in result.delete:
        size = raw.path.stat().st_size
        raw.path.unlink()
        freed += size
    return freed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="rl_binary_mm.store.retention", description=__doc__)
    parser.add_argument(
        "--data-dir", type=Path, default=Path(os.environ.get("RL_DATA_DIR", "data"))
    )
    parser.add_argument("--keep-days", type=int, default=KEEP_DAYS)
    parser.add_argument("--yes", action="store_true", help="delete (default: dry run)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    today = datetime.now(UTC).date()
    result = plan(args.data_dir, today, keep_days=args.keep_days)
    log.info(
        "today %s: %d raw files (%.1f GB) to delete; kept: %s",
        today,
        len(result.delete),
        result.delete_bytes / 1e9,
        dict(result.kept),
    )
    if not args.yes:
        log.info("dry run: nothing deleted (pass --yes to delete)")
        return 0
    freed = execute(result)
    log.info("deleted %d files, %.1f GB freed", len(result.delete), freed / 1e9)
    return 0


if __name__ == "__main__":
    sys.exit(main())
