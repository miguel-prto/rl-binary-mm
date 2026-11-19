"""Normalisation of recorded Polymarket raw files into Parquet tables.

Two stages, both idempotent:

1. The ``markets`` table is rebuilt from every ``pm_gamma`` file (cheap) and
   gives the asset map.
2. Each ``pm_clob`` raw file (one connection, one UTC hour) is normalised on
   its own into one Parquet file per table::

       {data_dir}/normalized/{table}/venue=polymarket/date=YYYY-MM-DD/hour=HH/{conn_id}.parquet

   The partition is the hour of recording (the one in the raw file name);
   events close to the hour boundary can have an exchange timestamp a few ms
   into the neighbouring hour.

A raw file is done when all of its outputs exist (every table is written,
even with no rows, and each one atomically). Files already done are skipped
unless ``force`` is set; re-running after a crash simply redoes the file.

Only finished raw files are normalised, so the pipeline can run on the
recording server next to the recorder: a file is finished when its UTC hour
ended ``hour_grace_s`` ago and it has not been modified for ``min_age_s``. The
rule is ``marketrec.pull.is_finished``, the one ``marketrec.pull`` applies
before moving a file.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from marketrec.pull import is_finished
from marketrec.raw import iter_raw_records

from rl_binary_mm.store import markets
from rl_binary_mm.store.markets import AssetMap
from rl_binary_mm.store.normalize.assemble import OUTPUTS, Assembler, AssembleStats
from rl_binary_mm.store.parquet_io import TableWriter
from rl_binary_mm.store.schema import NORMALIZE_STATS

log = logging.getLogger(__name__)

CLOB_SOURCE = "pm_clob"
STATS_TABLE = "normalize_stats"
# Bump whenever a change alters what is written, so that outputs of older
# versions can be found (normalize_stats.normalizer_version) and redone.
NORMALIZER_VERSION = 1
DRAIN_EVERY_LINES = 200_000
MIN_AGE_S = 10 * 60  # same defaults as marketrec.pull
HOUR_GRACE_S = 30 * 60

_RAW_NAME = re.compile(rf"{CLOB_SOURCE}_(?P<hour>\d{{2}})_(?P<conn_id>[^_]+)\.jsonl\.zst")


@dataclass(frozen=True, slots=True)
class RawFile:
    """A ``pm_clob`` raw file and the partition it belongs to."""

    path: Path
    date: str  # YYYY-MM-DD, UTC
    hour: str  # HH, UTC
    conn_id: str


def parse_raw_path(path: Path) -> RawFile:
    """``.../date=2026-11-13/pm_clob_18_20261113T100036-f1e5.jsonl.zst`` -> RawFile."""
    match = _RAW_NAME.fullmatch(path.name)
    date = path.parent.name.removeprefix("date=")
    if match is None or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise ValueError(f"not a {CLOB_SOURCE} raw file: {path}")
    return RawFile(path=path, date=date, hour=match["hour"], conn_id=match["conn_id"])


def output_paths(data_dir: Path, raw: RawFile) -> dict[str, Path]:
    """Where each table of ``raw`` goes."""
    return {
        table: data_dir
        / "normalized"
        / table
        / f"venue={markets.VENUE}"
        / f"date={raw.date}"
        / f"hour={raw.hour}"
        / f"{raw.conn_id}.parquet"
        for table in [*OUTPUTS, STATS_TABLE]
    }


def raw_files(data_dir: Path, date: str | None = None) -> list[RawFile]:
    """Every ``pm_clob`` raw file under ``data_dir``, optionally of one date."""
    pattern = f"date={date}" if date else "date=*"
    root = data_dir / "raw" / CLOB_SOURCE
    return [parse_raw_path(p) for p in sorted(root.glob(f"{pattern}/*.jsonl.zst"))]


def is_finished_raw(
    raw: RawFile, now_s: float, *, min_age_s: float = MIN_AGE_S, hour_grace_s: float = HOUR_GRACE_S
) -> bool:
    """Whether the recorder can no longer write to ``raw`` (see module docstring)."""
    return is_finished(
        f"date={raw.date}/{raw.path.name}",
        raw.path.stat().st_mtime,
        now_s,
        min_age_s=min_age_s,
        hour_grace_s=hour_grace_s,
    )


def is_done(data_dir: Path, raw: RawFile) -> bool:
    return all(path.exists() for path in output_paths(data_dir, raw).values())


def normalize_file(
    raw: RawFile,
    asset_map: AssetMap,
    data_dir: Path,
    *,
    drain_every: int = DRAIN_EVERY_LINES,
    row_group_rows: int | None = None,
) -> AssembleStats:
    """Normalise one raw file into its output tables.

    Every table is completed as a temporary file first and only then are all
    of them renamed, so a failure leaves either the previous outputs or none,
    never a mix (short of the OS failing between two renames). The
    ``normalize_stats`` row is one of those tables.
    """
    assembler = Assembler(asset_map)
    kwargs = {} if row_group_rows is None else {"row_group_rows": row_group_rows}
    schemas = {**OUTPUTS, STATS_TABLE: NORMALIZE_STATS}
    writers: dict[str, TableWriter] = {}
    try:
        for table, path in output_paths(data_dir, raw).items():
            writers[table] = TableWriter(path, schemas[table], **kwargs)
        for record in iter_raw_records(raw.path):
            assembler.feed(record)
            if assembler.stats.lines % drain_every == 0:
                _drain(assembler, writers)
        _drain(assembler, writers)
        writers[STATS_TABLE].write(stats_batch(raw, assembler.stats))
        for writer in writers.values():
            writer.finish()
    except BaseException:
        for writer in writers.values():
            writer.abort()
        raise
    for writer in writers.values():
        writer.commit()
    return assembler.stats


def stats_batch(raw: RawFile, stats: AssembleStats) -> pa.RecordBatch:
    """The ``normalize_stats`` row of one file."""
    row = {
        "conn_id": raw.conn_id,
        "raw_file": raw.path.name,
        "raw_bytes": raw.path.stat().st_size,
        "normalizer_version": NORMALIZER_VERSION,
        "normalized_at_ns": time.time_ns(),
        "lines": stats.lines,
        "level_changes": stats.events["LevelChange"],
        "trades": stats.events["Trade"],
        "tick_size_changes": stats.events["TickSizeChange"],
        "mirrored_pairs": stats.mirrored_pairs,
        "unmirrored_groups": stats.unmirrored_groups,
        "unknown_asset_events": stats.unknown_asset_events,
        "unknown_assets": len(stats.unknown_assets),
        "snapshots_seen": stats.snapshots_seen,
        "snapshots_kept": stats.snapshots_kept,
        "empty_snapshots_kept": stats.empty_snapshots_kept,
    }
    return pa.RecordBatch.from_pylist([row], schema=NORMALIZE_STATS)


def _drain(assembler: Assembler, writers: dict[str, TableWriter]) -> None:
    for table, batch in assembler.take_batches().items():
        writers[table].write(batch)


@dataclass(frozen=True, slots=True)
class RunResult:
    done: list[tuple[RawFile, AssembleStats]]
    failed: list[tuple[RawFile, str]]
    skipped: int


def run(
    data_dir: Path,
    *,
    date: str | None = None,
    force: bool = False,
    jobs: int = 1,
    min_age_s: float = MIN_AGE_S,
    hour_grace_s: float = HOUR_GRACE_S,
) -> RunResult:
    """Rebuild ``markets`` and normalise every finished, pending raw file."""
    rows = markets.read_markets(data_dir)
    markets.write_markets(rows, data_dir)
    asset_map = markets.asset_map(rows)
    log.info("markets: %d", len(rows))

    now_s = time.time()
    found = raw_files(data_dir, date)
    candidates = [
        raw
        for raw in found
        if is_finished_raw(raw, now_s, min_age_s=min_age_s, hour_grace_s=hour_grace_s)
    ]
    pending = [raw for raw in candidates if force or not is_done(data_dir, raw)]
    log.info("raw files: %d, finished: %d, pending: %d", len(found), len(candidates), len(pending))

    done: list[tuple[RawFile, AssembleStats]] = []
    failed: list[tuple[RawFile, str]] = []
    for raw, outcome in _process(pending, asset_map, data_dir, jobs):
        if isinstance(outcome, AssembleStats):
            done.append((raw, outcome))
            _log_stats(raw, outcome)
        else:
            failed.append((raw, outcome))
            log.error("%s failed: %s", raw.path.name, outcome)
    return RunResult(done=done, failed=failed, skipped=len(candidates) - len(pending))


def _process(
    pending: list[RawFile], asset_map: AssetMap, data_dir: Path, jobs: int
) -> Iterable[tuple[RawFile, AssembleStats | str]]:
    """Normalise files in order (``jobs=1``) or in worker processes."""
    if jobs <= 1:
        for raw in pending:
            yield raw, _try_normalize(raw, asset_map, data_dir)
        return
    with ProcessPoolExecutor(max_workers=jobs) as pool:
        futures = {pool.submit(_try_normalize, raw, asset_map, data_dir): raw for raw in pending}
        for future in as_completed(futures):
            yield futures[future], future.result()


def _try_normalize(raw: RawFile, asset_map: AssetMap, data_dir: Path) -> AssembleStats | str:
    # Errors are returned, not raised, so one bad file does not stop the run.
    try:
        return normalize_file(raw, asset_map, data_dir)
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"


def _log_stats(raw: RawFile, stats: AssembleStats) -> None:
    log.info(
        "%s: %d lines, %d delta rows, mirrored %d / unmirrored %d, snapshots %d/%d kept",
        raw.path.name,
        stats.lines,
        stats.rows["book_deltas"],
        stats.mirrored_pairs,
        stats.unmirrored_groups,
        stats.snapshots_kept,
        stats.snapshots_seen,
    )
    if stats.unknown_asset_events:
        log.warning(
            "%s: %d events of %d unknown assets dropped",
            raw.path.name,
            stats.unknown_asset_events,
            len(stats.unknown_assets),
        )
