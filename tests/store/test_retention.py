"""The raw retention policy (see store/retention.py)."""

from datetime import UTC, date, datetime
from pathlib import Path

from marketrec.raw import RawWriter

from rl_binary_mm.store import retention
from rl_binary_mm.store.normalize.assemble import OUTPUTS, AssembleStats
from rl_binary_mm.store.normalize.pipeline import (
    STATS_TABLE,
    RawFile,
    output_paths,
    parse_raw_path,
    stats_batch,
)
from rl_binary_mm.store.parquet_io import TableWriter
from rl_binary_mm.store.schema import NORMALIZE_STATS

TODAY = date(2026, 11, 19)


def raw_file(data_dir: Path, day: int, conn: str = "c") -> RawFile:
    noon_ns = int(datetime(2026, 11, day, 12, tzinfo=UTC).timestamp()) * 10**9
    with RawWriter(data_dir, "pm_clob", f"{conn}{day:02d}", clock=lambda: noon_ns) as writer:
        writer.write("PONG", "in")
        path = writer.path
    assert path is not None
    return parse_raw_path(path)


def normalised(data_dir: Path, raw: RawFile, *, unknown: int = 0) -> None:
    """Write the outputs of ``raw`` as the pipeline would (empty tables)."""
    stats = AssembleStats(unknown_asset_events=unknown)
    for table, path in output_paths(data_dir, raw).items():
        schema = NORMALIZE_STATS if table == STATS_TABLE else OUTPUTS[table]
        with TableWriter(path, schema) as writer:
            if table == STATS_TABLE:
                writer.write(stats_batch(raw, stats))


def test_policy(tmp_path: Path) -> None:
    old_ok = raw_file(tmp_path, 10)
    normalised(tmp_path, old_ok)
    old_pending = raw_file(tmp_path, 11)  # never normalised
    old_lossy = raw_file(tmp_path, 12)
    normalised(tmp_path, old_lossy, unknown=5)
    sample = raw_file(tmp_path, 15)
    normalised(tmp_path, sample)
    boundary = raw_file(tmp_path, 16)  # TODAY - 3 days: old enough
    normalised(tmp_path, boundary)
    recent = raw_file(tmp_path, 17)  # TODAY - 2 days: kept
    normalised(tmp_path, recent)

    result = retention.plan(tmp_path, TODAY, keep_days=2)
    assert sorted(r.date for r in result.delete) == ["2026-11-10", "2026-11-16"]
    assert result.kept == {
        "not normalised": 1,
        "normalised with dropped events": 1,
        "monthly sample": 1,
        "recent": 1,
    }

    planned_bytes = result.delete_bytes
    assert planned_bytes > 0
    assert retention.execute(result) == planned_bytes
    assert not old_ok.path.exists() and not boundary.path.exists()
    for kept in (old_pending, old_lossy, sample, recent):
        assert kept.path.exists()
    # Outputs are never touched.
    assert all(p.exists() for p in output_paths(tmp_path, old_ok).values())


def test_pm_gamma_is_never_considered(tmp_path: Path) -> None:
    noon_ns = int(datetime(2026, 11, 1, 12, tzinfo=UTC).timestamp()) * 10**9
    with RawWriter(tmp_path, "pm_gamma", "g", clock=lambda: noon_ns) as writer:
        writer.write("{}", "in")
    assert retention.plan(tmp_path, TODAY).delete == []


def test_dry_run_deletes_nothing(tmp_path: Path) -> None:
    raw = raw_file(tmp_path, 10)
    normalised(tmp_path, raw)
    assert retention.main(["--data-dir", str(tmp_path)]) == 0
    assert raw.path.exists()
