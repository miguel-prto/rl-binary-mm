from pathlib import Path

import orjson
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def pm_frames() -> dict[str, str]:
    """Real pm_clob frames, one per case, by name (see fixtures/pm_clob_frames.jsonl)."""
    lines = (FIXTURES / "pm_clob_frames.jsonl").read_text(encoding="utf-8").splitlines()
    return {rec["name"]: rec["frame"] for rec in map(orjson.loads, lines)}
