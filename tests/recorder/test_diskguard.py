import asyncio
from pathlib import Path

import pytest

from tfg.recorder.diskguard import (
    LowDiskSpaceError,
    run_guarded,
    wait_for_free_space,
    watch_free_space,
)

GB = 10**9
PATH = Path(".")


class FakeDisk:
    """Free space that follows a script, one value per query, repeating the last."""

    def __init__(self, *values: int) -> None:
        self.values = list(values)
        self.queries = 0

    def __call__(self, path: Path) -> int:
        value = self.values[min(self.queries, len(self.values) - 1)]
        self.queries += 1
        return value


async def test_watch_raises_when_space_runs_low() -> None:
    disk = FakeDisk(5 * GB, 4 * GB, 2 * GB)
    with pytest.raises(LowDiskSpaceError, match=r"2.00 GB free"):
        await watch_free_space(PATH, 3 * GB, interval_s=0, free=disk)
    assert disk.queries == 3


async def test_wait_returns_once_space_is_available() -> None:
    disk = FakeDisk(1 * GB, 1 * GB, 4 * GB)
    await asyncio.wait_for(wait_for_free_space(PATH, 3 * GB, interval_s=0, free=disk), 2)
    assert disk.queries == 3


async def test_run_guarded_pauses_and_resumes_work() -> None:
    # Start: plenty. Then low -> work cancelled. Stays under the resume
    # threshold (3.6 GB) for a while, then recovers -> work restarted.
    disk = FakeDisk(10 * GB, 10 * GB, 2 * GB, 3 * GB, 3 * GB, 10 * GB)
    starts = 0
    cancellations = 0
    restarted = asyncio.Event()

    async def work() -> None:
        nonlocal starts, cancellations
        starts += 1
        if starts == 2:
            restarted.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellations += 1
            raise

    task = asyncio.create_task(run_guarded(work, PATH, 3 * GB, interval_s=0.01, free=disk))
    await asyncio.wait_for(restarted.wait(), 2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert starts == 2
    assert cancellations == 2  # once by the guard, once by the final cancel


async def test_run_guarded_propagates_other_failures() -> None:
    async def work() -> None:
        raise ValueError("bug")

    with pytest.raises(ExceptionGroup) as info:
        await run_guarded(work, PATH, 1 * GB, interval_s=0.01, free=FakeDisk(10 * GB))
    assert isinstance(info.value.exceptions[0], ValueError)
