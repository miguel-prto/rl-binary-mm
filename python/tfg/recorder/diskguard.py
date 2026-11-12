"""Stops recording before the disk fills up.

The recorder shares its machine with other services, and a full disk would
also corrupt the file being written. The guard ends the recording when free
space drops below a minimum; recording resumes once space has been freed
(with some hysteresis so it does not flap around the threshold).
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

FreeBytes = Callable[[Path], int]


class LowDiskSpaceError(Exception):
    """Free space dropped below the configured minimum."""


def free_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


async def watch_free_space(
    path: Path,
    min_free_bytes: int,
    *,
    interval_s: float = 30.0,
    free: FreeBytes = free_bytes,
) -> None:
    """Return only by raising ``LowDiskSpaceError`` when space runs low."""
    while True:
        available = free(path)
        if available < min_free_bytes:
            raise LowDiskSpaceError(
                f"{available / 1e9:.2f} GB free at {path}, minimum is {min_free_bytes / 1e9:.2f} GB"
            )
        await asyncio.sleep(interval_s)


async def wait_for_free_space(
    path: Path,
    min_free_bytes: int,
    *,
    interval_s: float = 30.0,
    free: FreeBytes = free_bytes,
) -> None:
    """Block until at least ``min_free_bytes`` are available."""
    warned = False
    while (available := free(path)) < min_free_bytes:
        if not warned:
            log.error(
                "recording paused: %.2f GB free at %s, need %.2f GB",
                available / 1e9,
                path,
                min_free_bytes / 1e9,
            )
            warned = True
        await asyncio.sleep(interval_s)


async def run_guarded(
    work: Callable[[], Coroutine[Any, Any, None]],
    path: Path,
    min_free_bytes: int,
    *,
    resume_factor: float = 1.2,
    interval_s: float = 30.0,
    free: FreeBytes = free_bytes,
) -> None:
    """Run ``work`` while there is disk space; pause and resume around shortages.

    ``work`` is cancelled (so its files are closed cleanly) when free space
    drops below ``min_free_bytes`` and started again once
    ``min_free_bytes * resume_factor`` is available. Other failures of
    ``work`` propagate.
    """
    resume_at = int(min_free_bytes * resume_factor)
    while True:
        await wait_for_free_space(path, resume_at, interval_s=interval_s, free=free)
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(work())
                tg.create_task(
                    watch_free_space(path, min_free_bytes, interval_s=interval_s, free=free)
                )
        except* LowDiskSpaceError as group:
            log.error("recording stopped: %s", group.exceptions[0])
