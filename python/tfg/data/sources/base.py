"""Shared connector machinery: the reconnect loop."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Backoff:
    """Exponential backoff between sessions.

    The delay starts at ``initial_s``, is multiplied by ``factor`` after every
    session and capped at ``max_s``. A session that lasted at least
    ``reset_after_s`` is considered healthy and resets the delay.
    """

    initial_s: float = 1.0
    max_s: float = 60.0
    factor: float = 2.0
    reset_after_s: float = 60.0


async def run_forever(
    session: Callable[[], Awaitable[None]],
    *,
    backoff: Backoff = Backoff(),  # noqa: B008 - frozen dataclass, safe as default
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Run ``session`` again and again, waiting between attempts.

    A session is one connection from start to end. It is expected to end,
    either by raising (network error, stale connection) or by returning; both
    lead to a reconnect. The only way out of this function is cancellation,
    which is how the service is stopped.
    """
    delay = backoff.initial_s
    while True:
        started = clock()
        try:
            await session()
            log.warning("session ended without error; reconnecting")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("session failed: %r", exc)

        if clock() - started >= backoff.reset_after_s:
            delay = backoff.initial_s
        log.info("reconnecting in %.1f s", delay)
        await sleep(delay)
        delay = min(delay * backoff.factor, backoff.max_s)
