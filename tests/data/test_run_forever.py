import asyncio

import pytest

from tfg.data.sources.base import Backoff, run_forever


class FakeTime:
    """Fake monotonic clock + sleep that stops the loop after ``max_sleeps``."""

    def __init__(self, max_sleeps: int) -> None:
        self.now = 0.0
        self.delays: list[float] = []
        self.max_sleeps = max_sleeps

    def clock(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        self.delays.append(delay)
        if len(self.delays) >= self.max_sleeps:
            raise asyncio.CancelledError
        self.now += delay


async def test_backoff_grows_and_is_capped() -> None:
    fake = FakeTime(max_sleeps=6)

    async def session() -> None:
        raise ConnectionError("boom")

    with pytest.raises(asyncio.CancelledError):
        await run_forever(
            session,
            backoff=Backoff(initial_s=1, max_s=10, factor=2, reset_after_s=60),
            sleep=fake.sleep,
            clock=fake.clock,
        )
    assert fake.delays == [1, 2, 4, 8, 10, 10]


async def test_long_session_resets_backoff() -> None:
    fake = FakeTime(max_sleeps=4)
    durations = iter([0.0, 0.0, 120.0, 0.0])

    async def session() -> None:
        fake.now += next(durations)
        raise ConnectionError("boom")

    with pytest.raises(asyncio.CancelledError):
        await run_forever(
            session,
            backoff=Backoff(initial_s=1, max_s=60, factor=2, reset_after_s=60),
            sleep=fake.sleep,
            clock=fake.clock,
        )
    # Third session lasted 120 s: the delay goes back to the initial value.
    assert fake.delays == [1, 2, 1, 2]


async def test_session_returning_normally_also_reconnects() -> None:
    fake = FakeTime(max_sleeps=2)
    calls = 0

    async def session() -> None:
        nonlocal calls
        calls += 1

    with pytest.raises(asyncio.CancelledError):
        await run_forever(session, sleep=fake.sleep, clock=fake.clock)
    assert calls == 2


async def test_cancellation_inside_session_propagates() -> None:
    fake = FakeTime(max_sleeps=10)

    async def session() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_forever(session, sleep=fake.sleep, clock=fake.clock)
    assert fake.delays == []
