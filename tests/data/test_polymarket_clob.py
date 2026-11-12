"""Session tests against a real WebSocket server running in-process on localhost."""

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from websockets.asyncio.server import ServerConnection, serve

from tfg.data.discovery.spec import MarketSpec
from tfg.data.sources.polymarket_clob import (
    ClobConfig,
    PolymarketClobRecorder,
    StaleConnectionError,
    new_conn_id,
    subscription_diff,
)
from tfg.store.raw import RawRecord, iter_raw_records

Handler = Callable[[ServerConnection], Awaitable[None]]


def spec(name: str) -> MarketSpec:
    return MarketSpec(
        venue="polymarket",
        market_id=f"0x{name}",
        slug=name,
        underlying="BTC",
        window_start_ms=0,
        window_end_ms=900_000,
        asset_ids=(f"{name}-up", f"{name}-down"),
        tick_size_e4=100,
        raw={"slug": name},
    )


class FakeDiscovery:
    """Returns the scripted spec lists in order, then repeats the last one."""

    def __init__(self, *answers: list[MarketSpec]) -> None:
        self.answers = list(answers)
        self.calls = 0

    async def active_specs(self, now_ms: int) -> list[MarketSpec]:
        answer = self.answers[min(self.calls, len(self.answers) - 1)]
        self.calls += 1
        return answer


@asynccontextmanager
async def ws_server(handler: Handler) -> AsyncIterator[str]:
    async with serve(handler, "127.0.0.1", 0) as server:
        port = next(iter(server.sockets)).getsockname()[1]
        yield f"ws://127.0.0.1:{port}"


def config(url: str, **overrides: float) -> ClobConfig:
    defaults: dict[str, float] = {
        "ping_interval_s": 0.05,
        "refresh_interval_s": 0.05,
        "stale_after_s": 5.0,
    }
    return ClobConfig(url=url, **(defaults | overrides))  # type: ignore[arg-type]


def records(root: Path, source: str) -> list[RawRecord]:
    files = sorted((root / "raw" / source).rglob("*.jsonl.zst"))
    return [rec for f in files for rec in iter_raw_records(f)]


def meta_events(recs: list[RawRecord]) -> list[str]:
    return [json.loads(r.frame)["event"] for r in recs if r.direction == "meta"]


# -------------------------------------------------------------- pure helpers


def test_subscription_diff() -> None:
    assert subscription_diff({"a", "b"}, {"b", "c", "d"}) == (["c", "d"], ["a"])
    assert subscription_diff(set(), {"b", "a"}) == (["a", "b"], [])
    assert subscription_diff({"a"}, {"a"}) == ([], [])


def test_new_conn_id_format() -> None:
    conn_id = new_conn_id(1_794_524_400 * 1_000_000_000)
    assert conn_id.startswith("20261112T230000-")
    assert len(conn_id) == len("20261112T230000-") + 4


# ------------------------------------------------------------------ sessions


async def test_session_records_frames_pings_and_metadata(tmp_path: Path) -> None:
    book = '{"event_type":"book","asset_id":"m1-up","bids":[],"asks":[]}'
    change = '[{"event_type":"price_change",  "spacing":"kept"}]'

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()  # opening subscription
        await ws.send(book)
        await ws.send(change)
        assert await ws.recv() == "PING"
        await ws.send("PONG")
        await ws.close()

    async with ws_server(handler) as url:
        recorder = PolymarketClobRecorder(tmp_path, FakeDiscovery([spec("m1")]), config(url))
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(recorder.run_session(), timeout=5)

    recs = records(tmp_path, "pm_clob")
    assert len({r.conn_id for r in recs}) == 1

    sent = [r.frame for r in recs if r.direction == "out"]
    assert json.loads(sent[0]) == {
        "assets_ids": ["m1-down", "m1-up"],
        "type": "market",
        "custom_feature_enabled": True,
    }
    assert "PING" in sent

    # Received frames are stored byte for byte, including PONG.
    assert [r.frame for r in recs if r.direction == "in"] == [book, change, "PONG"]

    events = meta_events(recs)
    assert events[:2] == ["connecting", "connect"]
    assert events[-1] == "disconnect"

    # Market metadata goes to its own source.
    assert [json.loads(r.frame) for r in records(tmp_path, "pm_gamma")] == [{"slug": "m1"}]


async def test_session_follows_discovery_changes(tmp_path: Path) -> None:
    received: list[dict] = []
    done = asyncio.Event()

    async def handler(ws: ServerConnection) -> None:
        async for message in ws:
            if message == "PING":
                continue
            received.append(json.loads(message))
            if len(received) == 3:
                done.set()

    discovery = FakeDiscovery([spec("m1"), spec("m2")], [spec("m2"), spec("m3")])
    async with ws_server(handler) as url:
        recorder = PolymarketClobRecorder(tmp_path, discovery, config(url, ping_interval_s=60))
        task = asyncio.create_task(recorder.run_session())
        await asyncio.wait_for(done.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert received[0]["assets_ids"] == ["m1-down", "m1-up", "m2-down", "m2-up"]
    assert received[1] == {"assets_ids": ["m3-down", "m3-up"], "operation": "subscribe"}
    assert received[2] == {"assets_ids": ["m1-down", "m1-up"], "operation": "unsubscribe"}

    # Each market's metadata is written once, when first seen.
    slugs = [json.loads(r.frame)["slug"] for r in records(tmp_path, "pm_gamma")]
    assert slugs == ["m1", "m2", "m3"]
    # Cancellation still closes the raw file cleanly, with the disconnect marker.
    assert meta_events(records(tmp_path, "pm_clob"))[-1] == "disconnect"


async def test_watchdog_ends_a_silent_connection(tmp_path: Path) -> None:
    async def handler(ws: ServerConnection) -> None:
        async for message in ws:
            if message == "PING":
                await ws.send("PONG")  # alive at protocol level, but no data

    async with ws_server(handler) as url:
        recorder = PolymarketClobRecorder(
            tmp_path, FakeDiscovery([spec("m1")]), config(url, stale_after_s=0.2)
        )
        with pytest.raises(StaleConnectionError):
            await asyncio.wait_for(recorder.run_session(), timeout=5)

    recs = records(tmp_path, "pm_clob")
    assert "PONG" in [r.frame for r in recs if r.direction == "in"]
    assert meta_events(recs)[-2:] == ["stale", "disconnect"]
    assert "StaleConnectionError" in json.loads(recs[-1].frame)["reason"]


async def test_metadata_is_not_rewritten_on_reconnect(tmp_path: Path) -> None:
    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.close()

    async with ws_server(handler) as url:
        recorder = PolymarketClobRecorder(tmp_path, FakeDiscovery([spec("m1")]), config(url))
        for _ in range(2):
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(recorder.run_session(), timeout=5)

    assert len({r.conn_id for r in records(tmp_path, "pm_clob")}) == 2
    assert len(records(tmp_path, "pm_gamma")) == 1


async def test_session_fails_fast_without_markets(tmp_path: Path) -> None:
    recorder = PolymarketClobRecorder(tmp_path, FakeDiscovery([]), config("ws://127.0.0.1:1"))
    with pytest.raises(RuntimeError, match="no markets"):
        await recorder.run_session()
