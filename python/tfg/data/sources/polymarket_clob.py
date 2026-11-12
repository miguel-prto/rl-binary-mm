"""Recorder for the Polymarket CLOB market channel.

Protocol facts (official docs + live probe on 2026-11-12, see
docs/verification_log.md):
- No authentication. The opening frame is
  ``{"assets_ids": [...], "type": "market", "custom_feature_enabled": true}``.
- Markets are added and removed on the open connection with
  ``{"assets_ids": [...], "operation": "subscribe" | "unsubscribe"}``; a
  subscribe triggers a ``book`` snapshot for each added asset.
- The client must send the text frame ``PING`` every 10 s; the server
  answers ``PONG``.

A *session* is one connection from start to end. It has its own ``conn_id``
and raw file, and runs four concurrent tasks:

    reader         every received frame -> raw file, untouched
    pinger         ``PING`` every ``ping_interval_s``
    subscriptions  every ``refresh_interval_s``: ask discovery, send the diff
    watchdog       no data for ``stale_after_s`` -> end the session

If any task fails, all are cancelled, the raw file is closed cleanly and the
session raises, so that ``run_forever`` reconnects.

Sharding. The server closes connections that it considers slow consumers
(close code 1013, "slow consumer: send buffer full"). With all crypto markets
on one connection (64 assets, 1000-2800 msgs/s) this happened about once a
minute on 2026-11-12 even though the client was not falling behind (delivery
lag stayed constant). The recorder therefore runs several instances, each
with a discovery restricted to a subset of markets and its own connection.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import secrets
import time
from collections.abc import Callable, Iterable, Set
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
from websockets.asyncio.client import ClientConnection, connect

from tfg.data.discovery.spec import Discovery, MarketSpec
from tfg.store.raw import RawWriter

log = logging.getLogger(__name__)

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class StaleConnectionError(Exception):
    """The connection is open but no data has arrived for too long."""


@dataclass(frozen=True, slots=True)
class ClobConfig:
    url: str = WS_URL
    source: str = "pm_clob"  # raw files with the WebSocket frames
    markets_source: str = "pm_gamma"  # raw files with one line per discovered market
    ping_interval_s: float = 10.0
    refresh_interval_s: float = 30.0
    stale_after_s: float = 30.0
    max_frame_bytes: int = 1 << 24


def subscription_diff(current: Set[str], wanted: Set[str]) -> tuple[list[str], list[str]]:
    """Asset ids to subscribe to and to unsubscribe from, each sorted."""
    return sorted(wanted - current), sorted(current - wanted)


def new_conn_id(now_ns: int) -> str:
    """Connection id: UTC start time plus random suffix, e.g. ``20261112T021500-a3f1``."""
    dt = datetime.fromtimestamp(now_ns / 1e9, tz=UTC)
    return f"{dt:%Y%m%dT%H%M%S}-{secrets.token_hex(2)}"


def _json(obj: dict[str, Any]) -> str:
    return orjson.dumps(obj).decode()


def _asset_ids(specs: Iterable[MarketSpec]) -> set[str]:
    return {asset_id for spec in specs for asset_id in spec.asset_ids}


class PolymarketClobRecorder:
    """Records the market channel for the markets chosen by ``discovery``."""

    def __init__(
        self,
        data_dir: Path,
        discovery: Discovery,
        config: ClobConfig = ClobConfig(),  # noqa: B008 - frozen dataclass
        *,
        name: str = "all",
        clock: Callable[[], int] = time.time_ns,
    ) -> None:
        # Label of this recorder when markets are split over several
        # connections (e.g. "btc-15m"); stored in the connect event and logs.
        self._name = name
        self._data_dir = data_dir
        self._discovery = discovery
        self._config = config
        self._clock = clock
        # Slugs whose metadata has already been written, across sessions, so a
        # reconnect does not rewrite every market. Pruned to tracked markets.
        self._recorded_slugs: set[str] = set()

    async def run_session(self) -> None:
        """Run one connection until it fails. Always raises (or is cancelled)."""
        cfg = self._config
        conn_id = new_conn_id(self._clock())
        with (
            RawWriter(self._data_dir, cfg.source, conn_id, clock=self._clock) as frames,
            RawWriter(self._data_dir, cfg.markets_source, conn_id, clock=self._clock) as markets,
        ):
            session = _Session(self, conn_id, frames, markets)
            reason = "unknown"
            try:
                await session.run()
            except BaseException as exc:
                reason = repr(exc)
                raise
            finally:
                frames.write(_json({"event": "disconnect", "reason": reason}), "meta")
                log.info(
                    "%s: session ended after %d lines: %s", conn_id, frames.lines_written, reason
                )


class _Session:
    """State of one connection. Created by ``PolymarketClobRecorder.run_session``."""

    def __init__(
        self,
        recorder: PolymarketClobRecorder,
        conn_id: str,
        frames: RawWriter,
        markets: RawWriter,
    ) -> None:
        self._rec = recorder
        self._cfg = recorder._config
        self._conn_id = conn_id
        self._frames = frames
        self._markets = markets
        self._subscribed: set[str] = set()
        self._last_data = time.monotonic()

    async def run(self) -> None:
        specs = await self._current_specs()
        if not specs:
            raise RuntimeError("discovery returned no markets; nothing to subscribe to")

        self._frames.write(_json({"event": "connecting", "url": self._cfg.url}), "meta")
        async with connect(
            self._cfg.url,
            max_size=self._cfg.max_frame_bytes,
            # Liveness is handled at application level (PING + watchdog).
            ping_interval=None,
        ) as ws:
            self._subscribed = _asset_ids(specs)
            await self._send(
                ws,
                _json(
                    {
                        "assets_ids": sorted(self._subscribed),
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                ),
            )
            self._frames.write(
                _json(
                    {
                        "event": "connect",
                        "shard": self._rec._name,
                        "markets": len(specs),
                        "assets": len(self._subscribed),
                    }
                ),
                "meta",
            )
            log.info("%s [%s]: connected, %d markets", self._conn_id, self._rec._name, len(specs))
            self._last_data = time.monotonic()

            try:
                async with asyncio.TaskGroup() as tg:
                    tg.create_task(self._reader(ws))
                    tg.create_task(self._pinger(ws))
                    tg.create_task(self._subscriptions(ws))
                    tg.create_task(self._watchdog())
            except ExceptionGroup as group:
                # Surface the first real failure instead of the wrapping group.
                raise group.exceptions[0] from None

    # ------------------------------------------------------------------ tasks

    async def _reader(self, ws: ClientConnection) -> None:
        async for message in ws:
            if isinstance(message, str):
                self._frames.write(message, "in")
                if message != "PONG":
                    self._last_data = time.monotonic()
            else:
                # Never seen on this channel; keep it losslessly instead of failing.
                self._frames.write(
                    _json({"event": "binary_frame", "b64": base64.b64encode(message).decode()}),
                    "meta",
                )
        # The iterator ends silently on a clean close; that still ends the session.
        raise ConnectionError("connection closed by server")

    async def _pinger(self, ws: ClientConnection) -> None:
        while True:
            await asyncio.sleep(self._cfg.ping_interval_s)
            await self._send(ws, "PING")

    async def _subscriptions(self, ws: ClientConnection) -> None:
        while True:
            await asyncio.sleep(self._cfg.refresh_interval_s)
            specs = await self._current_specs()
            add, remove = subscription_diff(self._subscribed, _asset_ids(specs))
            if add:
                await self._send(ws, _json({"assets_ids": add, "operation": "subscribe"}))
            if remove:
                await self._send(ws, _json({"assets_ids": remove, "operation": "unsubscribe"}))
            self._subscribed = (self._subscribed | set(add)) - set(remove)
            log.info(
                "%s: %d lines written, %d assets subscribed (+%d -%d)",
                self._conn_id,
                self._frames.lines_written,
                len(self._subscribed),
                len(add),
                len(remove),
            )

    async def _watchdog(self) -> None:
        stale_after = self._cfg.stale_after_s
        while True:
            await asyncio.sleep(stale_after / 4)
            silent_for = time.monotonic() - self._last_data
            if silent_for > stale_after:
                self._frames.write(
                    _json({"event": "stale", "silent_s": round(silent_for, 3)}), "meta"
                )
                raise StaleConnectionError(f"no data for {silent_for:.1f} s")

    # ---------------------------------------------------------------- helpers

    async def _send(self, ws: ClientConnection, text: str) -> None:
        self._frames.write(text, "out")
        await ws.send(text)

    async def _current_specs(self) -> list[MarketSpec]:
        """Ask discovery and persist the metadata of markets not seen before."""
        specs = await self._rec._discovery.active_specs(self._rec._clock() // 1_000_000)
        recorded = self._rec._recorded_slugs
        new = [spec for spec in specs if spec.slug not in recorded]
        for spec in new:
            self._markets.write(_json(dict(spec.raw)), "in")
        if new:
            self._markets.flush()
        recorded.intersection_update({spec.slug for spec in specs})
        recorded.update(spec.slug for spec in new)
        return specs
