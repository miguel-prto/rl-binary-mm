"""Venue-independent description of a market to record."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class MarketSpec:
    """What a connector needs to subscribe to one binary market.

    Timestamps are integer milliseconds since the Unix epoch, UTC.
    Prices use the fixed scale of the storage layer: ``_e4`` = dollars x 10^4.
    """

    venue: str
    market_id: str  # Polymarket: conditionId. Kalshi: market ticker.
    slug: str
    underlying: str  # e.g. "BTC"
    window_start_ms: int
    window_end_ms: int
    asset_ids: tuple[str, str]  # Polymarket CLOB token ids, ordered (up, down)
    tick_size_e4: int
    # Full venue response, kept for the `markets` table. Excluded from
    # equality and hashing so specs can be used in sets and diffed cheaply.
    raw: Mapping[str, Any] = field(compare=False, repr=False)


class Discovery(Protocol):
    """Anything that can tell a connector which markets to record right now."""

    async def active_specs(self, now_ms: int) -> list[MarketSpec]: ...
