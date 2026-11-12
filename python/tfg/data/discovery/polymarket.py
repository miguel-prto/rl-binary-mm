"""Discovery of Polymarket crypto "Up/Down" markets through the Gamma API.

Slugs are deterministic: ``{asset}-updown-{window}-{t_start}``, where
``t_start`` is the window start in Unix seconds and a multiple of the window
length. Verified live on 2026-11-12 for the 5m and 15m windows of btc, eth,
sol and xrp (see docs/verification_log.md). Markets are created many hours
ahead, so polling by slug is enough; no push notification is needed.

``GET {base_url}/events?slug={slug}`` returns a JSON list with zero or one
event; the event holds exactly one binary market.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping, Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx

from tfg.data.discovery.spec import MarketSpec

log = logging.getLogger(__name__)

GAMMA_URL = "https://gamma-api.polymarket.com"
VENUE = "polymarket"

# Windows whose slug pattern has been verified. 1h and 4h markets exist but
# use a different naming scheme, not handled here yet.
WINDOW_SECONDS: Mapping[str, int] = {"5m": 300, "15m": 900}


def window_slug(asset: str, window: str, t_start_s: int) -> str:
    """Slug of the market whose window starts at ``t_start_s`` (Unix seconds)."""
    length = WINDOW_SECONDS[window]
    if t_start_s % length != 0:
        raise ValueError(f"t_start_s={t_start_s} is not a multiple of the {window} window")
    return f"{asset}-updown-{window}-{t_start_s}"


def _iso_to_ms(value: str) -> int:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError(f"timestamp without timezone: {value!r}")
    return round(dt.timestamp() * 1000)


def _price_to_e4(value: float | str) -> int:
    scaled = Decimal(str(value)) * 10_000
    if scaled != scaled.to_integral_value() or scaled <= 0:
        raise ValueError(f"price {value!r} is not a positive multiple of 0.0001")
    return int(scaled)


def parse_gamma_event(event: Mapping[str, Any], *, asset: str, window: str) -> MarketSpec:
    """Build a :class:`MarketSpec` from one Gamma event.

    Strict on purpose: anything that contradicts what was verified (two
    outcomes ordered Up/Down, window matching the slug) raises ``ValueError``
    instead of producing a spec that would silently mislabel data.
    """
    slug = event["slug"]
    markets = event["markets"]
    if len(markets) != 1:
        raise ValueError(f"{slug}: expected 1 market, got {len(markets)}")
    market = markets[0]

    outcomes = json.loads(market["outcomes"])
    if outcomes != ["Up", "Down"]:
        raise ValueError(f"{slug}: unexpected outcomes {outcomes!r}")
    token_ids = json.loads(market["clobTokenIds"])
    if len(token_ids) != 2 or not all(isinstance(t, str) and t for t in token_ids):
        raise ValueError(f"{slug}: unexpected clobTokenIds {token_ids!r}")

    start_ms = _iso_to_ms(market["eventStartTime"])
    end_ms = _iso_to_ms(market["endDate"])
    length_ms = WINDOW_SECONDS[window] * 1000
    if slug != window_slug(asset, window, start_ms // 1000) or start_ms % 1000 != 0:
        raise ValueError(f"{slug}: eventStartTime {market['eventStartTime']!r} does not match")
    if end_ms - start_ms != length_ms:
        raise ValueError(f"{slug}: window is {end_ms - start_ms} ms, expected {length_ms}")

    return MarketSpec(
        venue=VENUE,
        market_id=market["conditionId"],
        slug=slug,
        underlying=asset.upper(),
        window_start_ms=start_ms,
        window_end_ms=end_ms,
        asset_ids=(token_ids[0], token_ids[1]),
        tick_size_e4=_price_to_e4(market["orderPriceMinTickSize"]),
        raw=event,
    )


class PolymarketDiscovery:
    """Answers "which Up/Down markets should be subscribed right now?".

    For every (asset, window) it tracks the current window, the ``lookahead``
    following ones (so their books are recorded from before they open) and the
    ``lookbehind`` previous ones (so post-close trading and resolution are
    still captured).

    A spec never changes once created, so found markets are cached and each
    slug is fetched from Gamma only once. Failures (network errors, markets
    not created yet, malformed responses) are logged and skipped: they are
    retried on the next call and never prevent the other markets from being
    returned.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        assets: Sequence[str],
        windows: Sequence[str],
        *,
        lookahead: int = 2,
        lookbehind: int = 1,
        base_url: str = GAMMA_URL,
    ) -> None:
        unknown = [w for w in windows if w not in WINDOW_SECONDS]
        if unknown:
            raise ValueError(f"unsupported windows: {unknown}")
        if lookahead < 0 or lookbehind < 0:
            raise ValueError("lookahead and lookbehind must be >= 0")
        self._client = client
        self._assets = tuple(a.lower() for a in assets)
        self._windows = tuple(windows)
        self._lookahead = lookahead
        self._lookbehind = lookbehind
        self._base_url = base_url.rstrip("/")
        self._cache: dict[str, MarketSpec] = {}

    def wanted_slugs(self, now_ms: int) -> dict[str, tuple[str, str]]:
        """Map each slug that should be tracked at ``now_ms`` to (asset, window)."""
        now_s = now_ms // 1000
        wanted: dict[str, tuple[str, str]] = {}
        for window in self._windows:
            length = WINDOW_SECONDS[window]
            current = now_s - now_s % length
            for k in range(-self._lookbehind, self._lookahead + 1):
                for asset in self._assets:
                    wanted[window_slug(asset, window, current + k * length)] = (asset, window)
        return wanted

    async def active_specs(self, now_ms: int) -> list[MarketSpec]:
        """Specs to be subscribed at ``now_ms``, ordered by window start then slug."""
        wanted = self.wanted_slugs(now_ms)
        # Forget windows that are no longer tracked so the cache stays bounded.
        self._cache = {s: spec for s, spec in self._cache.items() if s in wanted}

        missing = [s for s in wanted if s not in self._cache]
        fetched = await asyncio.gather(*(self._fetch(s, *wanted[s]) for s in missing))
        for spec in fetched:
            if spec is not None:
                self._cache[spec.slug] = spec

        return sorted(self._cache.values(), key=lambda s: (s.window_start_ms, s.slug))

    async def _fetch(self, slug: str, asset: str, window: str) -> MarketSpec | None:
        try:
            response = await self._client.get(f"{self._base_url}/events", params={"slug": slug})
            response.raise_for_status()
            events = response.json()
            if not events:
                log.info("polymarket discovery: %s not created yet", slug)
                return None
            return parse_gamma_event(events[0], asset=asset, window=window)
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            log.warning("polymarket discovery: skipping %s: %r", slug, exc)
            return None
