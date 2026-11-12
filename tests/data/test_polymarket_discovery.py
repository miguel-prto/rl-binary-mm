import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from tfg.data.discovery.polymarket import (
    PolymarketDiscovery,
    parse_gamma_event,
    window_slug,
)

FIXTURE = Path(__file__).parent.parent / "fixtures" / "gamma" / "event_btc_updown_15m.json"
# Real Gamma response recorded on 2026-11-12 for this slug.
FIXTURE_START_S = 1_794_481_200


@pytest.fixture
def event() -> dict[str, Any]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))[0]


def iso(t_s: int) -> str:
    return datetime.fromtimestamp(t_s, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def event_for_slug(template: dict[str, Any], slug: str) -> dict[str, Any]:
    """Synthesize a Gamma event for any slug from the real fixture."""
    _asset, _, window, start = slug.split("-")
    length = {"5m": 300, "15m": 900}[window]
    ev = copy.deepcopy(template)
    ev["slug"] = slug
    market = ev["markets"][0]
    market["slug"] = slug
    market["conditionId"] = f"0xcond-{slug}"
    market["eventStartTime"] = iso(int(start))
    market["endDate"] = iso(int(start) + length)
    return ev


class FakeGamma:
    """httpx transport that serves synthesized events and counts requests."""

    def __init__(self, template: dict[str, Any]) -> None:
        self.template = template
        self.requests: list[str] = []
        self.missing: set[str] = set()
        self.failing: set[str] = set()

    def __call__(self, request: httpx.Request) -> httpx.Response:
        slug = request.url.params["slug"]
        self.requests.append(slug)
        if slug in self.failing:
            return httpx.Response(500)
        if slug in self.missing:
            return httpx.Response(200, json=[])
        return httpx.Response(200, json=[event_for_slug(self.template, slug)])

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


# ------------------------------------------------------------------ parsing


def test_window_slug() -> None:
    assert window_slug("btc", "15m", 1_794_481_200) == "btc-updown-15m-1794481200"
    with pytest.raises(ValueError, match="multiple"):
        window_slug("btc", "15m", 1_794_481_201)


def test_parse_real_gamma_event(event: dict[str, Any]) -> None:
    spec = parse_gamma_event(event, asset="btc", window="15m")
    market = event["markets"][0]

    assert spec.venue == "polymarket"
    assert spec.slug == f"btc-updown-15m-{FIXTURE_START_S}"
    assert spec.underlying == "BTC"
    assert spec.market_id == market["conditionId"]
    assert spec.window_start_ms == FIXTURE_START_S * 1000
    assert spec.window_end_ms == (FIXTURE_START_S + 900) * 1000
    assert list(spec.asset_ids) == json.loads(market["clobTokenIds"])
    assert spec.tick_size_e4 == 100  # 0.01
    assert spec.raw is event


def test_parse_rejects_reversed_outcomes(event: dict[str, Any]) -> None:
    event["markets"][0]["outcomes"] = '["Down", "Up"]'
    with pytest.raises(ValueError, match="outcomes"):
        parse_gamma_event(event, asset="btc", window="15m")


def test_parse_rejects_window_mismatch(event: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="does not match"):
        parse_gamma_event(event, asset="btc", window="5m")
    event["markets"][0]["endDate"] = iso(FIXTURE_START_S + 300)
    with pytest.raises(ValueError, match="window is"):
        parse_gamma_event(event, asset="btc", window="15m")


def test_parse_rejects_tick_finer_than_scale(event: dict[str, Any]) -> None:
    event["markets"][0]["orderPriceMinTickSize"] = 0.00001
    with pytest.raises(ValueError, match=r"multiple of 0.0001"):
        parse_gamma_event(event, asset="btc", window="15m")


def test_parse_accepts_finer_supported_tick(event: dict[str, Any]) -> None:
    event["markets"][0]["orderPriceMinTickSize"] = 0.001
    assert parse_gamma_event(event, asset="btc", window="15m").tick_size_e4 == 10


# ---------------------------------------------------------------- discovery

NOW_MS = (FIXTURE_START_S + 421) * 1000  # 7 min into the fixture's window


async def test_active_specs_covers_previous_current_and_next_windows(
    event: dict[str, Any],
) -> None:
    gamma = FakeGamma(event)
    async with gamma.client() as client:
        discovery = PolymarketDiscovery(client, ["btc", "eth"], ["15m"], lookahead=2, lookbehind=1)
        specs = await discovery.active_specs(NOW_MS)

    starts = [FIXTURE_START_S + k * 900 for k in (-1, 0, 1, 2)]
    assert [s.slug for s in specs] == [
        f"{asset}-updown-15m-{t}" for t in starts for asset in ("btc", "eth")
    ]
    assert {s.underlying for s in specs} == {"BTC", "ETH"}


async def test_windows_of_different_length_are_aligned_independently(
    event: dict[str, Any],
) -> None:
    gamma = FakeGamma(event)
    async with gamma.client() as client:
        discovery = PolymarketDiscovery(client, ["btc"], ["5m", "15m"], lookahead=0, lookbehind=0)
        specs = await discovery.active_specs(NOW_MS)

    # 421 s into the 15m window is 121 s into its second 5m window.
    assert {s.slug for s in specs} == {
        f"btc-updown-15m-{FIXTURE_START_S}",
        f"btc-updown-5m-{FIXTURE_START_S + 300}",
    }


async def test_found_specs_are_cached_and_old_ones_dropped(event: dict[str, Any]) -> None:
    gamma = FakeGamma(event)
    async with gamma.client() as client:
        discovery = PolymarketDiscovery(client, ["btc"], ["15m"], lookahead=1, lookbehind=0)
        first = await discovery.active_specs(NOW_MS)
        assert len(gamma.requests) == 2

        await discovery.active_specs(NOW_MS + 60_000)  # same window: nothing new to fetch
        assert len(gamma.requests) == 2

        later = await discovery.active_specs(NOW_MS + 900_000)  # next window
        assert len(gamma.requests) == 3

    assert [s.slug for s in first] == [
        f"btc-updown-15m-{FIXTURE_START_S}",
        f"btc-updown-15m-{FIXTURE_START_S + 900}",
    ]
    assert [s.slug for s in later] == [
        f"btc-updown-15m-{FIXTURE_START_S + 900}",
        f"btc-updown-15m-{FIXTURE_START_S + 1800}",
    ]


async def test_missing_and_failing_markets_are_skipped_then_retried(
    event: dict[str, Any],
) -> None:
    current = f"btc-updown-15m-{FIXTURE_START_S}"
    upcoming = f"btc-updown-15m-{FIXTURE_START_S + 900}"
    gamma = FakeGamma(event)
    gamma.missing.add(upcoming)
    gamma.failing.add(f"eth-updown-15m-{FIXTURE_START_S}")

    async with gamma.client() as client:
        discovery = PolymarketDiscovery(client, ["btc", "eth"], ["15m"], lookahead=1, lookbehind=0)
        specs = await discovery.active_specs(NOW_MS)
        assert [s.slug for s in specs] == [current, f"eth-updown-15m-{FIXTURE_START_S + 900}"]

        gamma.missing.clear()
        gamma.failing.clear()
        specs = await discovery.active_specs(NOW_MS)
        assert len(specs) == 4


async def test_malformed_market_does_not_block_the_others(event: dict[str, Any]) -> None:
    bad = copy.deepcopy(event)
    bad["markets"][0]["outcomes"] = '["Down", "Up"]'
    good_gamma = FakeGamma(event)

    def handler(request: httpx.Request) -> httpx.Response:
        slug = request.url.params["slug"]
        if slug.startswith("eth-"):
            return httpx.Response(200, json=[event_for_slug(bad, slug)])
        return good_gamma(request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        discovery = PolymarketDiscovery(client, ["btc", "eth"], ["15m"], lookahead=0, lookbehind=0)
        specs = await discovery.active_specs(NOW_MS)

    assert [s.slug for s in specs] == [f"btc-updown-15m-{FIXTURE_START_S}"]


def test_rejects_unverified_windows() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        PolymarketDiscovery(httpx.AsyncClient(), ["btc"], ["1h"])
