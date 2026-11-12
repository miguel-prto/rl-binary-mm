"""Command-line entry point of the recorder.

    python -m tfg.recorder polymarket-clob --assets btc,eth,sol,xrp --windows 5m,15m

Stop with Ctrl+C (or SIGTERM on Linux): the open raw file is closed cleanly.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import signal
from pathlib import Path

import httpx

from tfg.data.discovery.polymarket import PolymarketDiscovery
from tfg.data.sources.base import run_forever
from tfg.data.sources.polymarket_clob import PolymarketClobRecorder

log = logging.getLogger("tfg.recorder")


def _csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tfg.recorder", description="Record market data to raw files."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.environ.get("TFG_DATA_DIR", "./data")),
        help="root of the data tree (default: $TFG_DATA_DIR or ./data)",
    )
    sub = parser.add_subparsers(dest="source", required=True)

    clob = sub.add_parser("polymarket-clob", help="Polymarket crypto Up/Down order books")
    clob.add_argument("--assets", type=_csv, default=["btc", "eth", "sol", "xrp"])
    clob.add_argument("--windows", type=_csv, default=["5m", "15m"])
    clob.add_argument("--lookahead", type=int, default=2)
    clob.add_argument("--lookbehind", type=int, default=1)
    clob.add_argument(
        "--single-connection",
        action="store_true",
        help="use one connection for everything instead of one per (asset, window)",
    )
    parser.add_argument(
        "--max-seconds",
        type=float,
        default=None,
        help="stop cleanly after this many seconds (for tests and measurements)",
    )
    return parser


async def _polymarket_clob(args: argparse.Namespace) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        # One connection per (asset, window): the server drops busy connections
        # as "slow consumers", so the load is spread (see polymarket_clob.py).
        if args.single_connection:
            shards = [("all", args.assets, args.windows)]
        else:
            shards = [(f"{a}-{w}", [a], [w]) for a in args.assets for w in args.windows]

        async with asyncio.TaskGroup() as tg:
            for name, assets, windows in shards:
                discovery = PolymarketDiscovery(
                    client,
                    assets,
                    windows,
                    lookahead=args.lookahead,
                    lookbehind=args.lookbehind,
                )
                recorder = PolymarketClobRecorder(args.data_dir, discovery, name=name)
                tg.create_task(run_forever(recorder.run_session))


async def _main(args: argparse.Namespace) -> None:
    task = asyncio.current_task()
    assert task is not None
    # On Linux (Docker) SIGTERM must stop the service as cleanly as Ctrl+C.
    # Windows event loops do not support signal handlers; Ctrl+C still works.
    with contextlib.suppress(NotImplementedError):
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)

    if args.source == "polymarket-clob":
        work = _polymarket_clob(args)
    else:  # unreachable: argparse restricts the choices
        raise ValueError(args.source)

    try:
        async with asyncio.timeout(args.max_seconds):
            await work
    except TimeoutError:
        log.info("reached --max-seconds=%s", args.max_seconds)


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    log.info("recording %s into %s", args.source, args.data_dir.resolve())
    try:
        asyncio.run(_main(args))
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("stopped")


if __name__ == "__main__":
    main()
