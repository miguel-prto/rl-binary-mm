# rl-binary-mm

Adaptive liquidity provision in prediction markets using reinforcement
learning. Bachelor's thesis (Computer Science, UCM): an RL market maker for
short-dated crypto "Up/Down" binary markets on Polymarket and Kalshi, trained
in a C++ limit order book simulator calibrated and replayed on recorded data.

Status: phase 1 (data). Order books are recorded 24/7 by
[market-recorder](https://github.com/miguel-prto/market-recorder); this
repository turns those recordings into Parquet tables. See
[docs/data_design.md](docs/data_design.md).

## Layout

```
cpp/                  C++20 core (L3 order book, matching, fees) + Python bindings
python/rl_binary_mm/  data/ store/ calibration/ market/ env/ agents/ eval/ live/
experiments/          versioned configs + reproducible scripts
notebooks/            EDA and figures
docs/                 design documents
memoria/              thesis (LaTeX, Spanish)
```

## Development

Requires [uv](https://docs.astral.sh/uv/). Python 3.12 is pinned in `.python-version`.

```sh
uv sync                 # create .venv and install dependencies
uv run pytest           # tests
uv run ruff check .     # lint
uv run ruff format .    # format
uv run pyright          # type check
```

Recorded data is expected under `data/` (not committed). To fetch it from the
recording server, use `python -m marketrec.pull` from the market-recorder
repository.
