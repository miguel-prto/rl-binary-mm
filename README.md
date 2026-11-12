# Adaptive Liquidity Provision in Prediction Markets Using RL

Bachelor's thesis (Computer Science, UCM). An RL market maker for short-dated
crypto "Up/Down" binary markets on Polymarket and Kalshi, with a C++ limit
order book simulator and a 24/7 data recorder.

Status: phase 1 (data recorder). See [docs/recorder_design.md](docs/recorder_design.md).

## Layout

```
cpp/            C++20 core (L3 order book, matching, fees) + Python bindings
python/tfg/     data/ store/ calibration/ market/ env/ agents/ eval/ live/
recorder/       24/7 service deployable with Docker
experiments/    versioned configs + reproducible scripts
notebooks/      EDA and figures
docs/           design documents
memoria/        thesis (LaTeX, Spanish)
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

Secrets go in `.env` (see `.env.example`); nothing sensitive is committed.
