# Running the normaliser on the recording server

The normaliser runs as a one-shot container next to the recorder
([market-recorder](https://github.com/miguel-prto/market-recorder)) and reads
the same data directory. It only touches finished raw files (see
`python/rl_binary_mm/store/normalize/pipeline.py`), so it is safe to run while
the recorder is writing.

```sh
git clone https://github.com/miguel-prto/rl-binary-mm ~/rl-binary-mm
cd ~/rl-binary-mm
docker build -f deploy/Dockerfile -t rl-normalize .

# Raw read-only; Parquet goes to <data>/normalized. One CPU at most, so the
# recorder always keeps a core.
docker run --rm --cpus 1 \
  -v ~/tfg-data/raw:/data/raw:ro \
  -v ~/tfg-data/normalized:/data/normalized \
  rl-normalize
```

Measured on the Oracle ARM64 server (2 cores, 2026-11-19): 3.35 MB/s of raw
on one core, so a 15 GB day takes about 75 minutes.

On the server two small scripts wrap the containers: `~/rl-normalize.sh`
(the `docker run` above, inside `flock -n /tmp/rl-normalize.lock` so two runs
never overlap) and `~/rl-retention.sh` (the same image with
`--entrypoint python ... -m rl_binary_mm.store.retention`, raw mounted
read-write, waiting on the same lock). Both run as uid 1001, the owner of the
data directory.

## Raw retention

`python -m rl_binary_mm.store.retention` deletes raw `pm_clob` files that are
normalised with no dropped events, older than 2 days, except day 15 of each
month (the monthly sample). It never deletes `pm_gamma`. Dry run unless
`--yes`. See `python/rl_binary_mm/store/retention.py`.

The monthly sample is moved to the laptop with market-recorder's pull, after
the server has normalised that day:

```sh
uv run python -m marketrec.pull --source pm_clob --date 2026-11-15 --delete
```

Never run an unfiltered `marketrec.pull --delete` from older market-recorder
versions: they also delete `pm_gamma`, which the normaliser needs in full
(newer versions never delete it).

## Cron

```cron
20 * * * * /home/ubuntu/rl-normalize.sh >> /home/ubuntu/rl-normalize.log 2>&1
50 */6 * * * /home/ubuntu/rl-retention.sh --yes >> /home/ubuntu/rl-retention.log 2>&1
```
