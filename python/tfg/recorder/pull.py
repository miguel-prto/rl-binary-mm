"""Move finished raw files from the recording server to this machine.

    python -m tfg.recorder.pull            # copy and verify, keep remote files
    python -m tfg.recorder.pull --delete   # also delete verified files remotely

Connection settings come from the environment (or ``.env``, see .env.example):
``TFG_REMOTE_HOST`` (user@host), ``TFG_REMOTE_KEY`` (path to the SSH private
key) and ``TFG_REMOTE_DIR`` (data directory on the server).

Safety rules
- Only finished files are touched. A file is finished when the UTC hour in its
  name ended at least ``--hour-grace-min`` minutes ago *and* it has not been
  modified for ``--min-age-min`` minutes. The modification time alone is not
  enough: a quiet file (market metadata) can stay open for many minutes
  between writes, and a writer only rotates to the next hour's file on its
  first write after the hour changes.
- Every file is checked against the SHA-256 computed on the server before it
  is given its final name. A remote file is deleted only after that check.
- Re-running is safe: files already present with the right hash are skipped.

Requires the ``ssh`` client on PATH; the server needs find, sha256sum and tar.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import shlex
import subprocess
import sys
import tarfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import IO

log = logging.getLogger("tfg.recorder.pull")

BATCH = 100  # files per ssh/tar invocation
NUL = chr(0)  # separator for file names sent to `xargs -0` / `tar --null`

_RAW_NAME = re.compile(r"date=(\d{4})-(\d{2})-(\d{2})/[^/]*_(\d{2})_[^/_]+\.jsonl\.zst$")


@dataclass(frozen=True, slots=True)
class RemoteFile:
    path: str  # POSIX path relative to the remote data directory
    sha256: str


def parse_find_listing(output: str) -> tuple[float, list[tuple[float, str]]]:
    """Parse the output of ``date +%s`` followed by ``find -printf '%T@ TAB %p'``.

    Returns the server's current time and (mtime, path) pairs, all in Unix
    seconds by the server's clock, so local clock skew does not matter.
    """
    first, *lines = output.splitlines()
    entries = []
    for line in lines:
        mtime, sep, path = line.partition("\t")
        if sep:
            entries.append((float(mtime), path.removeprefix("./")))
    return float(first), entries


def is_finished(
    path: str, mtime_s: float, now_s: float, *, min_age_s: float, hour_grace_s: float
) -> bool:
    """Whether a raw file can no longer be written to (see module docstring)."""
    match = _RAW_NAME.search(path)
    if match is None:
        return False
    year, month, day, hour = map(int, match.groups())
    hour_end_s = datetime(year, month, day, hour, tzinfo=UTC).timestamp() + 3600
    return now_s - mtime_s >= min_age_s and now_s >= hour_end_s + hour_grace_s


def parse_sha256sum(output: str) -> list[RemoteFile]:
    """Parse ``sha256sum`` output (``<hash>  <path>`` per line)."""
    files = []
    for line in output.splitlines():
        if not line.strip():
            continue
        digest, sep, path = line.partition("  ")
        if not sep or len(digest) != 64:
            raise ValueError(f"unexpected sha256sum line: {line!r}")
        files.append(RemoteFile(path=path.removeprefix("./"), sha256=digest))
    return files


def local_path(local_dir: Path, remote_path: str) -> Path:
    """Local destination of a remote file; rejects paths escaping ``local_dir``."""
    parts = PurePosixPath(remote_path).parts
    if not parts or PurePosixPath(remote_path).is_absolute() or ".." in parts:
        raise ValueError(f"unsafe remote path: {remote_path!r}")
    return local_dir.joinpath(*parts)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def split_present(
    files: Iterable[RemoteFile], local_dir: Path
) -> tuple[list[RemoteFile], list[RemoteFile]]:
    """Split into (already present locally with the right hash, still to fetch)."""
    present, missing = [], []
    for file in files:
        dest = local_path(local_dir, file.path)
        if dest.exists() and sha256_of(dest) == file.sha256:
            present.append(file)
        else:
            missing.append(file)
    return present, missing


def extract_verified(
    stream: IO[bytes], expected: Sequence[RemoteFile], local_dir: Path
) -> list[RemoteFile]:
    """Extract a tar stream, keeping only files whose SHA-256 matches.

    Each file is written to ``<name>.part`` and renamed after verification, so
    an interrupted transfer never leaves a truncated file under its real name.
    Returns the verified files.
    """
    wanted = {file.path: file for file in expected}
    verified = []
    with tarfile.open(fileobj=stream, mode="r|") as tar:
        for member in tar:
            name = member.name.removeprefix("./")
            file = wanted.get(name)
            source = tar.extractfile(member) if member.isfile() else None
            if file is None or source is None:
                log.warning("ignoring unexpected tar entry %r", member.name)
                continue
            dest = local_path(local_dir, name)
            dest.parent.mkdir(parents=True, exist_ok=True)
            part = dest.with_name(dest.name + ".part")
            digest = hashlib.sha256()
            with open(part, "wb") as out:
                while chunk := source.read(1 << 20):
                    digest.update(chunk)
                    out.write(chunk)
            if digest.hexdigest() != file.sha256:
                part.unlink()
                log.error("checksum mismatch, not kept: %s", name)
                continue
            part.replace(dest)
            verified.append(file)
    return verified


# ------------------------------------------------------------------ ssh side


@dataclass(frozen=True, slots=True)
class Remote:
    host: str
    key: str | None
    directory: str

    def _ssh(self, command: str) -> list[str]:
        args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
        if self.key:
            args += ["-i", self.key]
        return [*args, self.host, f"cd {shlex.quote(self.directory)} && {command}"]

    def list_finished(self, min_age_s: float, hour_grace_s: float) -> list[RemoteFile]:
        listing = subprocess.run(
            self._ssh(r"date +%s && find raw -type f -name '*.zst' -printf '%T@\t%p\n'"),
            capture_output=True,
            check=True,
        )
        now_s, entries = parse_find_listing(listing.stdout.decode())
        names = sorted(
            path
            for mtime_s, path in entries
            if is_finished(path, mtime_s, now_s, min_age_s=min_age_s, hour_grace_s=hour_grace_s)
        )
        if not names:
            return []
        sums = subprocess.run(
            self._ssh("xargs -0 -r sha256sum"),
            input=NUL.join(names).encode(),
            capture_output=True,
            check=True,
        )
        return parse_sha256sum(sums.stdout.decode())

    def fetch(self, files: Sequence[RemoteFile], local_dir: Path) -> list[RemoteFile]:
        names = NUL.join(f.path for f in files).encode()
        proc = subprocess.Popen(
            self._ssh("tar -cf - --null -T -"), stdin=subprocess.PIPE, stdout=subprocess.PIPE
        )
        assert proc.stdin is not None and proc.stdout is not None
        proc.stdin.write(names)  # at most BATCH short names: fits the pipe buffer
        proc.stdin.close()
        try:
            return extract_verified(proc.stdout, files, local_dir)
        finally:
            proc.stdout.close()
            if proc.wait() != 0:
                log.error("remote tar exited with code %s", proc.returncode)

    def delete(self, files: Sequence[RemoteFile]) -> None:
        names = NUL.join(f.path for f in files).encode()
        subprocess.run(self._ssh("xargs -0 -r rm -f --"), input=names, check=True)


def _load_dotenv(path: Path) -> None:
    """Minimal .env reader: KEY=VALUE lines, existing variables win."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.strip().partition("=")
        if sep and key and not key.startswith("#"):
            os.environ.setdefault(key, value.strip().strip('"'))


def main() -> int:
    _load_dotenv(Path(".env"))
    parser = argparse.ArgumentParser(
        prog="tfg.recorder.pull", description="Pull finished raw files."
    )
    parser.add_argument("--host", default=os.environ.get("TFG_REMOTE_HOST"))
    parser.add_argument("--key", default=os.environ.get("TFG_REMOTE_KEY"))
    parser.add_argument("--remote-dir", default=os.environ.get("TFG_REMOTE_DIR", "tfg-data"))
    parser.add_argument(
        "--local-dir", type=Path, default=Path(os.environ.get("TFG_DATA_DIR", "./data"))
    )
    parser.add_argument("--min-age-min", type=float, default=10)
    parser.add_argument("--hour-grace-min", type=float, default=30)
    parser.add_argument("--delete", action="store_true", help="delete verified files remotely")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not args.host:
        parser.error("set TFG_REMOTE_HOST (or pass --host)")

    remote = Remote(args.host, args.key, args.remote_dir)
    finished = remote.list_finished(args.min_age_min * 60, args.hour_grace_min * 60)
    present, missing = split_present(finished, args.local_dir)
    log.info(
        "%d finished files on the server: %d already here, %d to fetch",
        len(finished),
        len(present),
        len(missing),
    )

    verified = list(present)
    for start in range(0, len(missing), BATCH):
        batch = missing[start : start + BATCH]
        got = remote.fetch(batch, args.local_dir)
        verified += got
        log.info("fetched %d/%d", min(start + BATCH, len(missing)), len(missing))
        if len(got) != len(batch):
            log.error("%d files of this batch were not verified", len(batch) - len(got))

    failed = len(finished) - len(verified)
    if args.delete and verified:
        remote.delete(verified)
        log.info("deleted %d verified files on the server", len(verified))
    elif verified:
        log.info("remote files kept (run with --delete to free space on the server)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
