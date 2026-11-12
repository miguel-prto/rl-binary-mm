import hashlib
import io
import tarfile
from pathlib import Path

import pytest

from tfg.recorder.pull import (
    RemoteFile,
    extract_verified,
    is_finished,
    local_path,
    parse_find_listing,
    parse_sha256sum,
    split_present,
)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def tar_stream(files: dict[str, bytes]) -> io.BytesIO:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    buffer.seek(0)
    return buffer


def test_parse_sha256sum() -> None:
    a, b = sha(b"a"), sha(b"b")
    out = f"{a}  raw/pm_clob/date=2026-11-12/x.jsonl.zst\n{b}  ./raw/with  spaces.zst\n\n"
    assert parse_sha256sum(out) == [
        RemoteFile("raw/pm_clob/date=2026-11-12/x.jsonl.zst", a),
        RemoteFile("raw/with  spaces.zst", b),
    ]
    with pytest.raises(ValueError, match="unexpected"):
        parse_sha256sum("not a checksum line\n")


def test_local_path_rejects_escapes(tmp_path: Path) -> None:
    assert local_path(tmp_path, "raw/a/b.zst") == tmp_path / "raw" / "a" / "b.zst"
    for bad in ("../x", "raw/../../x", "/etc/passwd", ""):
        with pytest.raises(ValueError, match="unsafe"):
            local_path(tmp_path, bad)


def test_split_present_checks_content_not_just_existence(tmp_path: Path) -> None:
    good = RemoteFile("raw/good.zst", sha(b"good"))
    stale = RemoteFile("raw/stale.zst", sha(b"new content"))
    absent = RemoteFile("raw/absent.zst", sha(b"x"))
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "good.zst").write_bytes(b"good")
    (tmp_path / "raw" / "stale.zst").write_bytes(b"old content")

    present, missing = split_present([good, stale, absent], tmp_path)
    assert present == [good]
    assert missing == [stale, absent]


def test_extract_verified_keeps_only_matching_files(tmp_path: Path) -> None:
    ok = RemoteFile("raw/s/date=2026-11-12/ok.zst", sha(b"payload"))
    corrupt = RemoteFile("raw/s/date=2026-11-12/corrupt.zst", sha(b"what the server had"))
    stream = tar_stream(
        {
            ok.path: b"payload",
            corrupt.path: b"damaged in transit",
            "raw/s/unexpected.zst": b"not requested",
        }
    )

    verified = extract_verified(stream, [ok, corrupt], tmp_path)

    assert verified == [ok]
    assert (tmp_path / ok.path).read_bytes() == b"payload"
    assert not (tmp_path / corrupt.path).exists()
    assert not (tmp_path / "raw/s/unexpected.zst").exists()
    assert list(tmp_path.rglob("*.part")) == []


# 2026-11-12T12:00:00Z
NOON = 1_794_484_800
OPEN_FILE = "raw/pm_gamma/date=2026-11-12/pm_gamma_12_20261112T120149-270a.jsonl.zst"


def test_parse_find_listing() -> None:
    listing = "\n".join([str(NOON), f"{NOON - 5}.25\t{OPEN_FILE}", ""])
    now, entries = parse_find_listing(listing)
    assert now == NOON
    assert entries == [(NOON - 5 + 0.25, OPEN_FILE)]


def test_quiet_file_of_the_current_hour_is_not_finished() -> None:
    # Regression: metadata files get a line every 5-15 minutes, so "not
    # modified for 10 minutes" alone would move a file that is still open.
    now = NOON + 40 * 60
    assert not is_finished(OPEN_FILE, now - 14 * 60, now, min_age_s=600, hour_grace_s=1800)


def test_file_is_finished_after_its_hour_plus_grace() -> None:
    hour_end = NOON + 3600
    old_mtime = NOON + 100
    kwargs = {"min_age_s": 600, "hour_grace_s": 1800}
    assert not is_finished(OPEN_FILE, old_mtime, hour_end + 1799, **kwargs)
    assert is_finished(OPEN_FILE, old_mtime, hour_end + 1800, **kwargs)
    # Still being written after the grace period (late rotation): not finished.
    assert not is_finished(OPEN_FILE, hour_end + 1795, hour_end + 1800, **kwargs)


def test_unrecognised_names_are_never_finished() -> None:
    assert not is_finished("raw/other/file.zst", 0, 10**10, min_age_s=0, hour_grace_s=0)
