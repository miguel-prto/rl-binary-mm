"""Exact conversion of decimal strings to fixed-scale integers.

Venues send prices and sizes as decimal strings ("0.28", "453.73"). Storing
them as floats would make equality checks and the matching engine fragile, so
every quantity is kept as an integer scaled by a power of ten: with scale 4,
"0.28" becomes 2800 (``price_e4``). See D2 in ``docs/data_design.md``.

The conversion works on the string itself, never through ``float``, and it
refuses to round: an input with more decimals than the scale allows raises
``ValueError``, so a change in the venue's precision is noticed immediately
instead of silently corrupting the data.
"""

from __future__ import annotations

import re

# Optional sign, integer part, optional fractional part. No exponent: venues
# send plain decimals, and accepting "1e-3" would hide a format change.
_DECIMAL = re.compile(r"(-?)(\d+)(?:\.(\d+))?")


def to_scaled(text: str, scale: int) -> int:
    """Convert a decimal string to an integer scaled by ``10**scale``.

    >>> to_scaled("0.28", 4)
    2800
    >>> to_scaled("453.73", 6)
    453730000

    Raises ``ValueError`` if ``text`` is not a plain decimal or has more than
    ``scale`` decimals (trailing zeros beyond the scale are accepted, since
    they do not change the value).
    """
    if scale < 0:
        raise ValueError(f"scale must be >= 0, got {scale}")
    match = _DECIMAL.fullmatch(text)
    if match is None:
        raise ValueError(f"not a plain decimal: {text!r}")
    sign, whole, frac = match.groups()
    frac = (frac or "").rstrip("0")
    if len(frac) > scale:
        raise ValueError(f"{text!r} has more than {scale} decimals")
    value = int(whole) * 10**scale + int(frac.ljust(scale, "0") or "0")
    return -value if sign else value
