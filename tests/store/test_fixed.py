import pytest

from rl_binary_mm.store.fixed import to_scaled


@pytest.mark.parametrize(
    ("text", "scale", "expected"),
    [
        ("0.28", 4, 2800),
        ("0.1", 4, 1000),
        ("0.001", 4, 10),
        ("0.999", 4, 9990),
        ("1", 4, 10000),
        ("0", 4, 0),
        ("453.73", 6, 453_730_000),
        ("96490.44", 6, 96_490_440_000),
        ("6", 6, 6_000_000),
        ("0.123456", 6, 123_456),
        ("0.10", 1, 1),  # trailing zeros beyond the scale do not change the value
        ("-0.5", 4, -5000),
        ("42", 0, 42),
    ],
)
def test_converts_exactly(text: str, scale: int, expected: int) -> None:
    assert to_scaled(text, scale) == expected


@pytest.mark.parametrize(("text", "scale"), [("0.00001", 4), ("0.1234567", 6), ("0.5", 0)])
def test_refuses_to_round(text: str, scale: int) -> None:
    with pytest.raises(ValueError, match="more than"):
        to_scaled(text, scale)


@pytest.mark.parametrize("text", ["", ".5", "5.", "1e-3", "abc", " 1", "1,5", "+1", "nan"])
def test_rejects_anything_but_a_plain_decimal(text: str) -> None:
    with pytest.raises(ValueError, match="plain decimal"):
        to_scaled(text, 4)


def test_rejects_negative_scale() -> None:
    with pytest.raises(ValueError, match="scale"):
        to_scaled("1", -1)
