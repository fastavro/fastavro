"""Tests for BinaryDecoder handling of truncated input."""

import io
import struct

import pytest

from fastavro.io.binary_decoder import BinaryDecoder


def _decoder(data):
    return BinaryDecoder(io.BytesIO(data))


def test_read_long_truncated_varint_raises_eoferror():
    # A varint whose continuation bit is set but which ends early must raise
    # EOFError, not a TypeError from ord(b"").
    for data in (b"\x80", b"\xff\xff", b"\x81\x82"):
        with pytest.raises(EOFError):
            _decoder(data).read_long()


def test_read_primitives_truncated_raise_eoferror():
    with pytest.raises(EOFError):
        _decoder(b"").read_boolean()
    with pytest.raises(EOFError):
        _decoder(b"\x00\x00").read_float()
    with pytest.raises(EOFError):
        _decoder(b"\x00" * 3).read_double()


def test_read_primitives_valid():
    assert _decoder(b"\x01").read_boolean() is True
    assert _decoder(b"\x96\x01").read_long() == 75
    assert _decoder(struct.pack("<f", 1.5)).read_float() == 1.5
    assert _decoder(struct.pack("<d", 2.5)).read_double() == 2.5
