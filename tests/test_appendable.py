import sys
import pytest
from fastavro._write_common import _is_appendable


def test_appendable_raises_valuerror(tmpdir):
    """_is_appendable() raises ValueError when file is only 'a' mode."""
    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "a") as new_file:
        new_file.write("this phrase forwards cursor position beyond zero")
        with pytest.raises(ValueError, match=r"you must use the 'a\+' mode"):
            _is_appendable(new_file)


def test_appendable_true_nonzero(tmpdir):
    """_is_appendable() returns True when file_like.tell() is non-zero."""
    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "a+b") as new_file:
        new_file.write(b"this phrase forwards cursor position beyond zero")
        assert _is_appendable(new_file)


def test_appendable_false_zero():
    """_is_appendable() returns True when file_like.tell() returns 0."""

    class MockFileLike:
        def seekable(self):
            return True

        def tell(self):
            # mock a 0 position
            return 0

    assert not _is_appendable(MockFileLike())


def test_appendable_false_unseekable_stream():
    """File streams that cannot seek return False."""

    class MockStreamLike:
        # This mock file-like object simply returns False for 'seekable', and,
        # if 'tell' were called, raises OSError. This mimics a streaming
        # buffer like sys.stdout.buffer without actually using it.
        def seekable(self):
            return False

        def tell(self):
            # mock what a write-only stream would do
            raise OSError(29, "Illegal seek")

    assert not _is_appendable(MockStreamLike())


def test_appendable_false_stdout(capfd):
    """_is_appendable() returns False when file_like is sys.stdout.buffer."""
    # normally, pytest performs "Capturing of stderr/stdout", which is pretty
    # great, but it impacts this "integration test": we'd like to use our true
    # stdout, whether it is a terminal or pipe, to invoke the true behavior of
    # _is_appendable() when used with 'sys.stdout.buffer'.
    with capfd.disabled():
        assert not _is_appendable(sys.stdout.buffer)
