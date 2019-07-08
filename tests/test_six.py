import sys
import pytest
from fastavro.six import appendable


def test_appendable_raises_valuerror(tmpdir):
    """six.appendable() raises ValueError when file is only 'a' mode."""
    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "a") as new_file:
        new_file.write('this phrase forwards cursor position beyond zero')
        with pytest.raises(ValueError, match=r"you must use the 'a\+' mode"):
            appendable(new_file)


def test_appendable_true_nonzero(tmpdir):
    """six.appendable() returns True when file_like.tell() is non-zero."""
    test_file = str(tmpdir.join("test.avro"))

    with open(test_file, "a+b") as new_file:
        new_file.write(b'this phrase forwards cursor position beyond zero')
        assert appendable(new_file)


def test_appendable_false_zero():
    """six.appendable() returns True when file_like.tell() returns 0."""
    class MockFileLike:
        def seekable(self):
            return True

        def tell(self):
            # mock a 0 position
            return 0

    assert not appendable(MockFileLike())


def test_appendable_false_unseekable_stream():
    """File streams that cannot seek return False."""

    class MockStreamLike:
        # This mock file-like object simply returns False for 'seekable', and,
        # if 'tell' were called, rasies OSError. This mimicks a streaming
        # buffer like sys.stdout.buffer without actually using it.
        def seekable(self):
            return False

        def tell(self):
            # mock what a write-only stream would do
            raise OSError(29, "Illegal seek")

    assert not appendable(MockStreamLike())


def test_appendable_false_stdout(capfd):
    """six.appendable() returns False when file_like is sys.stdout.buffer."""
    # normally, pytest performs "Capturing of stderr/stdout", which is pretty
    # great, but it impacts this "integration test": we'd like to use our true
    # stdout, whether it is a terminal or pipe, to invoke the true behavior of
    # appendable() when used with 'sys.stdout.buffer'.
    with capfd.disabled():
        if sys.version_info >= (3, 0):
            assert not appendable(sys.stdout.buffer)
        else:
            assert not appendable(sys.stdout)
