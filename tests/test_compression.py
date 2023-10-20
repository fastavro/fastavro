import builtins
from importlib import reload
from io import BytesIO
import os
import sys
from types import ModuleType

import pytest

import fastavro

from .conftest import is_testing_cython_modules


@pytest.mark.parametrize("codec", ["null", "deflate", "bzip2", "xz"])
def test_builtin_codecs(codec):
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": 1433270389},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111, "time": 1433275478},
    ]

    file = BytesIO()
    fastavro.writer(file, schema, records, codec=codec)

    file.seek(0)
    out_records = list(fastavro.reader(file))
    assert records == out_records


@pytest.mark.parametrize("codec", ["snappy", "zstandard", "lz4"])
@pytest.mark.skipif(os.name == "nt", reason="A pain to install codecs on windows")
def test_optional_codecs(codec):
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": 1433270389},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111, "time": 1433275478},
    ]

    file = BytesIO()
    fastavro.writer(file, schema, records, codec=codec)

    file.seek(0)
    out_records = list(fastavro.reader(file))
    assert records == out_records


@pytest.mark.parametrize("codec", ["snappy", "zstandard", "lz4"])
@pytest.mark.skipif(
    is_testing_cython_modules(),
    reason="difficult to monkeypatch builtins on cython compiled code",
)
@pytest.mark.skipif(os.name == "nt", reason="A pain to install codecs on windows")
def test_optional_codecs_not_installed_writing(monkeypatch, codec):
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": 1433270389},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111, "time": 1433275478},
    ]

    file = BytesIO()
    orig_import = __import__
    imports = {"snappy", "zstandard", "lz4.block", "cramjam"}

    def import_blocker(name, *args, **kwargs):
        if name in imports:
            raise ImportError()
        else:
            return orig_import(name, *args, **kwargs)

    with monkeypatch.context() as ctx:
        ctx.setattr(builtins, "__import__", import_blocker)
        for name in imports:
            ctx.delitem(sys.modules, name, raising=False)

        # Reload the module to have it update the BLOCK_WRITERS
        reload(fastavro._write_py)

    with pytest.raises(
        ValueError, match=f"{codec} codec is supported but you need to install"
    ):
        fastavro.writer(file, schema, records, codec=codec)

    # Reload again to get back to normal
    reload(fastavro._write_py)


@pytest.mark.parametrize("codec", ["snappy", "zstandard", "lz4"])
@pytest.mark.skipif(
    is_testing_cython_modules(),
    reason="difficult to monkeypatch builtins on cython compiled code",
)
@pytest.mark.skipif(os.name == "nt", reason="A pain to install codecs on windows")
def test_optional_codecs_not_installed_reading(monkeypatch, codec):
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": 1433270389},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111, "time": 1433275478},
    ]

    file = BytesIO()
    fastavro.writer(file, schema, records, codec=codec)
    file.seek(0)

    orig_import = __import__
    imports = {"snappy", "zstandard", "lz4.block", "cramjam"}

    def import_blocker(name, *args, **kwargs):
        if name in imports:
            raise ImportError()
        else:
            return orig_import(name, *args, **kwargs)

    with monkeypatch.context() as ctx:
        ctx.setattr(builtins, "__import__", import_blocker)
        for name in imports:
            ctx.delitem(sys.modules, name, raising=False)

        # Reload the module to have it update the BLOCK_READERS
        reload(fastavro._read_py)

    with pytest.raises(
        ValueError, match=f"{codec} codec is supported but you need to install"
    ):
        list(fastavro.reader(file))

    # Reload again to get back to normal
    reload(fastavro._read_py)


@pytest.mark.skipif(
    is_testing_cython_modules(),
    reason="difficult to monkeypatch builtins on cython compiled code",
)
def test_write_snappy_without_cramjam_gives_deprecation(monkeypatch):
    orig_import = __import__

    def import_blocker(name, *args, **kwargs):
        if name == "cramjam":
            raise ImportError()
        else:
            return orig_import(name, *args, **kwargs)

    with monkeypatch.context() as ctx:
        ctx.setattr(builtins, "__import__", import_blocker)
        ctx.delitem(sys.modules, "cramjam", raising=False)

        # Ensure that a snappy-like module exists
        mod = ModuleType("snappy")
        exec("compress = None", mod.__dict__)
        ctx.setitem(sys.modules, "snappy", mod)

        # Reload the module to have it update the BLOCK_WRITERS
        with pytest.deprecated_call():
            reload(fastavro._write_py)

    # Reload again to get back to normal
    reload(fastavro._write_py)


@pytest.mark.skipif(
    is_testing_cython_modules(),
    reason="difficult to monkeypatch builtins on cython compiled code",
)
def test_read_snappy_without_cramjam_gives_deprecation(monkeypatch):
    orig_import = __import__

    def import_blocker(name, *args, **kwargs):
        if name == "cramjam":
            raise ImportError()
        else:
            return orig_import(name, *args, **kwargs)

    with monkeypatch.context() as ctx:
        ctx.setattr(builtins, "__import__", import_blocker)
        ctx.delitem(sys.modules, "cramjam", raising=False)

        # Ensure that a snappy-like module exists
        mod = ModuleType("snappy")
        exec("decompress = None", mod.__dict__)
        ctx.setitem(sys.modules, "snappy", mod)

        # Reload the module to have it update the BLOCK_READERS
        with pytest.deprecated_call():
            reload(fastavro._read_py)

    # Reload again to get back to normal
    reload(fastavro._read_py)


def test_unsupported_codec():
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": 1433270389},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111, "time": 1433275478},
    ]

    file = BytesIO()
    with pytest.raises(ValueError, match="unrecognized codec"):
        fastavro.writer(file, schema, records, codec="unsupported")

    file = BytesIO()
    fastavro.writer(file, schema, records, codec="deflate")

    # Change the avro binary to act as if it were written with a codec called
    # `unsupported`
    modified_avro = file.getvalue().replace(b"\x0edeflate", b"\x16unsupported")
    modified_file = BytesIO(modified_avro)

    with pytest.raises(ValueError, match="Unrecognized codec"):
        list(fastavro.reader(modified_file))


@pytest.mark.parametrize("codec", ["deflate", "zstandard"])
@pytest.mark.skipif(os.name == "nt", reason="A pain to install codecs on windows")
def test_compression_level(codec):
    """https://github.com/fastavro/fastavro/issues/377"""
    schema = {
        "doc": "A weather reading.",
        "name": "Weather",
        "namespace": "test",
        "type": "record",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
    }

    records = [
        {"station": "011990-99999", "temp": 0, "time": 1433269388},
        {"station": "011990-99999", "temp": 22, "time": 1433270389},
        {"station": "011990-99999", "temp": -11, "time": 1433273379},
        {"station": "012650-99999", "temp": 111, "time": 1433275478},
    ]

    file = BytesIO()
    fastavro.writer(file, schema, records, codec=codec, codec_compression_level=9)

    file.seek(0)
    out_records = list(fastavro.reader(file))
    assert records == out_records


@pytest.mark.skipif(os.name == "nt", reason="A pain to install codecs on windows")
def test_zstandard_decompress_stream():
    """https://github.com/fastavro/fastavro/pull/575"""
    binary = (
        b'Obj\x01\x04\x14avro.codec\x12zstandard\x16avro.schema\xc6\x01{"name"'
        + b':"Weather","namespace":"test","type":"record","fields":[{"name":"s'
        + b'tation","type":"string"}]}\x001234567890123456\x02\x1c(\xb5/\xfd\x00'
        + b"X)\x00\x00\x08AAAA1234567890123456"
    )

    file = BytesIO(binary)
    out_records = list(fastavro.reader(file))
    assert [{"station": "AAAA"}] == out_records
