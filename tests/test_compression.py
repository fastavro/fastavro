from io import BytesIO
import os

import pytest

import fastavro


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
@pytest.mark.skipif(os.name == "nt", reason="A pain to set up on windows")
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
@pytest.mark.skipif(os.name != "nt", reason="codec is present")
def test_optional_codecs_not_installed(codec):
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
    with pytest.raises(
        ValueError, match=f"{codec} codec is supported but you need to install"
    ):
        fastavro.writer(file, schema, records, codec=codec)


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


def test_compression_level():
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
    fastavro.writer(file, schema, records, codec="deflate", codec_compression_level=9)

    file.seek(0)
    out_records = list(fastavro.reader(file))
    assert records == out_records
