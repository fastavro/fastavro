import os

import pytest

from fastavro.six import MemoryIO
import fastavro


@pytest.mark.skipif(os.name == "nt", reason="A pain to set up on windows")
def test_snappy():
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

    file = MemoryIO()
    fastavro.writer(file, schema, records, codec="snappy")

    file.seek(0)
    out_records = list(fastavro.reader(file))
    assert records == out_records
