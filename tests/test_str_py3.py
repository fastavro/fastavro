from io import BytesIO
from os import SEEK_SET
from random import choice, seed
from string import ascii_uppercase, digits

import fastavro


def test_str_py3():
    letters = ascii_uppercase + digits
    id_size = 100

    seed("str_py3")  # Repeatable results

    def gen_id():
        return "".join(choice(letters) for _ in range(id_size))

    keys = ["first", "second", "third", "fourth"]

    testdata = [{key: gen_id() for key in keys} for _ in range(50)]

    schema = {
        "fields": [{"name": key, "type": "string"} for key in keys],
        "namespace": "namespace",
        "name": "zerobyte",
        "type": "record",
    }

    buf = BytesIO()
    fastavro.writer(buf, schema, testdata)

    buf.seek(0, SEEK_SET)
    for i, rec in enumerate(fastavro.reader(buf), 1):
        pass

    size = len(testdata)

    assert i == size, "bad number of records"
    assert rec == testdata[-1], "bad last record"


def test_py3_union_string_and_bytes():
    schema = {
        "fields": [{"name": "field", "type": ["string", "bytes"]}],
        "namespace": "namespace",
        "name": "union_string_bytes",
        "type": "record",
    }

    records = [{"field": "string"}, {"field": b"bytes"}]

    buf = BytesIO()
    fastavro.writer(buf, schema, records)
