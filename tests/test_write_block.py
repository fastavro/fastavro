from io import BytesIO
import fastavro

import pytest

schema = {
    "type": "record",
    "name": "test_block_iteration",
    "fields": [
        {"name": "nullable_str", "type": ["string", "null"]},
        {"name": "str_field", "type": "string"},
        {"name": "int_field", "type": "int"},
    ],
}


def make_records(num_records=2000):
    return [
        {
            "nullable_str": None if i % 3 == 0 else f"{i}-{i}",
            "str_field": f"{i} {i} {i}",
            "int_field": i * 10,
        }
        for i in range(num_records)
    ]


def make_blocks(num_records=2000, codec="null"):
    records = make_records(num_records)

    new_file = BytesIO()
    fastavro.writer(new_file, schema, records, codec=codec)

    new_file.seek(0)
    block_reader = fastavro.block_reader(new_file, schema)

    blocks = list(block_reader)

    new_file.close()

    return blocks, records


@pytest.mark.parametrize(
    "source_codec,output_codec",
    [
        ("null", "null"),
        ("deflate", "deflate"),
        ("null", "deflate"),
        ("deflate", "null"),
    ],
)
def test_check_concatenate(source_codec, output_codec):
    blocks1, records1 = make_blocks(codec=source_codec)
    blocks2, records2 = make_blocks(codec=source_codec)

    new_file = BytesIO()
    w = fastavro.write.Writer(new_file, schema, codec=output_codec)
    for block in blocks1:
        w.write_block(block)
    for block in blocks2:
        w.write_block(block)

    # Read the file back to make sure we get back the same stuff
    new_file.seek(0)
    new_records = list(fastavro.reader(new_file, schema))
    assert new_records == records1 + records2
