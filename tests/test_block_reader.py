
import fastavro
from fastavro.six import MemoryIO

schema = {
    "type": "record",
    "name": "test_block_iteration",
    "fields": [{
        "name": "nullable_str",
        "type": ["string", "null"]
    }, {
        "name": "str_field",
        "type": "string"
    }, {
        "name": "int_field",
        "type": "int"
    }
    ]
}


def make_records(num_records=2000):
    return [
        {
            "nullable_str": None if i % 3 == 0 else "%d-%d" % (i, i),
            "str_field": "%d %d %d" % (i, i, i),
            "int_field": i * 10
        }
        for i in range(num_records)
    ]


def make_blocks(num_records=2000, codec='null'):
    records = make_records(num_records)

    new_file = MemoryIO()
    fastavro.writer(new_file, schema, records, codec=codec)
    bytes = new_file.tell()
    new_file.seek(0)

    block_reader = fastavro.block_reader(new_file, schema)

    blocks = list(block_reader)

    return blocks, records, bytes


def check_block(block, num_bytes, num_records, records, codec, offset, size):
    block_records = list(block)

    assert len(block.bytes) == num_bytes
    assert len(block_records) == num_records
    assert block.codec == codec
    assert block.reader_schema == schema
    assert block.writer_schema == schema
    assert block.offset == offset
    assert block.size == size
    assert block_records == records


def test_block_iteration():

    blocks, records, bytes = make_blocks()

    assert bytes == 46007

    check_block(blocks[0], 16004, 811, records[       :811    ], 'null',   247, 16025)  # noqa
    check_block(blocks[1], 16016, 656, records[    811:811+656], 'null', 16272, 16037)  # noqa
    check_block(blocks[2], 13677, 533, records[811+656:       ], 'null', 32309, 13698)  # noqa


def test_block_iteration_deflated():

    blocks, records, bytes = make_blocks(codec='deflate')

    assert bytes == 16543

    check_block(blocks[0], 16004, 811, records[       :811    ], 'deflate',   250, 6242)  # noqa
    check_block(blocks[1], 16016, 656, records[    811:811+656], 'deflate',  6492, 5624)  # noqa
    check_block(blocks[2], 13677, 533, records[811+656:       ], 'deflate', 12116, 4427)  # noqa
