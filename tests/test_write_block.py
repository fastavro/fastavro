import fastavro
from fastavro.six import MemoryIO

schema = {
    "type": "record",
    "name": "test_block_iteration",
    "fields": [
        {
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

    new_file.seek(0)
    block_reader = fastavro.block_reader(new_file, schema)

    blocks = list(block_reader)

    new_file.close()

    return blocks, records


def check_concatenate(source_codec='null', output_codec='null'):
    blocks1, records1 = make_blocks(codec=source_codec)
    blocks2, records2 = make_blocks(codec=source_codec)

    new_file = MemoryIO()
    w = fastavro.write.Writer(new_file, schema, codec=output_codec)
    for block in blocks1:
        w.write_block(block)
    for block in blocks2:
        w.write_block(block)

    # Read the file back to make sure we get back the same stuff
    new_file.seek(0)
    new_records = list(fastavro.reader(new_file, schema))
    assert new_records == records1 + records2


def test_block_concatenation():
    check_concatenate()


def test_block_concatenation_deflated():
    check_concatenate(source_codec='deflate', output_codec='deflate')


def test_block_concatenation_deflated_output():
    check_concatenate(source_codec='null', output_codec='deflate')
