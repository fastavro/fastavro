fastavro
========

The current Python `avro` package is packed with features but dog slow.

On a test case of about 10K records, it takes about 14sec to iterate over all of
them. In comparison the JAVA `avro` SDK does it in about 1.9sec.

`fastavro` is less feature complete than `avro`, however it's much faster. It
iterates over the same 10K records in 2.9sec, and if you use it with PyPy it'll
do it in 1.5sec (to be fair, the JAVA benchmark is doing some extra JSON
encoding/decoding).

Usage
=====

::

    from fastavro import iter_avro

    with open('some-file.avro', 'rb') as fo:
        records = iter_avro(fo)
        schema = records.schema

        for record in avro:
            process_record(record)

Limitations
===========
* Support only iteration
    - No writing for you!
* Supports only `null` and `deflate` codecs
    - `avro` also supports `snappy`
* No reader schema

