'''Fast Avro file iteration.

Most of the code here is ripped off the Python avro package. It's missing a lot
of features in order to get speed.

The only onterface function is iter_avro, example usage::

    import fastavro as avro

    with open('some-file.avro', 'rb') as fo:
        reader = fastavro.reader(fo)
        schema = reader.schema

        for record in reader:
            process_record(record)
'''

__all__ = [ 'iter_avro', 'reader']
__version__ = '0.6.9'


try:
    from . import _reader
except ImportError as e:
    from . import reader as _reader

reader = iter_avro = _reader.iter_avro

