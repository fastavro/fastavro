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

__all__ = [ 'iter_avro' ]
__version__ = '0.4.0'

try:
    from . import cfastavro as _avro
except ImportError:
    from . import pyfastavro as _avro

reader = iter_avro = _avro.iter_avro

