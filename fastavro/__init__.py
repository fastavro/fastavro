'''Fast Avro file iteration.

Most of the code here is ripped off the Python avro package. It's missing a lot
of features in order to get speed.

The only onterface function is iter_avro, example usage::

    from fastavro import iter_avro

    with open('some-file.avro', 'rb') as fo:
        avro = iter_avro(fo)
        schema = avro.schema

        for record in avro:
            process_record(record)
'''

__all__ = [ 'iter_avro' ]
__version__ = '0.3.0'

try:
    from . import cfastavro as _avro
except ImportError:
    from . import pyfastavro as _avro

iter_avro = _avro.iter_avro

