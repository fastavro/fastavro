# fastavro
[![Build Status](https://travis-ci.org/tebeka/fastavro.svg?branch=master)](https://travis-ci.org/tebeka/fastavro)

Because the Apache Python `avro` package is written in pure Python, it is
relatively slow. In one test case, it takes about 14 seconds to iterate through
a file of 10,000. By comparison, the JAVA `avro` SDK reads the same file in
1.9 seconds.

The `fastavro` library was written to offer performance comparable to the Java
library. With regular CPython, `fastavro` uses C extensions which allow it to
iterate the same 10,000 record file in 1.7 seconds. With PyPy, this drops to 1.5
seconds (to be fair, the JAVA benchmark is doing some extra JSON
encoding/decoding).

`fastavro` supports the following Python versions:

* Python 2.7
* Python 3.4
* Python 3.5
* Python 3.6
* PyPy
* PyPy3

[Cython]: http://cython.org/

# Usage

## Reading


```python
import fastavro as avro

with open('weather.avro', 'rb') as fo:
    reader = avro.reader(fo)
    schema = reader.schema

    for record in reader:
        process_record(record)
```

You may also explicitly specify reader schema to perform schema validation:

```python
import fastavro as avro

schema = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
    ],
}


with open('weather.avro', 'rb') as fo:
    reader = avro.reader(fo, reader_schema=schema)

    # will raise a fastavro.reader.SchemaResolutionError in case of
    # incompatible schema
    for record in reader:
        process_record(record)
```

## Writing

```python
from fastavro import writer

schema = {
    'doc': 'A weather reading.',
    'name': 'Weather',
    'namespace': 'test',
    'type': 'record',
    'fields': [
        {'name': 'station', 'type': 'string'},
        {'name': 'time', 'type': 'long'},
        {'name': 'temp', 'type': 'int'},
    ],
}

# 'records' can be any iterable (including a generator)
records = [
    {u'station': u'011990-99999', u'temp': 0, u'time': 1433269388},
    {u'station': u'011990-99999', u'temp': 22, u'time': 1433270389},
    {u'station': u'011990-99999', u'temp': -11, u'time': 1433273379},
    {u'station': u'012650-99999', u'temp': 111, u'time': 1433275478},
]

with open('weather.avro', 'wb') as out:
    writer(out, schema, records)
```

You can also use the `fastavro` script from the command line to dump `avro`
files.

    fastavro weather.avro

By default fastavro prints one JSON object per line, you can use the `--pretty`
flag to change this.

You can also dump the avro schema

    fastavro --schema weather.avro


Here's the full command line help

    usage: fastavro [-h] [--schema] [--codecs] [--version] [-p] [file [file ...]]

    iter over avro file, emit records as JSON

    positional arguments:
      file          file(s) to parse

    optional arguments:
      -h, --help    show this help message and exit
      --schema      dump schema instead of records
      --codecs      print supported codecs
      --version     show program's version number and exit
      -p, --pretty  pretty print json

# Installing
`fastavro` is available both on [PyPi](http://pypi.python.org/pypi)

    pip install fastavro

and on [conda-forge](https://conda-forge.github.io) `conda` channel.

    conda install -c conda-forge fastavro

# Hacking

As recommended by Cython, the C files output is distributed. This has the
advantage that the end user does not need to have Cython installed. However it
means that every time you change `fastavro/pyfastavro.py` you need to run
`make`.

For `make` to succeed you need both python and Python 3 installed, Cython on both
of them. For `./test-install.sh` you'll need [virtualenv][venv].

[venv]: http://pypi.python.org/pypi/virtualenv

### Releasing

We release both to [pypi][pypi] and to [conda-forge][conda-forge].

We assume you have [twine][twine] installed and that you've created your own
fork of [fastavro-feedstock][feedstock].

* Make sure the tests pass
* Copy the windows build artifacts for the new version from
  https://ci.appveyor.com/project/scottbelden/fastavro to the `dist` folder
* Run `make publish`
* Note the sha signature emitted at the above
* Switch to feedstock directory and edit `recipe/meta.yaml`
    - Update `version` and `sha256` variables at the top of the file
    - Run `python recipe/test_recipe.py`
    - Submit a [PR][pr]

[conda-forge]: https://conda-forge.org/
[feedstock]: https://github.com/conda-forge/fastavro-feedstock
[pr]: https://conda-forge.org/#update_recipe
[pypi]: https://pypi.python.org/pypi
[twine]: https://pypi.python.org/pypi/twine


# Changes

See the [ChangeLog]

[ChangeLog]: https://github.com/tebeka/fastavro/blob/master/ChangeLog

# Contact

[Project Home](https://github.com/tebeka/fastavro)
