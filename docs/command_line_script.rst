fastavro command line script
============================

A command line script is installed with the library that can be used to dump
the contents of avro file(s) to the standard output.

Usage::

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

Examples
--------

Read an avro file::

    $ fastavro weather.avro

    {"temp": 0, "station": "011990-99999", "time": -619524000000}
    {"temp": 22, "station": "011990-99999", "time": -619506000000}
    {"temp": -11, "station": "011990-99999", "time": -619484400000}
    {"temp": 111, "station": "012650-99999", "time": -655531200000}
    {"temp": 78, "station": "012650-99999", "time": -655509600000}

Show the schema::

    $ fastavro --schema weather.avro

    {
     "type": "record",
     "namespace": "test",
     "doc": "A weather reading.",
     "fields": [
      {
       "type": "string",
       "name": "station"
      },
      {
       "type": "long",
       "name": "time"
      },
      {
       "type": "int",
       "name": "temp"
      }
     ],
     "name": "Weather"
    }
