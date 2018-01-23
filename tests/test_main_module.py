from os.path import join, abspath, dirname

import pytest
import fastavro
from fastavro.__main__ import main

data_dir = join(abspath(dirname(__file__)), 'avro-files')


def test_codecs():
    argv = ['fastavro', '--codecs']
    with pytest.raises(SystemExit):
        ret_val = main(argv)
        assert 'deflate' in ret_val
        assert 'null' in ret_val


def test_dump_with_ujson():
    import ujson as ujson
    fastavro.six._HAS_UJSON = True
    fastavro.six.json = ujson

    # without pretty option
    argv = ['fastavro', join(data_dir, 'weather.avro')]
    main(argv)

    # with pretty option
    argv = ['fastavro', '-p', join(data_dir, 'weather.avro')]
    main(argv)


def test_dump_with_standard_json():
    import json as stdjson
    fastavro.six._HAS_UJSON = False
    fastavro.six.json = stdjson

    # without pretty option
    argv = ['fastavro', join(data_dir, 'weather.avro')]
    main(argv)

    # with pretty option
    argv = ['fastavro', '-p', join(data_dir, 'weather.avro')]
    main(argv)
