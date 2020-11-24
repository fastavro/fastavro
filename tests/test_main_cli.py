# Some basic unit and integration tests for the 'fastavro' CLI
#
# std imports
import os
import sys
import json
import subprocess

data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "avro-files")

main_py = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), os.pardir, "fastavro", "__main__.py"
)


def test_cli_record_output():
    # given,
    given_avro_input = os.path.join(data_dir, "weather.avro")
    given_cmd_args = [sys.executable, main_py, given_avro_input]
    expected_data = [
        {"station": "011990-99999", "time": -619524000000, "temp": 0},
        {"station": "011990-99999", "time": -619506000000, "temp": 22},
        {"station": "011990-99999", "time": -619484400000, "temp": -11},
        {"station": "012650-99999", "time": -655531200000, "temp": 111},
        {"station": "012650-99999", "time": -655509600000, "temp": 78},
    ]

    # exercise,
    result_output = subprocess.check_output(given_cmd_args).decode().splitlines()
    data = [json.loads(result_line_out) for result_line_out in result_output]

    # verify
    assert data == expected_data


def test_cli_stream_input():
    # given,
    given_avro_input = os.path.join(data_dir, "weather.avro")
    given_stdin_stream = open(given_avro_input, "rb")
    given_cmd_args = [sys.executable, main_py, "-"]
    expected_data = [
        {"station": "011990-99999", "time": -619524000000, "temp": 0},
        {"station": "011990-99999", "time": -619506000000, "temp": 22},
        {"station": "011990-99999", "time": -619484400000, "temp": -11},
        {"station": "012650-99999", "time": -655531200000, "temp": 111},
        {"station": "012650-99999", "time": -655509600000, "temp": 78},
    ]

    # exercise,
    result_output = (
        subprocess.check_output(given_cmd_args, stdin=given_stdin_stream)
        .decode()
        .splitlines()
    )
    data = [json.loads(result_line_out) for result_line_out in result_output]

    # verify
    assert data == expected_data


def test_cli_arg_metadata():
    # given,
    given_avro_input = os.path.join(data_dir, "testDataFileMeta.avro")
    given_cmd_args = [sys.executable, main_py, "--metadata", given_avro_input]
    expected_metadata = {"hello": "bar"}

    # exercise,
    result_output = subprocess.check_output(given_cmd_args).decode()
    data = json.loads(result_output)

    # verify
    assert data == expected_metadata


def test_cli_arg_schema():
    # given,
    given_avro_input = os.path.join(data_dir, "weather.avro")
    given_cmd_args = [sys.executable, main_py, "--schema", given_avro_input]
    expected_schema = {
        "type": "record",
        "name": "Weather",
        "namespace": "test",
        "fields": [
            {"name": "station", "type": "string"},
            {"name": "time", "type": "long"},
            {"name": "temp", "type": "int"},
        ],
        "doc": "A weather reading.",
    }

    # exercise,
    result_output = subprocess.check_output(given_cmd_args).decode()
    data = json.loads(result_output)

    # verify
    assert data == expected_schema


def test_cli_arg_codecs():
    # given,
    given_cmd_args = [sys.executable, main_py, "--codecs"]
    default_codecs = ("deflate", "null")

    # exercise,
    result_output = subprocess.check_output(given_cmd_args).decode()
    result_codecs = [
        line.strip() for line in result_output.splitlines() if line.strip()
    ]

    for codec in default_codecs:
        assert codec in result_codecs
