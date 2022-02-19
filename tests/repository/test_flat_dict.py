from os.path import join, abspath, dirname
import pytest
from fastavro.repository.base import SchemaRepositoryError
from fastavro.repository.flat_dict import FlatDictRepository


def test_load_returns_parsed_value():
    repo_dir = join(abspath(dirname(__file__)), "flat_dict_test")
    repo = FlatDictRepository(repo_dir)

    result = repo.load("Valid")
    expected = {
        "name": "Valid",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}],
    }
    assert expected == result


def test_load_raises_error_if_file_does_not_exist():
    repo_dir = join(abspath(dirname(__file__)), "flat_dict_test")
    repo = FlatDictRepository(repo_dir)

    with pytest.raises(SchemaRepositoryError) as err:
        repo.load("MissingFile")

    assert str(err.value) == "Failed to load 'MissingFile' schema"


def test_load_raises_error_if_file_is_not_valid_json():
    repo_dir = join(abspath(dirname(__file__)), "flat_dict_test")
    repo = FlatDictRepository(repo_dir)

    with pytest.raises(SchemaRepositoryError) as err:
        repo.load("InvalidJson")

    assert str(err.value) == "Failed to parse 'InvalidJson' schema"
