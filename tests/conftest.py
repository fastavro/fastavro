import pytest

from fastavro.read import READERS
from fastavro.write import WRITERS
from fastavro._schema_common import SCHEMA_DEFS


@pytest.fixture(scope='function')
def clean_readers_writers_and_schemas():
    reader_keys = {key for key in READERS.keys()}
    writer_keys = {key for key in WRITERS.keys()}
    schema_keys = {key for key in SCHEMA_DEFS.keys()}

    yield

    repo_keys = (
        (READERS, reader_keys),
        (WRITERS, writer_keys),
        (SCHEMA_DEFS, schema_keys),
    )
    for repo, keys in repo_keys:
        diff = set(repo) - keys
        for key in diff:
            del repo[key]
