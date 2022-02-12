import json
from os import path

from .base import AbstractSchemaRepository, SchemaRepositoryError


FILE_EXT = "avsc"


class FlatDictRepository(AbstractSchemaRepository):
    def __init__(self, path):
        self.path = path

    def load(self, name):
        file_path = path.join(self.path, f"{name}.{FILE_EXT}")
        try:
            with open(file_path) as schema_file:
                return json.load(schema_file)
        except IOError as error:
            raise SchemaRepositoryError(
                f"Failed to load '{name}' schema",
            ) from error
