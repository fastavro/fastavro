from collections import namedtuple
import json


class LogicalTypeValidationErrorData(
    namedtuple("LogicalTypeValidationErrorData", ["datum", "schema"])
):
    def __str__(self):
        return (
            f"<{self.datum}> is not valid for logical type in schema" + f"{self.schema}"
        )


class LogicalTypeValidationError(Exception):
    def __init__(self, *errors):
        message = json.dumps([str(e) for e in errors], indent=2, ensure_ascii=False)
        super().__init__(message)
        self.errors = errors
