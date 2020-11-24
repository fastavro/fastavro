from collections import namedtuple
import json


class ValidationErrorData(
    namedtuple("ValidationErrorData", ["datum", "schema", "field"])
):
    def __str__(self):
        # update field for prettier printing
        field = self.field
        if field is None:
            field = ""

        if self.datum is None:
            return f"Field({field}) is None expected {self.schema}"

        return (
            f"{field} is <{self.datum}> of type "
            + f"{type(self.datum)} expected {self.schema}"
        )


class ValidationError(Exception):
    def __init__(self, *errors):
        message = json.dumps([str(e) for e in errors], indent=2, ensure_ascii=False)
        super(ValidationError, self).__init__(message)
        self.errors = errors
