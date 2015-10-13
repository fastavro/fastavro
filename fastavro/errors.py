from .util import path_string


class UnknownType(Exception):
    def __init__(self, name):
        super(UnknownType, self).__init__(name)
        self.name = name


class AvroValueError(ValueError):
    pass

    @classmethod
    def create(cls, reading_or_writing, path, traceback):
        message = '\n'.join([
            "Error encountered %s avro message." % reading_or_writing,
            "Path was: %s" % path_string(path),
            "Exception traceback: %s" % traceback
        ])
        return cls(message)


class SchemaResolutionError(Exception):
    pass
