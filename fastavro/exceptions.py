class AvroValueError(ValueError):
    @classmethod
    def from_traceback(cls, datum, schema, path, traceback):
        msg = '\n'.join([
            'Exception traceback: %s' % traceback,
            'Path is: %s' % '.'.join(path),
            'Datum is %s of type %s' % (datum, type(datum)),
            'Expected avro schema type: %s' % schema,
        ])
        return cls(msg)
