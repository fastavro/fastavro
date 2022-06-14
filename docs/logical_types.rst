Logical Types
=============

Fastavro supports the following official logical types:

* decimal
* uuid
* date
* time-millis
* time-micros
* timestamp-millis
* timestamp-micros
* local-timestamp-millis
* local-timestamp-micros

Fastavro is missing support for the following official logical types:

* duration

How to specify logical types in your schemas
--------------------------------------------

The docs say that when you want to make something a logical type, you just need
to add a `logicalType` key. Unfortunately, this means that a common confusion is
that people will take a pre-existing schema like this::

    schema = {
        "type": "record",
        "name": "root",
        "fields": [
            {
                "name": "id",
                "type": "string",
            },
        ]
    }

And then add the uuid logical type like this::

    schema = {
        "type": "record",
        "name": "root",
        "fields": [
            {
                "name": "id",
                "type": "string",
                "logicalType": "uuid",  # This is the wrong place to add this key
            },
        ]
    }

However, that adds the `logicalType` key to the `field` schema which is not
correct. Instead, we want to group it with the `string` like so::

    schema = {
        "type": "record",
        "name": "root",
        "fields": [
            {
                "name": "id",
                "type": {
                    "type": "string",
                    "logicalType": "uuid",  # This is the correct place to add this key
                },
            },
        ]
    }

Custom Logical Types
--------------------

The Avro specification defines a handful of logical types that most implementations support. For example, one of the defined logical types is a microsecond precision timestamp. The specification states that this value will get encoded as an avro `long` type.

For the sake of an example, let's say you want to create a new logical type for a microsecond precision timestamp that uses a string as the underlying avro type.

To do this, there are a few functions that need to be defined. First, we need an encoder function that will encode a datetime object as a string. The encoder function is called with two arguments: the data and the schema. So we could define this like so::

    def encode_datetime_as_string(data, schema):
        return datetime.isoformat(data)

    # or

    def encode_datetime_as_string(data, *args):
        return datetime.isoformat(data)

Then, we need a decoder function that will transform the string back into a datetime object. The decoder function is called with three arguments: the data, the writer schema, and the reader schema. So we could define this like so::

    def decode_string_as_datetime(data, writer_schema, reader_schema):
        return datetime.fromisoformat(data)

    # or

    def decode_string_as_datetime(data, *args):
        return datetime.fromisoformat(data)

Finally, we need to tell `fastavro` to use these functions. The schema for this custom logical type will use the type `string` and can use whatever name you would like as the `logicalType`. In this example, let's suppose we call the logicalType `datetime2`. To have the library actually use the custom logical type, we use the name of `<avro_type>-<logical_type>`, so in this example that name would be `string-datetime2` and then we add those functions like so::

    fastavro.write.LOGICAL_WRITERS["string-datetime2"] = encode_datetime_as_string
    fastavro.read.LOGICAL_READERS["string-datetime2"] = decode_string_as_datetime

And you are done. Now if the library comes across a schema with a logical type of `datetime2` and an avro type of `string`, it will use the custom functions. For a complete example, see here::

    import io
    from datetime import datetime

    import fastavro
    from fastavro import writer, reader


    def encode_datetime_as_string(data, *args):
        return datetime.isoformat(data)

    def decode_string_as_datetime(data, *args):
        return datetime.fromisoformat(data)

    fastavro.write.LOGICAL_WRITERS["string-datetime2"] = encode_datetime_as_string
    fastavro.read.LOGICAL_READERS["string-datetime2"] = decode_string_as_datetime


    writer_schema = fastavro.parse_schema({
        "type": "record",
        "name": "root",
        "fields": [
            {
                "name": "some_date",
                "type": [
                    "null",
                    {
                        "type": "string",
                        "logicalType": "datetime2",
                    },
                ],
            },
        ]
    })

    records = [
        {"some_date": datetime.now()}
    ]

    bio = io.BytesIO()

    writer(bio, writer_schema, records)

    bio.seek(0)

    for record in reader(bio):
        print(record)
