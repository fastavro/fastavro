fastavro.write
==============

.. autofunction:: fastavro._write_py.writer

.. autofunction:: fastavro._write_py.schemaless_writer

Using the tuple notation to specify which branch of a union to take
-------------------------------------------------------------------

Since this library uses plain dictionaries to represent a record, it is
possible for that dictionary to fit the definition of two different records.

For example, given a dictionary like this::

    {"name": "My Name"}

It would be valid against both of these records::

    child_schema = {
        "name": "Child",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_color", "type": ["null", "string"]},
        ]
    }

    pet_schema = {
        "name": "Pet",
        "type": "record",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "favorite_toy", "type": ["null", "string"]},
        ]
    }

This becomes a problem when a schema contains a union of these two similar
records as it is not clear which record the dictionary represents. For example,
if you used the previous dictionary with the following schema, it wouldn't be
clear if the record should be serialized as a `Child` or a `Pet`::

    household_schema = {
        "name": "Household",
        "type": "record",
        "fields": [
            {"name": "address", "type": "string"},
            {
                "name": "family_members",
                "type": {
                    "type": "array", "items": [
                        {
                            "name": "Child",
                            "type": "record",
                            "fields": [
                                {"name": "name", "type": "string"},
                                {"name": "favorite_color", "type": ["null", "string"]},
                            ]
                        }, {
                            "name": "Pet",
                            "type": "record",
                            "fields": [
                                {"name": "name", "type": "string"},
                                {"name": "favorite_toy", "type": ["null", "string"]},
                            ]
                        }
                    ]
                }
            },
        ]
    }

To resolve this, you can use a tuple notation where the first value of the
tuple is the fully namespaced record name and the second value is the
dictionary. For example::

    records = [
        {
            "address": "123 Drive Street",
            "family_members": [
                ("Child", {"name": "Son"}),
                ("Child", {"name": "Daughter"}),
                ("Pet", {"name": "Dog"}),
            ]
        }
    ]

Using the record hint to specify which branch of a union to take
----------------------------------------------------------------

In addition to the tuple notation for specifying the name of a record, you can
also include a special `-type` attribute (note that this attribute is `-type`,
not `type`) on a record to do the same thing. So the example above which looked
like this::

    records = [
        {
            "address": "123 Drive Street",
            "family_members": [
                ("Child", {"name": "Son"}),
                ("Child", {"name": "Daughter"}),
                ("Pet", {"name": "Dog"}),
            ]
        }
    ]

Would now look like this::

    records = [
        {
            "address": "123 Drive Street",
            "family_members": [
                {"-type": "Child", "name": "Son"},
                {"-type": "Child", "name": "Daughter"},
                {"-type": "Pet", "name": "Dog"},
            ]
        }
    ]

Unlike the tuple notation which can be used with any avro type in a union, this
`-type` hint can only be used with records. However, this can be useful if you
want to make a single record dictionary that can be used both in and out of
unions.
