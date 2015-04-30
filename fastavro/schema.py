# cython: auto_cpdef=True


def acquaint_schema(schema):
    # TODO: Untangle this recursive dependency
    try:
        from ._reader import READERS, read_data
        from ._writer import CUSTOM_SCHEMAS, WRITERS, write_data
    except ImportError:
        from .reader import READERS, read_data
        from .writer import CUSTOM_SCHEMAS, WRITERS, write_data

    extract_named_schemas_into_repo(
        schema,
        READERS,
        lambda schema: lambda fo, _: read_data(fo, schema),
    )
    extract_named_schemas_into_repo(
        schema,
        WRITERS,
        lambda schema: lambda fo, datum, _: write_data(fo, datum, schema),
    )
    extract_named_schemas_into_repo(
        schema,
        CUSTOM_SCHEMAS,
        lambda schema: schema,
    )


def extract_record_type(schema):
    if isinstance(schema, dict):
        return schema['type']

    if isinstance(schema, list):
        return 'union'

    return schema


def schema_name(schema):
    name = schema.get('name')
    if not name:
        return
    namespace = schema.get('namespace')
    if not namespace:
        return name

    return namespace + '.' + name


def extract_named_schemas_into_repo(schema, repo, transformer):
    if type(schema) == list:
        for enum in schema:
            extract_named_schemas_into_repo(enum, repo, transformer)
        return

    if type(schema) != dict:
        return

    name = schema_name(schema)
    if name and (name not in repo):
        repo[name] = transformer(schema)

    for field in schema.get('fields', []):
        extract_named_schemas_into_repo(field['type'], repo, transformer)
