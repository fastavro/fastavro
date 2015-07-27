# cython: auto_cpdef=True


class UnknownType(Exception):
    def __init__(self, fullname):
        super(UnknownType, self).__init__(fullname)
        self.fullname = fullname


def acquaint_schema(schema):
    # TODO: Untangle this recursive dependency
    try:
        from ._reader import READERS, read_data
        from ._writer import SCHEMA_DEFS, WRITERS, write_data
    except ImportError:
        from .reader import READERS, read_data
        from .writer import SCHEMA_DEFS, WRITERS, write_data

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
        SCHEMA_DEFS,
        lambda schema: schema,
    )


def extract_record_type(schema):
    if isinstance(schema, dict):
        return schema['type']

    if isinstance(schema, list):
        return 'union'

    return schema


def schema_name(schema, parent_namespace):
    name = schema.get('name')
    if not name:
        return (None, None, None)

    namespace = schema.get('namespace', parent_namespace)
    if not namespace:
        return (namespace, name, name)

    return (
        namespace,
        name,
        namespace + '.' + name,
    )


def extract_named_schemas_into_repo(
    schema,
    repo,
    transformer,
    parent_namespace=None,
):
    if type(schema) == list:
        for enum in schema:
            extract_named_schemas_into_repo(
                enum,
                repo,
                transformer,
                parent_namespace,
            )
        return

    if type(schema) != dict:
        if schema not in repo:
            raise UnknownType(schema)
        return

    namespace, _, fullname = schema_name(schema, parent_namespace)

    if fullname and (fullname not in repo):
        repo[fullname] = transformer(schema)

    for field in schema.get('fields', []):
        extract_named_schemas_into_repo(
            field['type'],
            repo,
            transformer,
            namespace,
        )
