# cython: auto_cpdef=True

from os import path

try:
    import ujson as json
except ImportError:
    import json

from ._schema_common import PRIMITIVES, SCHEMA_DEFS, UnknownType


def extract_record_type(schema):
    if isinstance(schema, dict):
        return schema['type']

    if isinstance(schema, list):
        return 'union'

    return schema


def extract_logical_type(schema):
    if not isinstance(schema, dict):
        return None
    d_schema = schema
    rt = d_schema['type']
    lt = d_schema.get('logicalType')
    if lt:
        # TODO: Building this string every time is going to be relatively slow.
        return '{}-{}'.format(rt, lt)
    return None


def schema_name(schema, parent_ns):
    name = schema.get('name')
    if not name:
        return parent_ns, None

    namespace = schema.get('namespace', parent_ns)
    if not namespace:
        return namespace, name

    return namespace, '%s.%s' % (namespace, name)


def extract_named_schemas_into_repo(schema, repo, transformer, parent_ns=None):
    if type(schema) == list:
        for index, enum_schema in enumerate(schema):
            namespaced_name = extract_named_schemas_into_repo(
                enum_schema,
                repo,
                transformer,
                parent_ns,
            )
            if namespaced_name:
                schema[index] = namespaced_name
        return

    if type(schema) != dict:
        # If a reference to another schema is an unqualified name, but not one
        # of the primitive types, then we should add the current enclosing
        # namespace to reference name.
        if schema not in PRIMITIVES and '.' not in schema and parent_ns:
            schema = parent_ns + '.' + schema

        if schema not in repo:
            raise UnknownType(schema)
        return schema

    namespace, name = schema_name(schema, parent_ns)

    if name:
        repo[name] = transformer(schema)

    schema_type = schema.get('type')
    if schema_type == 'array':
        namespaced_name = extract_named_schemas_into_repo(
            schema['items'],
            repo,
            transformer,
            namespace,
        )
        if namespaced_name:
            schema['items'] = namespaced_name
        return
    if schema_type == 'map':
        namespaced_name = extract_named_schemas_into_repo(
            schema['values'],
            repo,
            transformer,
            namespace,
        )
        if namespaced_name:
            schema['values'] = namespaced_name
        return
    # Normal record.
    for field in schema.get('fields', []):
        namespaced_name = extract_named_schemas_into_repo(
            field['type'],
            repo,
            transformer,
            namespace,
        )
        if namespaced_name:
            field['type'] = namespaced_name


def load_schema(schema_path):
    '''
    Returns a schema loaded from the file at `schema_path`.

    Will recursively load referenced schemas assuming they can be found in
    files in the same directory and named with the convention
    `<type_name>.avsc`.
    '''
    with open(schema_path) as fd:
        schema = json.load(fd)
    schema_dir, schema_file = path.split(schema_path)
    return _load_schema(schema, schema_dir)


def _reader():
    # FIXME: This is due to circular depedency, find a better way
    try:
        from . import _reader as reader
    except ImportError:
        from . import reader

    return reader


def _load_schema(schema, schema_dir):
    try:
        _reader().acquaint_schema(schema)
    except UnknownType as e:
        try:
            avsc = path.join(schema_dir, '%s.avsc' % e.name)
            load_schema(avsc)
        except IOError:
            raise e
        _load_schema(schema, schema_dir)
    return schema


def populate_schema_defs(schema, repo=None):
    repo = SCHEMA_DEFS if repo is None else repo
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: schema,
    )


def acquaint_schema(schema,
                    repo=None,
                    reader_schema_defs=None):
    """Extract schema in repo (default READERS)"""
    repo = _reader().READERS if repo is None else repo
    reader_schema_defs = \
        SCHEMA_DEFS if reader_schema_defs is None else reader_schema_defs
    extract_named_schemas_into_repo(
        schema,
        repo,
        lambda schema: lambda fo, _, r_schema: _reader().read_data(
            fo, schema, reader_schema_defs.get(r_schema)),
    )
