from fastavro.compile._graph import (
    _schema_to_graph,
    _find_cycle_roots,
    find_recursive_types,
    NamegraphNode,
)


def test_find_recursive_types_single():
    schema = {
        "type": "record",
        "name": "LinkedList",
        "fields": [
            {"name": "value", "type": "long"},
            {
                "name": "next",
                "type": ["null", "LinkedList"],
            },
        ],
    }
    assert [schema] == find_recursive_types(schema)


def test_find_recursive_types_nonrecursive_tree():
    schema = {
        "type": "record",
        "name": "TreeRoot",
        "fields": [
            {
                "name": "named_a",
                "type": {
                    "name": "NamedType",
                    "type": "record",
                    "fields": [],
                },
            },
            {"name": "named_b", "type": "NamedType"},
            {"name": "named_c", "type": "NamedType"},
            {
                "name": "named_array",
                "type": {
                    "type": "array",
                    "items": "NamedType",
                },
            },
        ],
    }
    assert [] == find_recursive_types(schema)


def test_compute_namegraph_empty_record():
    schema = {"type": "record", "name": "root", "fields": []}
    graph = [NamegraphNode(schema, [])]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_ignored_types():
    schema = {
        "type": "record",
        "name": "root",
        "fields": [
            {"name": "intval", "type": "int"},
            {"name": "float", "type": "float"},
            {"name": "str", "type": "string"},
            {"name": "str_verbose", "type": {"type": "string"}},
            {
                "name": "enum",
                "type": {
                    "type": "enum",
                    "symbols": ["FOO"],
                },
            },
            {
                "name": "fixedval",
                "type": {
                    "type": "fixed",
                    "name": "fixedname",
                    "size": 8,
                },
            },
        ],
    }
    graph = [NamegraphNode(schema, [])]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_nested_record():
    subschema = {
        "name": "Subrecord",
        "type": "record",
        "fields": [
            {"name": "intval", "type": "int"},
        ],
    }
    schema = {
        "type": "record",
        "name": "root",
        "fields": [
            {"name": "subrecord", "type": subschema},
        ],
    }

    graph = [NamegraphNode(schema, [NamegraphNode(subschema, [])])]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_nested_through_array():
    subschema = {
        "name": "Subrecord",
        "type": "record",
        "fields": [
            {"name": "intval", "type": "int"},
        ],
    }
    schema = {
        "type": "record",
        "name": "root",
        "fields": [
            {
                "name": "array_field",
                "type": {"type": "array", "items": subschema},
            },
        ],
    }
    graph = [NamegraphNode(schema, [NamegraphNode(subschema, [])])]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_nested_through_map():
    subschema = {
        "name": "Subrecord",
        "type": "record",
        "fields": [
            {"name": "intval", "type": "int"},
        ],
    }
    schema = {
        "type": "record",
        "name": "root",
        "fields": [
            {
                "name": "map_field",
                "type": {
                    "type": "map",
                    "values": subschema,
                },
            },
        ],
    }
    graph = [NamegraphNode(schema, [NamegraphNode(subschema, [])])]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_recursive():
    schema = {
        "type": "record",
        "name": "InfiniteRecursion",
        "fields": [
            {
                "name": "next",
                "type": "InfiniteRecursion",
            },
        ],
    }

    graph = [NamegraphNode(schema)]
    graph[0].references = [graph[0]]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_recursive_through_map():
    schema = {
        "type": "record",
        "name": "Tree",
        "fields": [
            {
                "name": "children",
                "type": {
                    "type": "map",
                    "values": "Tree",
                },
            },
        ],
    }

    graph = [NamegraphNode(schema)]
    graph[0].references = [graph[0]]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_recursive_through_union():
    schema = {
        "type": "record",
        "name": "LinkedList",
        "fields": [
            {"name": "value", "type": "long"},
            {
                "name": "next",
                "type": ["null", "LinkedList"],
            },
        ],
    }

    graph = [NamegraphNode(schema)]
    graph[0].references = [graph[0]]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_recursive_through_alias():
    schema = {
        "type": "record",
        "name": "LinkedList",
        "aliases": ["LinkedListNode"],
        "fields": [
            {"name": "value", "type": "long"},
            {
                "name": "next",
                "type": ["null", "LinkedListNode"],
            },
        ],
    }
    graph = [NamegraphNode(schema)]
    graph[0].references = [graph[0]]
    have = _schema_to_graph(schema, {})
    assert have == graph


def test_compute_namegraph_complicated():
    association_schema = {
        "type": "record",
        "name": "Association",
        "fields": [
            {"name": "id", "type": "long"},
            {
                "name": "members",
                "type": {
                    "type": "array",
                    "items": "User",
                },
            },
            {"name": "db", "type": "Database"},
        ],
    }
    user_schema = {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "username", "type": "string"},
            {
                "name": "associations",
                "type": association_schema,
            },
        ],
    }
    db_schema = {
        "type": "record",
        "name": "Database",
        "aliases": ["db"],
        "fields": [
            {
                "name": "users",
                "type": {
                    "type": "array",
                    "items": user_schema,
                },
            },
            {
                "name": "admin",
                "type": "User",
            },
            {
                "name": "last_accessor",
                "type": ["null", "User"],
            },
        ],
    }

    db_node = NamegraphNode(db_schema)
    user_node = NamegraphNode(user_schema)
    association_node = NamegraphNode(association_schema)

    db_node.references = [user_node]
    user_node.references = [association_node]
    association_node.references = [user_node, db_node]

    graph = [db_node]
    have = _schema_to_graph(db_schema, {})
    assert have == graph

    roots = [db_node, user_node]
    have = _find_cycle_roots(graph[0])
    assert set(have) == set(roots)

    recursive_types = [user_schema, db_schema]
    have = find_recursive_types(db_schema)
    have.sort(key=lambda x: x["name"])
    recursive_types.sort(key=lambda x: x["name"])
    assert have == recursive_types
