"""
This module contains functions devoted to finding recursive components of
the graph formed by an Avro schema.

Recursion only occurs for named types which contain a reference to their own
name. Only a few types can have names: records, enums, and fixeds.

In addition, only a few types can contain a reference to a named type:
- Arrays (the type of their items can be named)
- Records (the type of their fields can be named)
- Maps (the type of their values can be named)
- Unions (any of their permitted types can be named)

Because enums and fixeds can't contain references, they can never be recursive.
Only records can, although they might be recursive through a chain that flows
through some other complex type like an array.
"""
from typing import Dict, Set, Optional, Iterable, List, Deque, DefaultDict

from fastavro.schema import expand_schema
from fastavro._schema_common import PRIMITIVES
import collections


def find_recursive_types(schema: Dict) -> List[str]:
    """
    Find the names of all types in the schema which are defined recursively.

    The return value is a list. Each item is the fully-qualified Avro type name
    of a type which includes a reference to itself, either in its fields or
    somewhere down the tree of its fields' fields.

    The list is returned in depth-first ordering.
    """
    expanded = expand_schema(schema)

    names: Dict[str, "NamegraphNode"] = {}
    graph = _schema_to_graph(expanded, names)
    if len(graph) != 1:
        return []
    root = graph[0]
    cycle_roots = _find_cycle_roots(root)
    return [node.name for node in cycle_roots]


def _find_cycle_roots(graph: "NamegraphNode") -> List["NamegraphNode"]:
    stack: Deque["NamegraphNode"] = collections.deque()

    # Map of node -> count of how many times it has appeared in the current
    # stack.
    visited: DefaultDict["NamegraphNode", int] = collections.defaultdict(int)

    roots: Set["NamegraphNode"] = set()

    def visit(node: "NamegraphNode"):
        stack.append(node)
        visited[node] += 1
        for ref in node.references:
            if ref in visited:
                # Found a cycle.
                roots.add(ref)
            else:
                visit(ref)
        visited[node] -= 1
        stack.pop()

    visit(graph)
    return list(roots)


class NamegraphNode:
    name: str  # Fully-qualified name.
    references: Set["NamegraphNode"]

    def __init__(
        self, name: str, references: Optional[Iterable["NamegraphNode"]] = None
    ):
        self.name = name
        if references is not None:
            self.references = set(references)
        else:
            self.references = set()

    def add_reference(self, ref: "NamegraphNode"):
        self.references.add(ref)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, NamegraphNode):
            return NotImplemented
        if self.name != other.name:
            return False
        if len(self.references) != len(other.references):
            return False
        self_names = set(x.name for x in self.references)
        other_names = set(x.name for x in other.references)
        return self_names == other_names

    def __hash__(self):
        return hash(self.name)

    def __repr__(self) -> str:
        name = repr(self.name)
        refs = repr(self.references)
        return f"NamegraphNode(name={name}, references={refs})"


def _schema_to_graph(schema: Dict, names: Dict) -> List[NamegraphNode]:
    # Convert a schema definition into a graph representation which only
    # includes named-type components.

    if isinstance(schema, str):
        # Strings are names of types.
        if schema in PRIMITIVES:
            return []
        # If the name is pointing to a user-defined type, it **must** be defined
        # at this point, according to the Avro spec:
        #
        #   A name must be defined before it is used ("before" in the
        #   depth-first, left-to-right traversal of the JSON parse tree, where
        #   the types attribute of a protocol is always deemed to come "before"
        #   the messages attribute.
        #
        return [names[schema]]

    elif isinstance(schema, list):
        # Lists are unions of types.
        result = []
        for s in schema:
            result.extend(_schema_to_graph(s, names))
        return result

    elif isinstance(schema, dict):
        # Dicts are complex types.
        schema_type = schema["type"]

        if schema_type == "array":
            # Arrays can have named references in the type of their items.
            return _schema_to_graph(schema["items"], names)

        elif schema_type == "map":
            # Maps can have named references in the type of their values.
            return _schema_to_graph(schema["values"], names)

        elif schema_type == "record" or schema_type == "error":
            # Records can have named references in the type of their fields.
            node = NamegraphNode(schema["name"])

            names[schema["name"]] = node
            for alias in schema.get("aliases", []):
                names[alias] = node

            for field in schema["fields"]:
                field_schema = field["type"]
                for subnode in _schema_to_graph(field_schema, names):
                    node.add_reference(subnode)
            return [node]

        elif (
            schema_type == "enum" or schema_type == "fixed" or schema_type in PRIMITIVES
        ):
            # Enums, fixeds, and primitives cannot include named references.
            return []

        else:
            # This is a verbosely-defined type name.
            return [names[schema_type]]
