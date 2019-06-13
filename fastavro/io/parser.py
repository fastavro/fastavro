from .symbols import (
    Root, Terminal, Boolean, Sequence, Repeater, Action, RecordStart,
    RecordEnd, FieldStart, FieldEnd, Int, Null, String, Alternative, Union,
    Long, Float, Double, Bytes, MapEnd, MapStart, MapKeyMarker, Enum,
    EnumLabels, Fixed, ArrayStart, ArrayEnd, ItemEnd,
)
from ..schema import extract_record_type


class Parser:
    def __init__(self, schema, action_function):
        self.schema = schema
        self.action_function = action_function
        self.stack = self.parse()

    def parse(self):
        symbol = self._parse(self.schema)
        root = Root([symbol])
        root.production.insert(0, root)
        return [root, symbol]

    def _parse(self, schema):
        record_type = extract_record_type(schema)

        if record_type == 'record':
            production = []

            production.append(RecordStart())
            for field in schema["fields"]:
                production.insert(0, FieldStart(field["name"]))
                production.insert(0, self._parse(field["type"]))
                production.insert(0, FieldEnd())
            production.insert(0, RecordEnd())

            seq = Sequence(*production)
            return seq

        elif record_type == 'union':
            symbols = []
            labels = []
            for candidate_schema in schema:
                symbols.append(self._parse(candidate_schema))
                if isinstance(candidate_schema, dict):
                    labels.append(
                        candidate_schema.get(
                            "name",
                            candidate_schema.get("type")
                        )
                    )
                else:
                    labels.append(candidate_schema)

            return Sequence(Alternative(symbols, labels), Union())

        elif record_type == "map":
            repeat = Repeater(
                MapEnd(),
                # ItemEnd(),  # TODO: Maybe need this?
                self._parse(schema["values"]),
                MapKeyMarker(),
                String(),
            )
            return Sequence(repeat, MapStart())

        elif record_type == "array":
            repeat = Repeater(
                ArrayEnd(),
                ItemEnd(),
                self._parse(schema["items"]),
            )
            return Sequence(repeat, ArrayStart())

        elif record_type == "enum":
            return Sequence(EnumLabels(schema["symbols"]), Enum())

        elif record_type == "null":
            return Null()
        elif record_type == "boolean":
            return Boolean()
        elif record_type == "string":
            return String()
        elif record_type == "bytes":
            return Bytes()
        elif record_type == "int":
            return Int()
        elif record_type == "long":
            return Long()
        elif record_type == "float":
            return Float()
        elif record_type == "double":
            return Double()
        elif record_type == "fixed":
            return Fixed()
        else:
            raise Exception("Unhandled type: {}".format(record_type))

    def advance(self, symbol):
        while True:
            top = self.stack.pop()

            if top == symbol:
                return top
            elif isinstance(top, Action):
                self.action_function(top)
            elif isinstance(top, Terminal):
                raise Exception("Internal Parser Exception: {}".format(top))
            elif isinstance(top, Repeater) and top.end == symbol:
                return symbol
            else:
                self.stack.extend(top.production)

    def drain_actions(self):
        while True:
            top = self.stack.pop()

            if isinstance(top, Root):
                self.push_symbol(top)
                break
            elif isinstance(top, Action):
                self.action_function(top)
            elif not isinstance(top, Terminal):
                self.stack.extend(top.production)
            else:
                raise Exception("Internal Parser Exception: {}".format(top))

    def pop_symbol(self):
        return self.stack.pop()

    def push_symbol(self, symbol):
        self.stack.append(symbol)

    def flush(self):
        while len(self.stack) > 0:
            top = self.stack.pop()

            if isinstance(top, Action) or isinstance(top, Root):
                self.action_function(top)
            else:
                raise Exception("Internal Parser Exception: {}".format(top))
