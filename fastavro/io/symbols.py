class Symbol(object):
    def __init__(self, production=None):
        self.production = production

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def __ne__(self, other):
        return not self.__eq__(other)


class Root(Symbol):
    pass


class Terminal(Symbol):
    pass


Null = type("Null", (Terminal,), {})
Boolean = type("Boolean", (Terminal,), {})
String = type("String", (Terminal,), {})
Bytes = type("Bytes", (Terminal,), {})
Int = type("Int", (Terminal,), {})
Long = type("Long", (Terminal,), {})
Float = type("Float", (Terminal,), {})
Double = type("Double", (Terminal,), {})
Fixed = type("Fixed", (Terminal,), {})

Union = type("Union", (Terminal,), {})

MapEnd = type("MapEnd", (Terminal,), {})
MapStart = type("MapStart", (Terminal,), {})
MapKeyMarker = type("MapKeyMarker", (Terminal,), {})
ItemEnd = type("ItemEnd", (Terminal,), {})

ArrayEnd = type("ArrayEnd", (Terminal,), {})
ArrayStart = type("ArrayStart", (Terminal,), {})

Enum = type("Enum", (Terminal,), {})


class Sequence(Symbol):
    def __init__(self, *symbols):
        Symbol.__init__(self, list(symbols))


class Repeater(Symbol):
    """Arrays"""

    def __init__(self, end, *symbols):
        Symbol.__init__(self, list(symbols))
        self.production.insert(0, self)
        self.end = end


class Alternative(Symbol):
    """Unions"""

    def __init__(self, symbols, labels):
        Symbol.__init__(self, symbols)
        self.labels = labels

    def get_symbol(self, index):
        return self.production[index]

    def get_label(self, index):
        return self.labels[index]


class Action(Symbol):
    pass


class EnumLabels(Action):
    def __init__(self, labels):
        self.labels = labels


class UnionEnd(Action):
    pass


class RecordStart(Action):
    pass


class RecordEnd(Action):
    pass


class FieldStart(Action):
    def __init__(self, field_name):
        self.field_name = field_name


class FieldEnd(Action):
    pass
