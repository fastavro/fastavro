from typing import Dict, Any, Union, List, Callable, IO
from ast import (
    FunctionDef,
    arguments,
    Compare,
    arg,
    Name,
    Load,
    Store,
    Expr,
    Assign,
    Pass,
    For,
    While,
    If,
    AST,
    Call,
    Module,
    ImportFrom,
    alias,
    Constant,
    Subscript,
    Dict as DictLiteral,
    fix_missing_locations,
    dump,
    unparse,
    Return,
    walk,
    parse,
    Eq,
)


PRIMITIVE_READERS = {
    "string": "read_utf8",
    "int": "read_long",
    "long": "read_long",
    "float": "read_float",
    "double": "read_double",
    "boolean": "read_boolean",
    "bytes": "read_bytes",
    "null": "read_null",
}


class SchemaParser:
    schema: Dict[str, Any]
    variable_count: int
    reader_name: str

    def __init__(self, schema: Dict[str, Any], reader_name: str):
        self.schema = schema
        self.reader_name = reader_name
        self.variable_count = 0

    def new_variable(self) -> str:
        """
        Returns a new name for a variable which is guaranteed to be unique.
        """
        self.variable_count += 1
        return f"v{self.variable_count}"

    def compile(self) -> Callable[[IO[bytes]], Any]:
        """
        Compile the schema and return a callable function which will read from a
        file-like byte source and produce a value determined by schema.
        """
        module = self.generate_module()
        filename = "<generated>"
        compiled = compile(module, filename, mode="exec")
        namespace = {}
        exec(compiled, namespace)
        return namespace[self.reader_name]

    def generate_module(self) -> Module:
        import_from_fastavro_read = []
        for reader in PRIMITIVE_READERS.values():
            import_from_fastavro_read.append(alias(name=reader))

        body = [
            ImportFrom(
                module="fastavro._read",
                names=import_from_fastavro_read,
                level=0,
            ),
            self.generate_reader_func(),
        ]
        module = Module(
            body=body,
            type_ignores=[],
        )
        module = fix_missing_locations(module)
        return module

    def generate_reader_func(self) -> FunctionDef:
        """
        Returns an AST describing a function which can read an Avro message from a
        IO[bytes] source. The message is parsed according to the SchemaParser's
        schema.
        """
        src_var = Name(id="src", ctx=Load())
        result_var = Name(id=self.new_variable(), ctx=Store())
        body = []
        body.extend(self._gen_reader(self.schema, src_var, result_var))
        body.append(Return(value=Name(id=result_var.id, ctx=Load())))
        func = FunctionDef(
            name=self.reader_name,
            args=arguments(
                args=[arg("src")],
                posonlyargs=[],
                kwonlyargs=[],
                kw_defaults=[],
                defaults=[],
            ),
            body=body,
            decorator_list=[],
        )
        return func

    def _gen_reader(self, schema: Any, src: Name, dest: AST) -> List[AST]:
        """
        Returns a sequence of statements which will read data from src and write
        the deserialized value into dest.
        """
        if isinstance(schema, str):
            if schema in PRIMITIVE_READERS.keys():
                return self._gen_primitive_reader(
                    primitive_type=schema,
                    src=src,
                    dest=dest
                )
            else:
                # TODO: Named type reference.
                pass
        if isinstance(schema, list):
            return self._gen_union_reader(
                options=schema,
                src=src,
                dest=dest,
            )
        if isinstance(schema, dict):
            schema_type = schema["type"]
            if schema_type in PRIMITIVE_READERS.keys():
                return self._gen_primitive_reader(
                    primitive_type=schema_type,
                    src=src,
                    dest=dest,
                )
            if schema_type == "record":
                return self._gen_record_reader(
                    schema=schema,
                    src=src,
                    dest=dest,
                )
            # TODO: Array.
            # TODO: Map.
            # TODO: Fixed.
            # TODO: Enum.

        raise NotImplementedError(
            f"Schema type not implemented: {schema}"
        )

    def _gen_union_reader(self, options: List[Any], src: Name, dest: AST) -> List[AST]:
        statements = []
        # Read a long to figure out which option in the union is chosen.
        idx_var = self.new_variable()
        idx_var_dest = Name(id=idx_var, ctx=Store())
        statements.extend(self._gen_primitive_reader("long", src, idx_var_dest))
        # TODO special case optional fields, which have exactly two options, one
        # of which is null.

        idx_var_ref = Name(id=idx_var, ctx=Load())
        prev_if = None
        for idx, option in enumerate(options):
            if_idx_matches = Compare(
                left=idx_var_ref,
                ops=[Eq()],
                comparators=[
                    Constant(idx)
                ]
            )
            if_stmt = If(
                test=if_idx_matches,
                body=self._gen_reader(option, src, dest),
                orelse=[],
            )

            if prev_if is None:
                statements.append(if_stmt)
            else:
                prev_if.orelse = [if_stmt]
            prev_if = if_stmt

        return statements

    def _gen_record_reader(self, schema: Dict, src: Name, dest: AST) -> List[AST]:
        statements = []

        # Construct a new empty dictionary to hold the record contents.
        value_name = self.new_variable()
        empty_dict = DictLiteral(keys=[], values=[])
        statements.append(
            Assign(
                targets=[Name(id=value_name, ctx=Store())],
                value=empty_dict,
                lineno=0,
            ),
        )
        value_reference = Name(id=value_name, ctx=Load())

        # Write statements to populate all the fields of the record.
        for field in schema["fields"]:
            # Make an AST node that references an entry in the record dict,
            # using the field name as a key.
            field_dest = Subscript(
                value=value_reference,
                slice=Constant(value=field["name"]),
                ctx=Store(),
            )

            # Generate the statements required to read that field's type, and to
            # store it into field_dest.
            read_statements = self._gen_reader(field["type"], src, field_dest)
            statements.extend(read_statements)


        # Now that we have a fully constructed record, write it into the
        # destination provided.
        statements.append(
            Assign(
                targets=[dest],
                value=value_reference,
                lineno=0,
            )
        )
        return statements

    def _gen_primitive_reader(self, primitive_type: str, src: Name, dest: AST) -> List[AST]:
        """
        Returns a statement which will deserialize a given primitive type from src
        into dest.
        """
        if primitive_type == "null":
            statement = Assign(
                targets=[dest],
                value=Constant(value=None),
                lineno=0,
            )
            return [statement]

        reader_func_name = PRIMITIVE_READERS[primitive_type]
        value = Call(
            func=Name(id=reader_func_name, ctx=Load()),
            args=[src],
            keywords=[],
        )
        statement = Assign(
            targets=[dest],
            value=value,
            lineno=0,
        )
        return [statement]
