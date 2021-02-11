import timeit
import io
from fastavro import schemaless_reader, schemaless_writer
from fastavro.compile.ast_compile import SchemaParser

def main():
    compare_primitive_record()
    compare_nested_record()

def prepare_read_buffer(message, schema):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, message)
    buf.seek(0)
    return buf

def compare(message, schema, name):
    buf = prepare_read_buffer(message, schema)

    def read_schemaless():
        buf.seek(0)
        schemaless_reader(buf, schema)

    compiled_reader = SchemaParser(schema, "reader").compile()
    def read_compiled():
        buf.seek(0)
        compiled_reader(buf)

    print(f"benchmarking {name}")
    t = timeit.Timer(stmt="read_schemaless()", globals=locals())
    schemaless = time_and_print(t, "schemaless")
    t = timeit.Timer(stmt="read_compiled()", globals=locals())
    compiled = time_and_print(t, "compiled")
    delta = 100 * (compiled - schemaless) / schemaless
    print(f"\tdiff: {delta:.2f}%")

def compare_primitive_record():
    schema = {
        "type": "record",
        "name": "Record",
        "fields": [
            {"type": "string", "name": "string_field"},
            {"type": "int", "name": "int_field"},
            {"type": "long", "name": "long_field"},
            {"type": "float", "name": "float_field"},
            {"type": "double", "name": "double_field"},
            {"type": "boolean", "name": "boolean_field"},
            {"type": "bytes", "name": "bytes_field"},
            {"type": "null", "name": "null_field"},
        ],
    }

    message = {
        "string_field": "string_value",
        "int_field": 1,
        "long_field": 2,
        "float_field": 3.0,
        "double_field": -4.0,
        "boolean_field": True,
        "bytes_field": b"bytes_value",
        "null_field": None,
    }
    compare(message, schema, "primitive_record")

def compare_nested_record():
    schema = {
        "type": "record",
        "name": "parent",
        "fields": [
            {"name": "child",
             "type": {
                 "type": "record",
                 "name": "child",
                 "fields": [
                     {"type": "int", "name": "int_field"},
                     {"type": "long", "name": "long_field"},
                     {"name": "grandchild", "type": {
                         "type": "record",
                         "name": "grandchild",
                         "fields": [
                             {"type": "string", "name": "str_field"},
                         ]
                     }
                     }
                 ]
             }},
            {"type": "int", "name": "int_field"},
            {"type": "long", "name": "long_field"},
        ],
    }

    message = {
        "int_field": 1,
        "long_field": 2,
        "child": {
            "int_field": 3,
            "long_field": 4,
            "grandchild": {
                "str_field": "blah blah blah",
            }
        }
    }
    compare(message, schema, "nested_record")


def format_time(dt):
    units = {"nsec": 1e-9, "usec": 1e-6, "msec": 1e-3, "sec": 1.0}
    scales = [(scale, unit) for unit, scale in units.items()]
    scales.sort(reverse=True)
    for scale, unit in scales:
        if dt >= scale:
            break

    return "%.*g %s" % (3, dt / scale, unit)


def time_and_print(timer, label):
    n, _ = timer.autorange()
    timings = [dt / n for dt in timer.repeat(repeat=7, number=n)]
    best = min(timings)
    print(f"\t{label}:  {n} iterations, best of 7: {format_time(best)} / iteration")
    return best


if __name__ == "__main__":
    main()
