import timeit
import io
from fastavro import schemaless_reader, schemaless_writer
from fastavro.compile.ast_compile import SchemaParser

from tests.test_ast_compile import testcases


def main():
    for tc in testcases:
        compare(tc.messages[0], tc.schema, tc.label)


def prepare_read_buffer(message, schema):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, message)
    buf.seek(0)
    return buf


def compare(message, schema, name):
    print(f"benchmarking '{name}'")
    buf = prepare_read_buffer(message, schema)

    def read_schemaless():
        buf.seek(0)
        return schemaless_reader(buf, schema)

    compiled_reader = SchemaParser(schema).compile()

    def read_compiled():
        buf.seek(0)
        return compiled_reader(buf)

    assert read_schemaless() == read_compiled()

    t = timeit.Timer(stmt="read_schemaless()", globals=locals())
    schemaless = time_and_print(t, "schemaless")
    t = timeit.Timer(stmt="read_compiled()", globals=locals())
    compiled = time_and_print(t, "compiled")
    delta = 100 * (compiled - schemaless) / schemaless
    print(f"\tdiff: {delta:.2f}%")


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
