from typing import Any, Dict

from fastavro.parse_new import depth_first_walk_schema, parse_to_canonical

input: Dict[str, Any] = dict(
    schema_record_A={
        "name": "A",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}],
    },
    schema_record_B={
        "name": "B",
        "namespace": "ns_B",
        "type": "record",
        "fields": [{"name": "bar", "type": "string"}],
    },
    fixed_item={"name": "fixed_item", "type": "fixed", "size": "016"},
)
input["schema_record_C"] = {
    "type": {
        "type": "record",
        "name": "C",
        "namespace": "ns_C",
        "doc": "This is the input",
        "fields": [
            {"name": "f\u00f6o", "type": input["schema_record_A"]},
            {"name": "bar", "type": input["schema_record_B"]},
            {"name": "foobar", "type": input["fixed_item"]},
        ],
    }
}

output = dict(
    schema_record_A={
        "name": "ns_C.A",
        "type": "record",
        "fields": [{"name": "foo", "type": "string"}],
    },
    schema_record_B={
        "name": "ns_B.B",
        "type": "record",
        "fields": [{"name": "bar", "type": "string"}],
    },
    fixed_item={"name": "ns_C.fixed_item", "type": "fixed", "size": 16},
)
output["schema_record_C"] = {
    "name": "ns_C.C",
    "type": "record",
    "fields": [
        {"name": "föo", "type": output["schema_record_A"]},
        {"name": "bar", "type": output["schema_record_B"]},
        {"name": "foobar", "type": output["fixed_item"]},
    ],
}


def test_resolve_fullname():
    schema_out = parse_to_canonical(input["schema_record_C"], keep_logicalType=False)
    assert schema_out == output["schema_record_C"]
