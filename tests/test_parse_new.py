import json
from typing import Any, Dict

from fastavro.parse_new import decompose_schema, parse_to_canonical

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
    enum={"name": "myenum", "type": "enum", "symbols": ["A", "B", "C"]},
    array={"type": "array", "items": "string"},
    union=["null", "string"],
    logical={"logicalType": "logicalInt", "type": "int"},
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
            {"name": "myenum", "type": input["enum"]},
            {"name": "myarray", "type": input["array"]},
            {"name": "myunion", "type": input["union"]},
            {"name": "mylogical", "type": input["logical"]},
            {"name": "missing", "type": "ns_D.D"},
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
    enum={"name": "ns_C.myenum", "type": "enum", "symbols": ["A", "B", "C"]},
    array={"type": "array", "items": "string"},
    union=["null", "string"],
    logical={"type": "int", "logicalType": "logicalInt"},
)
output["schema_record_C"] = {
    "name": "ns_C.C",
    "type": "record",
    "fields": [
        {"name": "föo", "type": output["schema_record_A"]},
        {"name": "bar", "type": output["schema_record_B"]},
        {"name": "foobar", "type": output["fixed_item"]},
        {"name": "myenum", "type": output["enum"]},
        {"name": "myarray", "type": output["array"]},
        {"name": "myunion", "type": output["union"]},
        {"name": "mylogical", "type": output["logical"]},
        {"name": "missing", "type": "ns_D.D"},
    ],
}


def to_json_no_whitespace(obj):
    return json.dumps(obj, separators=(",", ":"))


def test_parse_to_canonical():
    schema_out = parse_to_canonical(
        input["schema_record_C"], keep_logicalType=True, keep_attributes=False
    )
    assert to_json_no_whitespace(schema_out) == to_json_no_whitespace(
        output["schema_record_C"]
    )


def test_decompose():
    schema_out = parse_to_canonical(
        input["schema_record_C"], keep_logicalType=True, keep_attributes=True
    )
    decomposed_schema, sub_schemas, referenced_schema_names, missing_schema_names = (
        decompose_schema(schema_out)
    )
    exp_ref_schema_names = set(
        (
            "ns_C.C",
            "ns_B.B",
            "ns_C.A",
            "ns_C.fixed_item",
            "ns_C.myenum",
            "array_95d849abd4f49d40e0efe5d1dd9dab63",
            "union_9e050db2b774e33e2d03046c04c98671",
            "logical_2bde359c645bfad7b5df654da2331627",
        )
    )
    assert decomposed_schema == "ns_C.C"
    assert exp_ref_schema_names == set(sub_schemas.keys())
    assert missing_schema_names == {"ns_D.D"}
    assert referenced_schema_names == exp_ref_schema_names
