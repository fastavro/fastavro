import pytest
from fastavro.schema import (
    FINGERPRINT_ALGORITHMS,
    fingerprint,
    to_parsing_canonical_form,
)


@pytest.mark.parametrize(
    "fingerprint", ["CRC-64-AVRO", "SHA-256", "MD5", "sha256", "md5"]
)
def test_required_fingerprints(fingerprint):
    assert fingerprint in FINGERPRINT_ALGORITHMS


def test_unknown_algorithm():
    unknown_algorithm = "UNKNOWN"
    assert unknown_algorithm not in FINGERPRINT_ALGORITHMS

    with pytest.raises(ValueError, match="Unknown schema fingerprint algorithm"):
        fingerprint("string", unknown_algorithm)


@pytest.mark.parametrize(
    "original_schema,algorithm,expected_fingerprint",
    [
        ("int", "CRC-64-AVRO", "8f5c393f1ad57572"),
        ("int", "md5", "ef524ea1b91e73173d938ade36c1db32"),
        (
            "int",
            "sha256",
            "3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45",
        ),
        (
            {"type": "int"},
            "CRC-64-AVRO",
            "8f5c393f1ad57572",
        ),
        (
            {"type": "int"},
            "md5",
            "ef524ea1b91e73173d938ade36c1db32",
        ),
        (
            {"type": "int"},
            "sha256",
            "3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45",
        ),
        (
            "float",
            "CRC-64-AVRO",
            "90d7a83ecb027c4d",
        ),
        (
            "float",
            "md5",
            "50a6b9db85da367a6d2df400a41758a6",
        ),
        (
            "float",
            "sha256",
            "1e71f9ec051d663f56b0d8e1fc84d71aa56ccfe9fa93aa20d10547a7abeb5cc0",
        ),
        (
            {"type": "float"},
            "CRC-64-AVRO",
            "90d7a83ecb027c4d",
        ),
        (
            {"type": "float"},
            "md5",
            "50a6b9db85da367a6d2df400a41758a6",
        ),
        (
            {"type": "float"},
            "sha256",
            "1e71f9ec051d663f56b0d8e1fc84d71aa56ccfe9fa93aa20d10547a7abeb5cc0",
        ),
        (
            "long",
            "CRC-64-AVRO",
            "b71df49344e154d0",
        ),
        (
            "long",
            "md5",
            "e1dd9a1ef98b451b53690370b393966b",
        ),
        (
            "long",
            "sha256",
            "c32c497df6730c97fa07362aa5023f37d49a027ec452360778114cf427965add",
        ),
        (
            {"type": "long"},
            "CRC-64-AVRO",
            "b71df49344e154d0",
        ),
        (
            {"type": "long"},
            "md5",
            "e1dd9a1ef98b451b53690370b393966b",
        ),
        (
            {"type": "long"},
            "sha256",
            "c32c497df6730c97fa07362aa5023f37d49a027ec452360778114cf427965add",
        ),
        (
            "double",
            "CRC-64-AVRO",
            "7e95ab32c035758e",
        ),
        (
            "double",
            "md5",
            "bfc71a62f38b99d6a93690deeb4b3af6",
        ),
        (
            "double",
            "sha256",
            "730a9a8c611681d7eef442e03c16c70d13bca3eb8b977bb403eaff52176af254",
        ),
        (
            {"type": "double"},
            "CRC-64-AVRO",
            "7e95ab32c035758e",
        ),
        (
            {"type": "double"},
            "md5",
            "bfc71a62f38b99d6a93690deeb4b3af6",
        ),
        (
            {"type": "double"},
            "sha256",
            "730a9a8c611681d7eef442e03c16c70d13bca3eb8b977bb403eaff52176af254",
        ),
        (
            "bytes",
            "CRC-64-AVRO",
            "651920c3da16c04f",
        ),
        (
            "bytes",
            "md5",
            "b462f06cb909be57c85008867784cde6",
        ),
        (
            "bytes",
            "sha256",
            "9ae507a9dd39ee5b7c7e285da2c0846521c8ae8d80feeae5504e0c981d53f5fa",
        ),
        (
            {"type": "bytes"},
            "CRC-64-AVRO",
            "651920c3da16c04f",
        ),
        (
            {"type": "bytes"},
            "md5",
            "b462f06cb909be57c85008867784cde6",
        ),
        (
            {"type": "bytes"},
            "sha256",
            "9ae507a9dd39ee5b7c7e285da2c0846521c8ae8d80feeae5504e0c981d53f5fa",
        ),
        (
            "string",
            "CRC-64-AVRO",
            "c70345637248018f",
        ),
        (
            "string",
            "md5",
            "095d71cf12556b9d5e330ad575b3df5d",
        ),
        (
            "string",
            "sha256",
            "e9e5c1c9e4f6277339d1bcde0733a59bd42f8731f449da6dc13010a916930d48",
        ),
        (
            {"type": "string"},
            "CRC-64-AVRO",
            "c70345637248018f",
        ),
        (
            {"type": "string"},
            "md5",
            "095d71cf12556b9d5e330ad575b3df5d",
        ),
        (
            {"type": "string"},
            "sha256",
            "e9e5c1c9e4f6277339d1bcde0733a59bd42f8731f449da6dc13010a916930d48",
        ),
        (
            "boolean",
            "CRC-64-AVRO",
            "64f7d4a478fc429f",
        ),
        (
            "boolean",
            "md5",
            "01f692b30d4a1c8a3e600b1440637f8f",
        ),
        (
            "boolean",
            "sha256",
            "a5b031ab62bc416d720c0410d802ea46b910c4fbe85c50a946ccc658b74e677e",
        ),
        (
            {"type": "boolean"},
            "CRC-64-AVRO",
            "64f7d4a478fc429f",
        ),
        (
            {"type": "boolean"},
            "md5",
            "01f692b30d4a1c8a3e600b1440637f8f",
        ),
        (
            {"type": "boolean"},
            "sha256",
            "a5b031ab62bc416d720c0410d802ea46b910c4fbe85c50a946ccc658b74e677e",
        ),
        (
            "null",
            "CRC-64-AVRO",
            "8a8f25cce724dd63",
        ),
        (
            "null",
            "md5",
            "9b41ef67651c18488a8b08bb67c75699",
        ),
        (
            "null",
            "sha256",
            "f072cbec3bf8841871d4284230c5e983dc211a56837aed862487148f947d1a1f",
        ),
        (
            {"type": "null"},
            "CRC-64-AVRO",
            "8a8f25cce724dd63",
        ),
        (
            {"type": "null"},
            "md5",
            "9b41ef67651c18488a8b08bb67c75699",
        ),
        (
            {"type": "null"},
            "sha256",
            "f072cbec3bf8841871d4284230c5e983dc211a56837aed862487148f947d1a1f",
        ),
        (
            {"type": "fixed", "name": "Test", "size": 1},
            "CRC-64-AVRO",
            "6869897b4049355b",
        ),
        (
            {"type": "fixed", "name": "Test", "size": 1},
            "md5",
            "db01bc515fcfcd2d4be82ed385288261",
        ),
        (
            {"type": "fixed", "name": "Test", "size": 1},
            "sha256",
            "f527116a6f44455697e935afc31dc60ad0f95caf35e1d9c9db62edb3ffeb9170",
        ),
        (
            {
                "type": "fixed",
                "name": "MyFixed",
                "namespace": "org.apache.hadoop.avro",
                "size": 1,
            },
            "CRC-64-AVRO",
            "fadbd138e85bdf45",
        ),
        (
            {
                "type": "fixed",
                "name": "MyFixed",
                "namespace": "org.apache.hadoop.avro",
                "size": 1,
            },
            "md5",
            "d74b3726484422711c465d49e857b1ba",
        ),
        (
            {
                "type": "fixed",
                "name": "MyFixed",
                "namespace": "org.apache.hadoop.avro",
                "size": 1,
            },
            "sha256",
            "28e493a44771cecc5deca4bd938cdc3d5a24cfe1f3760bc938fa1057df6334fc",
        ),
        (
            {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            "CRC-64-AVRO",
            "03a2f2c2e27f7a16",
        ),
        (
            {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            "md5",
            "d883f2a9b16ed085fcc5e4ca6c8f6ed1",
        ),
        (
            {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            "sha256",
            "9b51286144f87ce5aebdc61ca834379effa5a41ce6ac0938630ff246297caca8",
        ),
        (
            {"type": "array", "items": "long"},
            "CRC-64-AVRO",
            "715e2ea28bc91654",
        ),
        (
            {"type": "array", "items": "long"},
            "md5",
            "c1c387e8d6a58f0df749b698991b1f43",
        ),
        (
            {"type": "array", "items": "long"},
            "sha256",
            "f78e954167feb23dcb1ce01e8463cebf3408e0a4259e16f24bd38f6d0f1d578b",
        ),
        (
            {
                "type": "array",
                "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            "CRC-64-AVRO",
            "10d9ade1fa3a0387",
        ),
        (
            {
                "type": "array",
                "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            "md5",
            "cfc7b861c7cfef082a6ef082948893fa",
        ),
        (
            {
                "type": "array",
                "items": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            "sha256",
            "0d8edd49d7f7e9553668f133577bc99f842852b55d9f84f1f7511e4961aa685c",
        ),
        (
            {"type": "map", "values": "long"},
            "CRC-64-AVRO",
            "6f74f4e409b1334e",
        ),
        (
            {"type": "map", "values": "long"},
            "md5",
            "32b3f1a3177a0e73017920f00448b56e",
        ),
        (
            {"type": "map", "values": "long"},
            "sha256",
            "b8fad07d458971a07692206b8a7cf626c86c62fe6bcff7c1b11bc7295de34853",
        ),
        (
            {
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            "CRC-64-AVRO",
            "df2ab0626f6b812d",
        ),
        (
            {
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            "md5",
            "c588da6ba99701c41e73fd30d23f994e",
        ),
        (
            {
                "type": "map",
                "values": {"type": "enum", "name": "Test", "symbols": ["A", "B"]},
            },
            "sha256",
            "3886747ed1669a8af476b549e97b34222afb2fed5f18bb27c6f367ea0351a576",
        ),
        (
            ["string", "null", "long"],
            "CRC-64-AVRO",
            "65a5be410d687566",
        ),
        (
            ["string", "null", "long"],
            "md5",
            "b11cf95f0a55dd55f9ee515a37bf937a",
        ),
        (
            ["string", "null", "long"],
            "sha256",
            "ed8d254116441bb35e237ad0563cf5432b8c975334bd222c1ee84609435d95bb",
        ),
        (
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            "CRC-64-AVRO",
            "ed94e5f5e6eb588e",
        ),
        (
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            "md5",
            "69531a03db788afe353244cd049b1e6d",
        ),
        (
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            "sha256",
            "9670f15a8f96d23e92830d00b8bd57275e02e3e173ffef7c253c170b6beabeb8",
        ),
        (
            {
                "type": "error",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            "CRC-64-AVRO",
            "ed94e5f5e6eb588e",
        ),
        (
            {
                "type": "error",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            "md5",
            "69531a03db788afe353244cd049b1e6d",
        ),
        (
            {
                "type": "error",
                "name": "Test",
                "fields": [{"name": "f", "type": "long"}],
            },
            "sha256",
            "9670f15a8f96d23e92830d00b8bd57275e02e3e173ffef7c253c170b6beabeb8",
        ),
        (
            {
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "label", "type": "string"},
                    {"name": "children", "type": {"type": "array", "items": "Node"}},
                ],
            },
            "CRC-64-AVRO",
            "52cba544c3e756b7",
        ),
        (
            {
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "label", "type": "string"},
                    {"name": "children", "type": {"type": "array", "items": "Node"}},
                ],
            },
            "md5",
            "99625b0cc02050363e89ef66b0f406c9",
        ),
        (
            {
                "type": "record",
                "name": "Node",
                "fields": [
                    {"name": "label", "type": "string"},
                    {"name": "children", "type": {"type": "array", "items": "Node"}},
                ],
            },
            "sha256",
            "65d80dc8c95c98a9671d92cf0415edfabfee2cb058df2138606656cd6ae4dc59",
        ),
        (
            {
                "type": "record",
                "name": "Lisp",
                "fields": [
                    {
                        "name": "value",
                        "type": [
                            "null",
                            "string",
                            {
                                "type": "record",
                                "name": "Cons",
                                "fields": [
                                    {"name": "car", "type": "Lisp"},
                                    {"name": "cdr", "type": "Lisp"},
                                ],
                            },
                        ],
                    },
                ],
            },
            "CRC-64-AVRO",
            "68d91a23eda0b306",
        ),
        (
            {
                "type": "record",
                "name": "Lisp",
                "fields": [
                    {
                        "name": "value",
                        "type": [
                            "null",
                            "string",
                            {
                                "type": "record",
                                "name": "Cons",
                                "fields": [
                                    {"name": "car", "type": "Lisp"},
                                    {"name": "cdr", "type": "Lisp"},
                                ],
                            },
                        ],
                    },
                ],
            },
            "md5",
            "9e1d0d15b52789fcb8e3a88b53059d5f",
        ),
        (
            {
                "type": "record",
                "name": "Lisp",
                "fields": [
                    {
                        "name": "value",
                        "type": [
                            "null",
                            "string",
                            {
                                "type": "record",
                                "name": "Cons",
                                "fields": [
                                    {"name": "car", "type": "Lisp"},
                                    {"name": "cdr", "type": "Lisp"},
                                ],
                            },
                        ],
                    },
                ],
            },
            "sha256",
            "e5ce4f4a15ce19fa1047cfe16a3b0e13a755db40f00f23284fdd376fc1c7dd21",
        ),
        (
            {
                "type": "record",
                "name": "HandshakeRequest",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "clientHash",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            "CRC-64-AVRO",
            "b96ad79e5a7c5757",
        ),
        (
            {
                "type": "record",
                "name": "HandshakeRequest",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "clientHash",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            "md5",
            "4c822af2e17eecd92422827eede97f5b",
        ),
        (
            {
                "type": "record",
                "name": "HandshakeRequest",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "clientHash",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {"name": "clientProtocol", "type": ["null", "string"]},
                    {"name": "serverHash", "type": "MD5"},
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            "sha256",
            "2b2f7a9b22991fe0df9134cb6b5ff7355343e797aaea337e0150e20f3a35800e",
        ),
        (
            {
                "type": "record",
                "name": "HandshakeResponse",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "match",
                        "type": {
                            "type": "enum",
                            "name": "HandshakeMatch",
                            "symbols": ["BOTH", "CLIENT", "NONE"],
                        },
                    },
                    {"name": "serverProtocol", "type": ["null", "string"]},
                    {
                        "name": "serverHash",
                        "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}],
                    },
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            "CRC-64-AVRO",
            "00feee01de4ea50e",
        ),
        (
            {
                "type": "record",
                "name": "HandshakeResponse",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "match",
                        "type": {
                            "type": "enum",
                            "name": "HandshakeMatch",
                            "symbols": ["BOTH", "CLIENT", "NONE"],
                        },
                    },
                    {"name": "serverProtocol", "type": ["null", "string"]},
                    {
                        "name": "serverHash",
                        "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}],
                    },
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            "md5",
            "afe529d01132daab7f4e2a6663e7a2f5",
        ),
        (
            {
                "type": "record",
                "name": "HandshakeResponse",
                "namespace": "org.apache.avro.ipc",
                "fields": [
                    {
                        "name": "match",
                        "type": {
                            "type": "enum",
                            "name": "HandshakeMatch",
                            "symbols": ["BOTH", "CLIENT", "NONE"],
                        },
                    },
                    {"name": "serverProtocol", "type": ["null", "string"]},
                    {
                        "name": "serverHash",
                        "type": ["null", {"name": "MD5", "size": 16, "type": "fixed"}],
                    },
                    {
                        "name": "meta",
                        "type": ["null", {"type": "map", "values": "bytes"}],
                    },
                ],
            },
            "sha256",
            "a303cbbfe13958f880605d70c521a4b7be34d9265ac5a848f25916a67b11d889",
        ),
        (
            {
                "type": "record",
                "name": "Interop",
                "namespace": "org.apache.avro",
                "fields": [
                    {"name": "intField", "type": "int"},
                    {"name": "longField", "type": "long"},
                    {"name": "stringField", "type": "string"},
                    {"name": "boolField", "type": "boolean"},
                    {"name": "floatField", "type": "float"},
                    {"name": "doubleField", "type": "double"},
                    {"name": "bytesField", "type": "bytes"},
                    {"name": "nullField", "type": "null"},
                    {
                        "name": "arrayField",
                        "type": {"type": "array", "items": "double"},
                    },
                    {
                        "name": "mapField",
                        "type": {
                            "type": "map",
                            "values": {
                                "name": "Foo",
                                "type": "record",
                                "fields": [{"name": "label", "type": "string"}],
                            },
                        },
                    },
                    {
                        "name": "unionField",
                        "type": [
                            "boolean",
                            "double",
                            {"type": "array", "items": "bytes"},
                        ],
                    },
                    {
                        "name": "enumField",
                        "type": {
                            "type": "enum",
                            "name": "Kind",
                            "symbols": ["A", "B", "C"],
                        },
                    },
                    {
                        "name": "fixedField",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {
                        "name": "recordField",
                        "type": {
                            "type": "record",
                            "name": "Node",
                            "fields": [
                                {"name": "label", "type": "string"},
                                {
                                    "name": "children",
                                    "type": {"type": "array", "items": "Node"},
                                },
                            ],
                        },
                    },
                ],
            },
            "CRC-64-AVRO",
            "e82c0a93a6a0b5a4",
        ),
        (
            {
                "type": "record",
                "name": "Interop",
                "namespace": "org.apache.avro",
                "fields": [
                    {"name": "intField", "type": "int"},
                    {"name": "longField", "type": "long"},
                    {"name": "stringField", "type": "string"},
                    {"name": "boolField", "type": "boolean"},
                    {"name": "floatField", "type": "float"},
                    {"name": "doubleField", "type": "double"},
                    {"name": "bytesField", "type": "bytes"},
                    {"name": "nullField", "type": "null"},
                    {
                        "name": "arrayField",
                        "type": {"type": "array", "items": "double"},
                    },
                    {
                        "name": "mapField",
                        "type": {
                            "type": "map",
                            "values": {
                                "name": "Foo",
                                "type": "record",
                                "fields": [{"name": "label", "type": "string"}],
                            },
                        },
                    },
                    {
                        "name": "unionField",
                        "type": [
                            "boolean",
                            "double",
                            {"type": "array", "items": "bytes"},
                        ],
                    },
                    {
                        "name": "enumField",
                        "type": {
                            "type": "enum",
                            "name": "Kind",
                            "symbols": ["A", "B", "C"],
                        },
                    },
                    {
                        "name": "fixedField",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {
                        "name": "recordField",
                        "type": {
                            "type": "record",
                            "name": "Node",
                            "fields": [
                                {"name": "label", "type": "string"},
                                {
                                    "name": "children",
                                    "type": {"type": "array", "items": "Node"},
                                },
                            ],
                        },
                    },
                ],
            },
            "md5",
            "994fea1a1be7ff8603cbe40c3bc7e4ca",
        ),
        (
            {
                "type": "record",
                "name": "Interop",
                "namespace": "org.apache.avro",
                "fields": [
                    {"name": "intField", "type": "int"},
                    {"name": "longField", "type": "long"},
                    {"name": "stringField", "type": "string"},
                    {"name": "boolField", "type": "boolean"},
                    {"name": "floatField", "type": "float"},
                    {"name": "doubleField", "type": "double"},
                    {"name": "bytesField", "type": "bytes"},
                    {"name": "nullField", "type": "null"},
                    {
                        "name": "arrayField",
                        "type": {"type": "array", "items": "double"},
                    },
                    {
                        "name": "mapField",
                        "type": {
                            "type": "map",
                            "values": {
                                "name": "Foo",
                                "type": "record",
                                "fields": [{"name": "label", "type": "string"}],
                            },
                        },
                    },
                    {
                        "name": "unionField",
                        "type": [
                            "boolean",
                            "double",
                            {"type": "array", "items": "bytes"},
                        ],
                    },
                    {
                        "name": "enumField",
                        "type": {
                            "type": "enum",
                            "name": "Kind",
                            "symbols": ["A", "B", "C"],
                        },
                    },
                    {
                        "name": "fixedField",
                        "type": {"type": "fixed", "name": "MD5", "size": 16},
                    },
                    {
                        "name": "recordField",
                        "type": {
                            "type": "record",
                            "name": "Node",
                            "fields": [
                                {"name": "label", "type": "string"},
                                {
                                    "name": "children",
                                    "type": {"type": "array", "items": "Node"},
                                },
                            ],
                        },
                    },
                ],
            },
            "sha256",
            "cccfd6e3f917cf53b0f90c206342e6703b0d905071f724a1c1f85b731c74058d",
        ),
        (
            {
                "type": "record",
                "name": "ipAddr",
                "fields": [
                    {
                        "name": "addr",
                        "type": [
                            {"name": "IPv6", "type": "fixed", "size": 16},
                            {"name": "IPv4", "type": "fixed", "size": 4},
                        ],
                    }
                ],
            },
            "CRC-64-AVRO",
            "8d961b4e298a1844",
        ),
        (
            {
                "type": "record",
                "name": "ipAddr",
                "fields": [
                    {
                        "name": "addr",
                        "type": [
                            {"name": "IPv6", "type": "fixed", "size": 16},
                            {"name": "IPv4", "type": "fixed", "size": 4},
                        ],
                    }
                ],
            },
            "md5",
            "45d85c69b353a99b93d7c4f2fcf0c30d",
        ),
        (
            {
                "type": "record",
                "name": "ipAddr",
                "fields": [
                    {
                        "name": "addr",
                        "type": [
                            {"name": "IPv6", "type": "fixed", "size": 16},
                            {"name": "IPv4", "type": "fixed", "size": 4},
                        ],
                    }
                ],
            },
            "sha256",
            "6f6fc8f685a4f07d99734946565d63108806d55a8620febea047cf52cb0ac181",
        ),
        (
            {
                "type": "record",
                "name": "TestDoc",
                "doc": "Doc string",
                "fields": [{"name": "name", "type": "string", "doc": "Doc String"}],
            },
            "CRC-64-AVRO",
            "0e6660f02bcdc109",
        ),
        (
            {
                "type": "record",
                "name": "TestDoc",
                "doc": "Doc string",
                "fields": [{"name": "name", "type": "string", "doc": "Doc String"}],
            },
            "md5",
            "f2da75f5131f5ab80629538287b8beb2",
        ),
        (
            {
                "type": "record",
                "name": "TestDoc",
                "doc": "Doc string",
                "fields": [{"name": "name", "type": "string", "doc": "Doc String"}],
            },
            "sha256",
            "0b3644f7aa5ca2fc4bad93ca2d3609c12aa9dbda9c15e68b34c120beff08e7b9",
        ),
        (
            {
                "type": "enum",
                "name": "Test",
                "symbols": ["A", "B"],
                "doc": "Doc String",
            },
            "CRC-64-AVRO",
            "03a2f2c2e27f7a16",
        ),
        (
            {
                "type": "enum",
                "name": "Test",
                "symbols": ["A", "B"],
                "doc": "Doc String",
            },
            "md5",
            "d883f2a9b16ed085fcc5e4ca6c8f6ed1",
        ),
        (
            {
                "type": "enum",
                "name": "Test",
                "symbols": ["A", "B"],
                "doc": "Doc String",
            },
            "sha256",
            "9b51286144f87ce5aebdc61ca834379effa5a41ce6ac0938630ff246297caca8",
        ),
        (
            {"type": "int"},
            "MD5",  # JAVA Name
            "ef524ea1b91e73173d938ade36c1db32",
        ),
        (
            {"type": "int"},
            "SHA-256",  # JAVA Name
            "3f2b87a9fe7cc9b13835598c3981cd45e3e355309e5090aa0933d7becb6fba45",
        ),
    ],
)
def test_random_cases(original_schema, algorithm, expected_fingerprint):
    # All of these random test cases came from the test cases here:
    # https://github.com/apache/avro/blob/0552c674637dd15b8751ed5181387cdbd81480d5/lang/py3/avro/tests/test_normalization.py
    canonical_form = to_parsing_canonical_form(original_schema)
    assert fingerprint(canonical_form, algorithm) == expected_fingerprint
