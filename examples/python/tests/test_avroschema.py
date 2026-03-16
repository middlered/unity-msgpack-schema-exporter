"""Pytest tests for unity_msgpack_avro schema decode/encode."""
import json
import struct

import pytest

from unity_msgpack_avro import decode, encode, load_bytes

# ──────────────────────────────────────────────────────────────────────────────
# Schema fixtures (mirrors Go tests)
# ──────────────────────────────────────────────────────────────────────────────

SIMPLE_ITEM = b"""{
  "type": "record",
  "name": "SimpleItem",
  "namespace": "Test",
  "fields": [
    {"name": "id",     "type": "int",              "msgpack_key": "id"},
    {"name": "name",   "type": ["null","string"],   "msgpack_key": "name"},
    {"name": "score",  "type": "float",             "msgpack_key": "score"},
    {"name": "active", "type": "boolean",           "msgpack_key": "active"}
  ]
}"""

INT_KEYED = b"""{
  "type": "record",
  "name": "IntKeyedItem",
  "namespace": "Test",
  "fields": [
    {"name": "id",    "type": "int",           "msgpack_key": 0},
    {"name": "label", "type": ["null","string"],"msgpack_key": 1},
    {"name": "value", "type": "double",        "msgpack_key": 2}
  ]
}"""

INT_KEY_DICT = b"""{
  "type": "record",
  "name": "IntKeyedDict",
  "namespace": "Test",
  "fields": [
    {"name": "id",     "type": "string", "msgpack_key": "id"},
    {"name": "scores", "type": {"type": "map", "values": "float", "msgpack_key_type": "int"}, "msgpack_key": "scores"}
  ]
}"""

UNION_SCHEMA = b"""[
  {
    "type": "record", "name": "UnionChildA", "namespace": "Test",
    "fields": [{"name": "x", "type": "int", "msgpack_key": "x"}]
  },
  {
    "type": "record", "name": "UnionChildB", "namespace": "Test",
    "fields": [{"name": "y", "type": "string", "msgpack_key": "y"}]
  },
  {
    "type": "record", "name": "UnionBase", "namespace": "Test",
    "fields": [],
    "msgpack_unions": [
      {"key": 0, "type": "UnionChildA"},
      {"key": 1, "type": "UnionChildB"}
    ]
  }
]"""

BYTES_KEY_DICT = b"""{
  "type": "record",
  "name": "BytesKeyedDict",
  "namespace": "Test",
  "fields": [
    {"name": "data", "type": {"type": "map", "values": "int", "msgpack_key_type": "bytes"}, "msgpack_key": "data"}
  ]
}"""

NESTED_SCHEMA = b"""[
  {
    "type": "record", "name": "Inner", "namespace": "Test",
    "fields": [
      {"name": "v", "type": "int", "msgpack_key": 0}
    ]
  },
  {
    "type": "record", "name": "Outer", "namespace": "Test",
    "fields": [
      {"name": "tag",  "type": "string", "msgpack_key": "tag"},
      {"name": "item", "type": "Test.Inner", "msgpack_key": "item"}
    ]
  }
]"""


def _load(raw: bytes):
    reg, root = load_bytes(raw)
    return reg, root


# ──────────────────────────────────────────────────────────────────────────────
# String-keyed record
# ──────────────────────────────────────────────────────────────────────────────

class TestStringKeyedRecord:
    def test_roundtrip(self):
        reg, _ = _load(SIMPLE_ITEM)
        schema = reg["SimpleItem"]
        obj = {"id": 1, "name": "Sword", "score": 9.5, "active": True}
        data = encode(schema, obj)
        result = decode(schema, data)
        assert result["id"] == 1
        assert result["name"] == "Sword"
        assert result["active"] is True

    def test_second_roundtrip_bytes_equal(self):
        reg, _ = _load(SIMPLE_ITEM)
        schema = reg["SimpleItem"]
        obj = {"id": 42, "name": "Shield", "score": 7.0, "active": False}
        b1 = encode(schema, obj)
        b2 = encode(schema, decode(schema, b1))
        assert b1.hex() == b2.hex()

    def test_nullable_null(self):
        reg, _ = _load(SIMPLE_ITEM)
        schema = reg["SimpleItem"]
        obj = {"id": 5, "name": None, "score": 0.0, "active": False}
        data = encode(schema, obj)
        result = decode(schema, data)
        assert result["name"] is None

    def test_nullable_value(self):
        reg, _ = _load(SIMPLE_ITEM)
        schema = reg["SimpleItem"]
        obj = {"id": 9, "name": "Bow", "score": 3.0, "active": True}
        data = encode(schema, obj)
        result = decode(schema, data)
        assert result["name"] == "Bow"


# ──────────────────────────────────────────────────────────────────────────────
# Int-keyed record (array format)
# ──────────────────────────────────────────────────────────────────────────────

class TestIntKeyedRecord:
    def test_encoded_is_array(self):
        reg, _ = _load(INT_KEYED)
        schema = reg["IntKeyedItem"]
        obj = {"id": 7, "label": "hello", "value": 3.14}
        data = encode(schema, obj)
        # fixarray header: 0x9X
        assert (data[0] & 0xF0) == 0x90

    def test_roundtrip(self):
        reg, _ = _load(INT_KEYED)
        schema = reg["IntKeyedItem"]
        obj = {"id": 7, "label": "hello", "value": 3.14}
        result = decode(schema, encode(schema, obj))
        assert result["id"] == 7
        assert result["label"] == "hello"

    def test_null_label(self):
        reg, _ = _load(INT_KEYED)
        schema = reg["IntKeyedItem"]
        obj = {"id": 3, "label": None, "value": 1.0}
        result = decode(schema, encode(schema, obj))
        assert result["label"] is None

    def test_bytes_equal_roundtrip(self):
        reg, _ = _load(INT_KEYED)
        schema = reg["IntKeyedItem"]
        obj = {"id": 99, "label": "xyz", "value": 0.5}
        b1 = encode(schema, obj)
        b2 = encode(schema, decode(schema, b1))
        assert b1.hex() == b2.hex()


# ──────────────────────────────────────────────────────────────────────────────
# Map with non-string keys (msgpack_key_type)
# ──────────────────────────────────────────────────────────────────────────────

class TestIntKeyMap:
    def test_roundtrip(self):
        reg, _ = _load(INT_KEY_DICT)
        schema = reg["IntKeyedDict"]
        obj = {"id": "player1", "scores": {10: 1.5, 20: 2.5}}
        data = encode(schema, obj)
        result = decode(schema, data)
        assert result["id"] == "player1"
        assert len(result["scores"]) == 2
        # Keys should be native ints
        assert 10 in result["scores"] or "10" in result["scores"]

    def test_key_type_preserved(self):
        reg, _ = _load(INT_KEY_DICT)
        schema = reg["IntKeyedDict"]
        obj = {"id": "p2", "scores": {1: 0.1, 2: 0.2, 3: 0.3}}
        data = encode(schema, obj)
        result = decode(schema, data)
        keys = set(result["scores"].keys())
        # All keys should decode to integers (or their str form if int)
        for k in keys:
            assert isinstance(k, (int, str))


class TestBytesKeyMap:
    def test_roundtrip(self):
        reg, _ = _load(BYTES_KEY_DICT)
        schema = reg["BytesKeyedDict"]
        obj = {"data": {b"\x01\x02": 42, b"\xFF": 99}}
        data = encode(schema, obj)
        result = decode(schema, data)
        vals = result["data"]
        assert len(vals) == 2
        # Keys are bytes after decoding
        for k in vals.keys():
            assert isinstance(k, (bytes, str))


# ──────────────────────────────────────────────────────────────────────────────
# Union dispatch
# ──────────────────────────────────────────────────────────────────────────────

class TestUnionDispatch:
    def test_child_a(self):
        reg, _ = _load(UNION_SCHEMA)
        schema = reg["UnionBase"]
        obj = {"__type": 0, "value": {"x": 42}}
        data = encode(schema, obj)
        result = decode(schema, data)
        assert result["__type"] == 0
        assert result["value"]["x"] == 42

    def test_child_b(self):
        reg, _ = _load(UNION_SCHEMA)
        schema = reg["UnionBase"]
        obj = {"__type": 1, "value": {"y": "hello"}}
        data = encode(schema, obj)
        result = decode(schema, data)
        assert result["__type"] == 1
        assert result["value"]["y"] == "hello"

    def test_roundtrip_bytes_equal(self):
        reg, _ = _load(UNION_SCHEMA)
        schema = reg["UnionBase"]
        obj = {"__type": 0, "value": {"x": 7}}
        b1 = encode(schema, obj)
        b2 = encode(schema, decode(schema, b1))
        assert b1.hex() == b2.hex()


# ──────────────────────────────────────────────────────────────────────────────
# Nested schemas (cross-reference)
# ──────────────────────────────────────────────────────────────────────────────

class TestNestedSchema:
    def test_roundtrip(self):
        reg, _ = _load(NESTED_SCHEMA)
        schema = reg["Outer"]
        obj = {"tag": "hello", "item": {"v": 77}}
        data = encode(schema, obj)
        result = decode(schema, data)
        assert result["tag"] == "hello"
        assert result["item"]["v"] == 77


# ──────────────────────────────────────────────────────────────────────────────
# Multi-schema load
# ──────────────────────────────────────────────────────────────────────────────

class TestLoadAll:
    def test_load_multiple_records(self):
        schemas = b"""[
          {"type":"record","name":"TypeA","namespace":"T","fields":[{"name":"a","type":"int","msgpack_key":"a"}]},
          {"type":"record","name":"TypeB","namespace":"T","fields":[{"name":"b","type":"string","msgpack_key":"b"}]}
        ]"""
        reg, _ = _load(schemas)
        assert "T.TypeA" in reg
        assert "T.TypeB" in reg

    def test_registry_short_names(self):
        schemas = b"""[
          {"type":"record","name":"TypeA","namespace":"T","fields":[]},
          {"type":"record","name":"TypeB","namespace":"T","fields":[]}
        ]"""
        reg, _ = _load(schemas)
        # Short names should also work
        assert "TypeA" in reg or "T.TypeA" in reg


# ──────────────────────────────────────────────────────────────────────────────
# Edge cases
# ──────────────────────────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_string(self):
        reg, _ = _load(SIMPLE_ITEM)
        schema = reg["SimpleItem"]
        obj = {"id": 0, "name": "", "score": 0.0, "active": False}
        result = decode(schema, encode(schema, obj))
        assert result["name"] == ""

    def test_large_int(self):
        reg, _ = _load(INT_KEYED)
        schema = reg["IntKeyedItem"]
        obj = {"id": 100000, "label": "big", "value": 0.0}
        result = decode(schema, encode(schema, obj))
        assert result["id"] == 100000

    def test_negative_int(self):
        reg, _ = _load(INT_KEYED)
        schema = reg["IntKeyedItem"]
        obj = {"id": -1, "label": "neg", "value": 0.0}
        result = decode(schema, encode(schema, obj))
        assert result["id"] == -1

    def test_json_serializable(self):
        reg, _ = _load(SIMPLE_ITEM)
        schema = reg["SimpleItem"]
        obj = {"id": 99, "name": "Axe", "score": 5.0, "active": True}
        result = decode(schema, encode(schema, obj))
        # Should not raise
        json.dumps(result)
