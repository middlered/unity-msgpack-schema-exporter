"""
Tests for unity_msgpack_schema Python bindings.

These tests require:
1. The NativeAOT shared library (built with dotnet publish)
2. The TestSchemas DLL (built with dotnet build)

Tests are automatically skipped if the native library is not available.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def schema(native_lib_path: str, test_dll_path: str):
    """Create a MsgPackSchema instance for testing."""
    from unity_msgpack_schema import MsgPackSchema

    s = MsgPackSchema(test_dll_path, lib_path=native_lib_path)
    yield s
    s.close()


class TestMsgPackSchema:
    """Tests for the MsgPackSchema wrapper."""

    def test_schema_count(self, schema):
        assert schema.schema_count >= 10

    def test_list_classes(self, schema):
        classes = schema.list_classes()
        assert isinstance(classes, list)
        assert len(classes) >= 10
        class_set = set(classes)
        for name in [
            "UnityMsgpackSchemaExporter.TestSchemas.SimpleItem",
            "UnityMsgpackSchemaExporter.TestSchemas.IntKeyedItem",
            "UnityMsgpackSchemaExporter.TestSchemas.Order",
        ]:
            assert name in class_set, f"{name} not found in class list"

    def test_get_schema_simple_item(self, schema):
        info = schema.get_schema("SimpleItem")
        assert info["name"] == "SimpleItem"
        assert info["isStringKeyed"] is True
        assert len(info["fields"]) == 5
        field_names = {f["memberName"] for f in info["fields"]}
        assert {"id", "name", "active", "score", "bigValue"} == field_names

    def test_get_schema_int_keyed(self, schema):
        info = schema.get_schema("IntKeyedItem")
        assert info["isStringKeyed"] is False
        assert len(info["fields"]) == 3

    def test_get_schema_not_found(self, schema):
        with pytest.raises(RuntimeError):
            schema.get_schema("NonExistentClass")

    def test_avro_schema(self, schema):
        avro = schema.avro_schema("SimpleItem")
        assert avro["type"] == "record"
        assert avro["name"] == "SimpleItem"
        assert len(avro["fields"]) == 5

    def test_avro_schema_nullable(self, schema):
        avro = schema.avro_schema("SimpleItem", nullable=True)
        for field in avro["fields"]:
            assert isinstance(field["type"], list), (
                f"{field['name']} should be nullable"
            )
            assert "null" in field["type"]

    def test_avro_schema_flat(self, schema):
        avro = schema.avro_schema("Order", flat=True)
        items_field = next(f for f in avro["fields"] if f["name"] == "items")
        items_type = items_field["type"]
        assert items_type["type"] == "array"
        assert isinstance(items_type["items"], str)
        assert "OrderItem" in items_type["items"]

    def test_avro_non_string_key_has_key_type(self, schema):
        """Dictionary with non-string key should have msgpack_key_type in Avro map output."""
        avro = schema.avro_schema("ComplexNested")
        tag_groups = next(f for f in avro["fields"] if f["name"] == "tagGroups")
        t = tag_groups["type"]
        assert t["type"] == "map", (
            "non-string-keyed dict should become Avro map with msgpack_key_type"
        )
        assert "msgpack_key_type" in t, "msgpack_key_type custom field must be present"
        assert t["msgpack_key_type"] == "int"


class TestMsgPackRoundtrip:
    """Tests for the msgpack->msgpack encode/decode (no JSON overhead)."""

    def test_decode_returns_bytes(self, schema):
        """decode() should return raw bytes (named-key msgpack map)."""
        # First encode via JSON path to get compact bytes
        compact = schema.encode_json(
            "SimpleItem",
            {
                "id": 1,
                "name": "x",
                "active": False,
                "score": 0.0,
                "bigValue": 0,
            },
        )
        named = schema.decode("SimpleItem", compact)
        assert isinstance(named, bytes)
        assert len(named) > 0

    def test_encode_returns_bytes(self, schema):
        """encode() should accept named-key msgpack bytes and return compact bytes."""
        compact0 = schema.encode_json(
            "SimpleItem",
            {
                "id": 99,
                "name": "test",
                "active": True,
                "score": 1.5,
                "bigValue": 123,
            },
        )
        named = schema.decode("SimpleItem", compact0)
        compact1 = schema.encode("SimpleItem", named)
        assert isinstance(compact1, bytes)
        assert len(compact1) > 0

    def test_msgpack_roundtrip(self, schema):
        """compact -> decode -> encode -> decode_json should recover original values."""
        original = {
            "id": 42,
            "name": "hello",
            "active": True,
            "score": 3.14,
            "bigValue": 9999,
        }
        compact = schema.encode_json("SimpleItem", original)
        named = schema.decode("SimpleItem", compact)
        recompact = schema.encode("SimpleItem", named)
        result = schema.decode_json("SimpleItem", recompact)
        assert result["id"] == 42
        assert result["name"] == "hello"
        assert result["active"] is True
        assert abs(result["score"] - 3.14) < 0.01
        assert result["bigValue"] == 9999

    def test_msgpack_nested_roundtrip(self, schema):
        """Nested objects should survive msgpack->msgpack roundtrip."""
        original = {
            "orderId": 55,
            "customer": "Bob",
            "items": [{"productId": 3, "quantity": 10, "unitPrice": 1.5}],
            "tags": ["a", "b"],
            "metadata": {"k": "v"},
        }
        compact = schema.encode_json("Order", original)
        named = schema.decode("Order", compact)
        recompact = schema.encode("Order", named)
        result = schema.decode_json("Order", recompact)
        assert result["orderId"] == 55
        assert len(result["items"]) == 1
        assert result["items"][0]["productId"] == 3

    def test_decode_invalid_class(self, schema):
        with pytest.raises(RuntimeError):
            schema.decode("NoSuchClass", b"\x80")

    def test_encode_invalid_class(self, schema):
        with pytest.raises(RuntimeError):
            schema.encode("NoSuchClass", b"\x80")


class TestJsonRoundtrip:
    """Tests for the JSON-based encode_json/decode_json convenience wrappers."""

    def test_simple_item_roundtrip(self, schema):
        original = {
            "id": 7,
            "name": "world",
            "active": False,
            "score": 2.5,
            "bigValue": 42,
        }
        encoded = schema.encode_json("SimpleItem", original)
        assert isinstance(encoded, bytes)
        decoded = schema.decode_json("SimpleItem", encoded)
        assert decoded["id"] == 7
        assert decoded["name"] == "world"
        assert abs(decoded["score"] - 2.5) < 0.01

    def test_decode_json_invalid_class(self, schema):
        with pytest.raises(RuntimeError):
            schema.decode_json("NoSuchClass", b"\xc0")

    def test_encode_json_invalid_class(self, schema):
        with pytest.raises(RuntimeError):
            schema.encode_json("NoSuchClass", {"key": "value"})


class TestContextManager:
    def test_context_manager(self, native_lib_path: str, test_dll_path: str):
        from unity_msgpack_schema import MsgPackSchema

        with MsgPackSchema(test_dll_path, lib_path=native_lib_path) as s:
            assert s.schema_count >= 10
        with pytest.raises(RuntimeError):
            s.list_classes()

    def test_double_close(self, native_lib_path: str, test_dll_path: str):
        from unity_msgpack_schema import MsgPackSchema

        s = MsgPackSchema(test_dll_path, lib_path=native_lib_path)
        s.close()
        s.close()
