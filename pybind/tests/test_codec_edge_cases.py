"""
Port of CodecEdgeCaseTests.cs, CoverageBranchTests.cs, and PrimitiveAndPropertyTests.cs.

Uses Python msgpack library to manually construct msgpack bytes for edge-case tests.
"""

from __future__ import annotations

import base64
import pytest

try:
    import msgpack

    HAS_MSGPACK = True
except ImportError:
    HAS_MSGPACK = False

pytestmark = pytest.mark.skipif(not HAS_MSGPACK, reason="msgpack library not available")


@pytest.fixture(scope="module")
def schema(native_lib_path: str, test_dll_path: str):
    from unity_msgpack_schema import MsgPackSchema

    s = MsgPackSchema(test_dll_path, lib_path=native_lib_path)
    yield s
    s.close()


def pack(*args, **kwargs):
    """Pack with use_bin_type=True for consistent binary handling."""
    return msgpack.packb(*args, use_bin_type=True, **kwargs)


def unpack(data: bytes) -> object:
    return msgpack.unpackb(data, raw=False, strict_map_key=False)


# ─────────────────────────────────────────────────────────────────────────────
# Port of CodecEdgeCaseTests.cs
# ─────────────────────────────────────────────────────────────────────────────


class TestCodecEdgeCases:
    # Unknown string key stored as-is
    def test_decode_json_unknown_string_key_stored_as_is(self, schema):
        data = pack({"id": 1, "name": "test", "unknownField": 42})
        result = schema.decode_json("SimpleItem", data)
        assert result["unknownField"] == 42

    def test_decode_json_null_key_stored_with_fallback_name(self, schema):
        data = pack({"id": 1, None: 99})
        result = schema.decode_json("SimpleItem", data)
        assert result["_unknown_1"] == 99

    # Int-keyed schema with extra indices -> _index_ fallback
    def test_decode_json_int_keyed_unknown_index_fallback_name(self, schema):
        # SparseIntKeys has keys 0, 2, 5; extra indices 1, 3, 4, 6 are unknown
        data = pack([10, 99, "hello", 88, 77, True, 66])
        result = schema.decode_json("SparseIntKeys", data)
        assert result["a"] == 10
        assert result["b"] == "hello"
        assert result["c"] is True
        assert result["_index_1"] == 99
        assert result["_index_3"] == 88
        assert result["_index_4"] == 77
        assert result["_index_6"] == 66

    # ReadAny branches
    def test_read_any_boolean(self, schema):
        data = pack({"id": 1, "extra": True})
        result = schema.decode_json("SimpleItem", data)
        assert result["extra"] is True

    def test_read_any_float(self, schema):
        data = pack({"id": 1, "extra": 3.14})
        result = schema.decode_json("SimpleItem", data)
        assert abs(result["extra"] - 3.14) < 0.001

    def test_read_any_binary(self, schema):
        data = pack({"id": 1, "extra": bytes([0xDE, 0xAD])})
        result = schema.decode_json("SimpleItem", data)
        assert result["extra"] is not None

    def test_read_any_array(self, schema):
        data = pack({"id": 1, "extra": [10, 20]})
        result = schema.decode_json("SimpleItem", data)
        assert isinstance(result["extra"], list)
        assert len(result["extra"]) == 2

    def test_read_any_map(self, schema):
        data = pack({"id": 1, "extra": {"k": "v"}})
        result = schema.decode_json("SimpleItem", data)
        assert result["extra"]["k"] == "v"

    def test_read_any_extension(self, schema):
        data = pack({"id": 1, "extra": msgpack.ExtType(42, bytes([1, 2, 3]))})
        result = schema.decode_json("SimpleItem", data)
        assert result["extra"]["_ext_type"] == 42
        assert result["extra"]["_ext_data"] is not None

    def test_read_any_nil(self, schema):
        data = pack({"id": 1, "extra": None})
        result = schema.decode_json("SimpleItem", data)
        assert result["extra"] is None

    def test_read_any_large_positive_integer(self, schema):
        large = 2**62  # fits in int64
        data = pack({"id": 1, "extra": large})
        result = schema.decode_json("SimpleItem", data)
        assert result["extra"] == large

    def test_read_any_string(self, schema):
        data = pack({"id": 1, "unknownStr": "hello-world"})
        result = schema.decode_json("SimpleItem", data)
        assert result["unknownStr"] == "hello-world"

    # EncodeJson: null/nil handling
    def test_encode_json_null_input_writes_nil(self, schema):
        encoded = schema.encode_json("SimpleItem", None)
        result = unpack(encoded)
        assert result is None

    def test_encode_json_int_keyed_missing_field_writes_nil(self, schema):
        encoded = schema.encode_json("IntKeyedItem", {"id": 5})
        result = schema.decode_json("IntKeyedItem", encoded)
        assert result["id"] == 5
        assert result["label"] is None
        assert result["value"] is None

    def test_encode_json_int_keyed_gaps_filled_with_nil(self, schema):
        encoded = schema.encode_json("SparseIntKeys", {"a": 1, "b": "x", "c": True})
        result = schema.decode_json("SparseIntKeys", encoded)
        assert result["a"] == 1
        assert result["b"] == "x"
        assert result["c"] is True

    def test_encode_json_with_enum_like_integer_priority(self, schema):
        obj = {"id": 1, "status": "active", "priority": 5}
        encoded = schema.encode_json("WithEnumLike", obj)
        decoded = schema.decode_json("WithEnumLike", encoded)
        assert decoded["priority"] == 5
        assert decoded["status"] == "active"

    def test_encode_json_dictionary_int_key_roundtrip(self, schema):
        obj = {"id": 1, "scores": {"5": 1.5, "10": 2.5}}
        encoded = schema.encode_json("IntKeyedDict", obj)
        decoded = schema.decode_json("IntKeyedDict", encoded)
        assert decoded["id"] == 1

    def test_encode_json_null_array_writes_nil(self, schema):
        obj = {
            "orderId": 1,
            "customer": "test",
            "items": None,
            "tags": None,
            "metadata": None,
        }
        encoded = schema.encode_json("Order", obj)
        decoded = schema.decode_json("Order", encoded)
        assert decoded["items"] is None

    # Union encode edge cases
    def test_encode_json_union_unknown_discriminator_writes_result(self, schema):
        obj = {"_type": 999, "_data": {"x": 1}}
        encoded = schema.encode_json("UnionBase", obj)
        decoded = schema.decode_json("UnionBase", encoded)
        assert decoded["_type"] == 999

    def test_encode_json_union_null_payload(self, schema):
        obj = {"_type": 0}
        encoded = schema.encode_json("UnionBase", obj)
        decoded = schema.decode_json("UnionBase", encoded)
        assert decoded["_type"] == 0

    # DecodeJson: nil msgpack -> null
    def test_decode_json_nil_msgpack_returns_none(self, schema):
        data = pack(None)
        result = schema.decode_json("SimpleItem", data)
        assert result is None

    # MsgPack: unknown key passthrough
    def test_msgpack_decode_unknown_key_passed_through(self, schema):
        data = pack({"id": 1, "unknownExtra": 42})
        named = schema.decode("SimpleItem", data)
        named_map = unpack(named)
        assert "unknownExtra" in named_map

    def test_msgpack_decode_int_keyed_extra_indices_named(self, schema):
        data = pack([10, 99, "hi", 88])
        decoded = schema.decode("SparseIntKeys", data)
        named_map = unpack(decoded)
        assert "_index_1" in named_map
        assert "_index_3" in named_map

    # MsgPack roundtrip via encode
    def test_msgpack_encode_string_keyed_roundtrip(self, schema):
        obj = {"id": 1, "name": "test", "active": True, "score": 3.14, "bigValue": 9999}
        compact = schema.encode_json("SimpleItem", obj)
        named = schema.decode("SimpleItem", compact)
        recompact = schema.encode("SimpleItem", named)
        result = schema.decode_json("SimpleItem", recompact)
        assert result["id"] == 1
        assert result["name"] == "test"

    def test_msgpack_transcode_nullable_fields(self, schema):
        obj = {
            "id": 1,
            "optionalInt": 42,
            "optionalLong": None,
            "optionalBool": True,
            "optionalStr": None,
        }
        compact = schema.encode_json("NullableFields", obj)
        named = schema.decode("NullableFields", compact)
        recompact = schema.encode("NullableFields", named)
        result = schema.decode_json("NullableFields", recompact)
        assert result["optionalInt"] == 42
        assert result["optionalLong"] is None
        assert result["optionalBool"] is True

    def test_msgpack_transcode_dictionary_int_key(self, schema):
        obj = {"id": 1, "scores": {"5": 1.5, "10": 2.5}}
        compact = schema.encode_json("IntKeyedDict", obj)
        named = schema.decode("IntKeyedDict", compact)
        recompact = schema.encode("IntKeyedDict", named)
        result = schema.decode_json("IntKeyedDict", recompact)
        assert result["id"] == 1

    def test_msgpack_transcode_hashset(self, schema):
        obj = {"id": 1, "memberIds": [10, 20, 30]}
        compact = schema.encode_json("WithHashSet", obj)
        named = schema.decode("WithHashSet", compact)
        recompact = schema.encode("WithHashSet", named)
        result = schema.decode_json("WithHashSet", recompact)
        assert result["id"] == 1
        assert len(result["memberIds"]) == 3

    def test_msgpack_transcode_string_array(self, schema):
        obj = {
            "orderId": 1,
            "customer": "Alice",
            "items": [{"productId": 1, "quantity": 2, "unitPrice": 9.99}],
            "tags": ["rush", "vip"],
            "metadata": {"k": "v"},
        }
        compact = schema.encode_json("Order", obj)
        named = schema.decode("Order", compact)
        recompact = schema.encode("Order", named)
        result = schema.decode_json("Order", recompact)
        assert result["customer"] == "Alice"
        assert len(result["tags"]) == 2

    def test_msgpack_transcode_three_levels_deep(self, schema):
        obj = {"id": 1, "items": [{"name": "L2", "children": [{"value": "L3"}]}]}
        compact = schema.encode_json("Level1", obj)
        named = schema.decode("Level1", compact)
        recompact = schema.encode("Level1", named)
        result = schema.decode_json("Level1", recompact)
        assert result["items"][0]["children"][0]["value"] == "L3"

    def test_msgpack_transcode_union_string_keyed(self, schema):
        obj = {"_type": 0, "_data": {"label": "hello", "value": 42}}
        compact = schema.encode_json("UnionBase", obj)
        named = schema.decode("UnionBase", compact)
        recompact = schema.encode("UnionBase", named)
        result = schema.decode_json("UnionBase", recompact)
        assert result["_type"] == 0
        assert result["_data"]["label"] == "hello"

    def test_msgpack_transcode_union_int_keyed(self, schema):
        obj = {"_type": 0, "_data": {"num": 10, "name": "test"}}
        compact = schema.encode_json("IntUnionBase", obj)
        named = schema.decode("IntUnionBase", compact)
        recompact = schema.encode("IntUnionBase", named)
        result = schema.decode_json("IntUnionBase", recompact)
        assert result["_type"] == 0
        assert result["_data"]["num"] == 10

    def test_msgpack_transcode_int_keyed_with_string_child(self, schema):
        obj = {
            "id": 1,
            "tag": {"tagId": 5, "label": "test"},
            "tags": [{"tagId": 6, "label": "other"}],
        }
        compact = schema.encode_json("IntKeyedWithStringChild", obj)
        named = schema.decode("IntKeyedWithStringChild", compact)
        recompact = schema.encode("IntKeyedWithStringChild", named)
        result = schema.decode_json("IntKeyedWithStringChild", recompact)
        assert result["tag"]["tagId"] == 5

    def test_msgpack_transcode_string_keyed_with_int_child(self, schema):
        obj = {
            "id": 1,
            "child": {"id": 10, "label": "inner", "value": 3.14},
            "children": [{"id": 20, "label": "inner2", "value": 2.72}],
        }
        compact = schema.encode_json("StringKeyedWithIntChild", obj)
        named = schema.decode("StringKeyedWithIntChild", compact)
        recompact = schema.encode("StringKeyedWithIntChild", named)
        result = schema.decode_json("StringKeyedWithIntChild", recompact)
        assert result["child"]["id"] == 10

    def test_msgpack_transcode_int_keyed_with_int_child(self, schema):
        obj = {
            "id": 1,
            "child": {"id": 10, "label": "inner", "value": 3.14},
            "children": [{"id": 20, "label": "inner2", "value": 2.72}],
        }
        compact = schema.encode_json("IntKeyedWithIntChild", obj)
        named = schema.decode("IntKeyedWithIntChild", compact)
        recompact = schema.encode("IntKeyedWithIntChild", named)
        result = schema.decode_json("IntKeyedWithIntChild", recompact)
        assert result["child"]["id"] == 10

    def test_msgpack_decode_nil_input_writes_nil(self, schema):
        data = pack(None)
        decoded = schema.decode("SimpleItem", data)
        assert unpack(decoded) is None

    def test_msgpack_encode_nil_input_writes_nil(self, schema):
        data = pack(None)
        encoded = schema.encode("SimpleItem", data)
        assert unpack(encoded) is None

    def test_msgpack_transcode_complex_nested(self, schema):
        obj = {
            "id": 1,
            "singleTag": {"tagId": 1, "label": "t1"},
            "tagArray": [{"tagId": 2, "label": "t2"}],
            "tagGroups": {"100": [{"tagId": 3, "label": "t3"}]},
        }
        compact = schema.encode_json("ComplexNested", obj)
        named = schema.decode("ComplexNested", compact)
        recompact = schema.encode("ComplexNested", named)
        result = schema.decode_json("ComplexNested", recompact)
        assert result["singleTag"]["tagId"] == 1

    def test_msgpack_transcode_message_with_union(self, schema):
        obj = {
            "id": 1,
            "payload": {"_type": 0, "_data": {"label": "main", "value": 10}},
            "payloads": [
                {"_type": 0, "_data": {"label": "a", "value": 1}},
                {"_type": 1, "_data": {"message": "b", "enabled": True}},
            ],
        }
        compact = schema.encode_json("MessageWithUnion", obj)
        named = schema.decode("MessageWithUnion", compact)
        recompact = schema.encode("MessageWithUnion", named)
        result = schema.decode_json("MessageWithUnion", recompact)
        assert result["id"] == 1
        assert len(result["payloads"]) == 2

    def test_decode_msgpack_invalid_class_name_throws(self, schema):
        with pytest.raises(RuntimeError):
            schema.decode("NonExistent", bytes([0xC0]))

    def test_encode_msgpack_invalid_class_name_throws(self, schema):
        with pytest.raises(RuntimeError):
            schema.encode("NonExistent", bytes([0xC0]))

    def test_msgpack_encode_string_keyed_missing_fields_writes_nil(self, schema):
        data = pack({"id": 5})
        encoded = schema.encode("SimpleItem", data)
        result = schema.decode_json("SimpleItem", encoded)
        assert result["id"] == 5

    def test_encode_json_byte_array_base64_roundtrip(self, schema):
        obj = {"id": 1, "name": "hello", "active": False, "score": 0.0, "bigValue": 0}
        encoded = schema.encode_json("SimpleItem", obj)
        decoded = schema.decode_json("SimpleItem", encoded)
        encoded2 = schema.encode_json("SimpleItem", decoded)
        decoded2 = schema.decode_json("SimpleItem", encoded2)
        assert decoded2["name"] == "hello"

    def test_write_primitive_default_branch_boolean(self, schema):
        obj = {"id": 1, "status": "active", "priority": 5}
        encoded = schema.encode_json("WithEnumLike", obj)
        decoded = schema.decode_json("WithEnumLike", encoded)
        assert decoded["priority"] == 5

    def test_decode_json_string_field_nil(self, schema):
        data = pack({"id": 1, "name": None})
        result = schema.decode_json("SimpleItem", data)
        assert result["name"] is None

    def test_encode_json_null_string_writes_nil(self, schema):
        obj = {"id": 1, "name": None, "active": True, "score": 1.0, "bigValue": 100}
        encoded = schema.encode_json("SimpleItem", obj)
        decoded = schema.decode_json("SimpleItem", encoded)
        assert decoded["name"] is None

    def test_encode_json_nullable_with_value_roundtrip(self, schema):
        obj = {
            "id": 1,
            "optionalInt": 42,
            "optionalLong": 9999,
            "optionalBool": False,
            "optionalStr": "hi",
        }
        encoded = schema.encode_json("NullableFields", obj)
        decoded = schema.decode_json("NullableFields", encoded)
        assert decoded["optionalInt"] == 42
        assert decoded["optionalLong"] == 9999
        assert decoded["optionalBool"] is False
        assert decoded["optionalStr"] == "hi"

    def test_encode_json_hashset_roundtrip(self, schema):
        obj = {"id": 1, "memberIds": [10, 20, 30]}
        encoded = schema.encode_json("WithHashSet", obj)
        decoded = schema.decode_json("WithHashSet", encoded)
        assert len(decoded["memberIds"]) == 3

    def test_msgpack_transcode_null_nested_object(self, schema):
        obj = {"id": 1, "singleTag": None, "tagArray": [], "tagGroups": {}}
        compact = schema.encode_json("ComplexNested", obj)
        named = schema.decode("ComplexNested", compact)
        recompact = schema.encode("ComplexNested", named)
        result = schema.decode_json("ComplexNested", recompact)
        assert result["singleTag"] is None

    def test_msgpack_transcode_int_keyed_with_union(self, schema):
        obj = {
            "id": 1,
            "payload": {"_type": 0, "_data": {"num": 10, "name": "test"}},
            "payloads": [
                {"_type": 0, "_data": {"num": 1, "name": "a"}},
                {"_type": 1, "_data": {"data": "b", "flag": True}},
            ],
        }
        compact = schema.encode_json("IntKeyedWithUnion", obj)
        named = schema.decode("IntKeyedWithUnion", compact)
        recompact = schema.encode("IntKeyedWithUnion", named)
        result = schema.decode_json("IntKeyedWithUnion", recompact)
        assert result["id"] == 1

    def test_decode_json_bytes_field_returns_base64(self, schema):
        # Manually inject binary data into an unknown field
        data = pack({"id": 1, "extraBinary": bytes([0x01, 0x02, 0x03])})
        result = schema.decode_json("SimpleItem", data)
        assert result["extraBinary"] is not None

    def test_msgpack_transcode_empty_collections(self, schema):
        obj = {
            "orderId": 1,
            "customer": "test",
            "items": [],
            "tags": [],
            "metadata": {},
        }
        compact = schema.encode_json("Order", obj)
        named = schema.decode("Order", compact)
        recompact = schema.encode("Order", named)
        result = schema.decode_json("Order", recompact)
        assert len(result["items"]) == 0
        assert len(result["tags"]) == 0

    def test_encode_json_dictionary_string_value_roundtrip(self, schema):
        obj = {
            "orderId": 1,
            "customer": "test",
            "items": [],
            "tags": [],
            "metadata": {"key1": "val1"},
        }
        compact = schema.encode_json("Order", obj)
        result = schema.decode_json("Order", compact)
        compact2 = schema.encode_json("Order", result)
        result2 = schema.decode_json("Order", compact2)
        assert result2["metadata"]["key1"] == "val1"

    def test_encode_json_float_field_roundtrip(self, schema):
        obj = {"id": 1, "name": "test", "active": False, "score": 2.718, "bigValue": 0}
        encoded = schema.encode_json("SimpleItem", obj)
        decoded = schema.decode_json("SimpleItem", encoded)
        assert abs(decoded["score"] - 2.718) < 0.01

    def test_encode_json_double_and_long_roundtrip(self, schema):
        obj = {"id": 0, "label": "test", "value": 1.23456789}
        encoded = schema.encode_json("IntKeyedItem", obj)
        decoded = schema.decode_json("IntKeyedItem", encoded)
        assert abs(decoded["value"] - 1.23456789) < 0.0001

    def test_nullable_fields_all_null_returns_nulls(self, schema):
        obj = {
            "id": 1,
            "optionalInt": None,
            "optionalLong": None,
            "optionalBool": None,
            "optionalStr": None,
        }
        compact = schema.encode_json("NullableFields", obj)
        result = schema.decode_json("NullableFields", compact)
        assert result["optionalInt"] is None
        assert result["optionalLong"] is None
        assert result["optionalBool"] is None

    def test_encode_json_string_value_for_array_field_writes_nil(self, schema):
        """Order.tags is string[] — passing a non-array string should write nil."""
        obj = {
            "orderId": 1,
            "customer": "test",
            "items": [],
            "tags": "not-an-array",
            "metadata": {},
        }
        encoded = schema.encode_json("Order", obj)
        decoded = schema.decode_json("Order", encoded)
        assert decoded["tags"] is None

    def test_encode_json_string_value_for_dict_field_writes_nil(self, schema):
        obj = {
            "orderId": 1,
            "customer": "test",
            "items": [],
            "tags": [],
            "metadata": "not-a-dict",
        }
        encoded = schema.encode_json("Order", obj)
        decoded = schema.decode_json("Order", encoded)
        assert decoded["metadata"] is None

    def test_msgpack_transcode_union_unknown_discriminator(self, schema):
        data = pack([999, "some-data"])
        named = schema.decode("UnionBase", data)
        named_map = unpack(named)
        assert isinstance(named_map, dict)
        assert len(named_map) == 2

    def test_msgpack_transcode_union_compact_unknown(self, schema):
        # Named map with _type=999 should produce compact array [999, ...]
        data = pack({"_type": 999, "_data": "raw-data"})
        compact = schema.encode("UnionBase", data)
        compact_val = unpack(compact)
        assert isinstance(compact_val, list)
        assert len(compact_val) == 2
        assert compact_val[0] == 999

    def test_msgpack_transcode_null_union_payload(self, schema):
        data = pack([0, None])
        named = schema.decode("UnionBase", data)
        assert named is not None
        assert len(named) > 0

    def test_msgpack_encode_union_missing_data_writes_nil(self, schema):
        data = pack({"_type": 0})
        compact = schema.encode("UnionBase", data)
        compact_val = unpack(compact)
        assert isinstance(compact_val, list)
        assert len(compact_val) == 2
        assert compact_val[0] == 0
        assert compact_val[1] is None

    def test_encode_json_null_field_value_writes_nil(self, schema):
        obj = {"id": 1, "singleTag": None, "tagArray": [], "tagGroups": {}}
        encoded = schema.encode_json("ComplexNested", obj)
        decoded = schema.decode_json("ComplexNested", encoded)
        assert decoded["singleTag"] is None

    def test_msgpack_encode_string_keyed_partial_fields(self, schema):
        data = pack({"id": 42, "name": "partial"})
        encoded = schema.encode("SimpleItem", data)
        decoded = schema.decode_json("SimpleItem", encoded)
        assert decoded["id"] == 42
        assert decoded["name"] == "partial"

    def test_msgpack_encode_int_keyed_missing_members(self, schema):
        data = pack({"id": 10})
        encoded = schema.encode("IntKeyedItem", data)
        decoded = schema.decode_json("IntKeyedItem", encoded)
        assert decoded["id"] == 10
        assert decoded["label"] is None

    def test_read_any_binary_null_sequence(self, schema):
        data = pack({"id": 1, "bin": bytes([])})
        result = schema.decode_json("SimpleItem", data)
        assert result["bin"] is not None


# ─────────────────────────────────────────────────────────────────────────────
# Port of PrimitiveAndPropertyTests.cs
# ─────────────────────────────────────────────────────────────────────────────


def _make_all_primitives_dict(**overrides):
    """Build a full AllPrimitiveTypes dict with default values."""
    b64_empty = base64.b64encode(b"").decode()
    d = {
        "byteVal": 0,
        "sbyteVal": 0,
        "shortVal": 0,
        "ushortVal": 0,
        "intVal": 0,
        "uintVal": 0,
        "longVal": 0,
        "ulongVal": 0,
        "floatVal": 0.0,
        "doubleVal": 0.0,
        "boolVal": False,
        "stringVal": "",
        "bytesVal": b64_empty,
    }
    d.update(overrides)
    return d


class TestPrimitiveAndPropertyTests:
    def test_all_primitives_json_roundtrip(self, schema):
        b64_bytes = base64.b64encode(bytes([0xDE, 0xAD, 0xBE, 0xEF])).decode()
        obj = {
            "byteVal": 0xFF,
            "sbyteVal": -100,
            "shortVal": -30000,
            "ushortVal": 60000,
            "intVal": -2000000,
            "uintVal": 4000000000,
            "longVal": -9000000000000,
            "ulongVal": 18000000000000000000,
            "floatVal": 3.14,
            "doubleVal": 2.71828,
            "boolVal": True,
            "stringVal": "hello",
            "bytesVal": b64_bytes,
        }
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["byteVal"] == 0xFF
        assert result["sbyteVal"] == -100
        assert result["shortVal"] == -30000
        assert result["ushortVal"] == 60000
        assert result["intVal"] == -2000000
        assert result["uintVal"] == 4000000000
        assert result["longVal"] == -9000000000000
        assert result["ulongVal"] == 18000000000000000000
        assert abs(result["floatVal"] - 3.14) < 0.01
        assert abs(result["doubleVal"] - 2.71828) < 0.0001
        assert result["boolVal"] is True
        assert result["stringVal"] == "hello"
        assert result["bytesVal"] == b64_bytes

    def test_all_primitives_json_encode_roundtrip(self, schema):
        b64 = base64.b64encode(bytes([1, 2, 3])).decode()
        obj = {
            "byteVal": 42,
            "sbyteVal": -1,
            "shortVal": 100,
            "ushortVal": 200,
            "intVal": 300,
            "uintVal": 400,
            "longVal": 500,
            "ulongVal": 600,
            "floatVal": 1.5,
            "doubleVal": 2.5,
            "boolVal": False,
            "stringVal": "test",
            "bytesVal": b64,
        }
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        encoded = schema.encode_json("AllPrimitiveTypes", result)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["byteVal"] == 42
        assert decoded["sbyteVal"] == -1
        assert decoded["shortVal"] == 100
        assert decoded["ushortVal"] == 200
        assert decoded["intVal"] == 300
        assert decoded["uintVal"] == 400
        assert decoded["longVal"] == 500
        assert decoded["ulongVal"] == 600
        assert decoded["boolVal"] is False
        assert decoded["stringVal"] == "test"

    def test_all_primitives_null_bytes_roundtrip(self, schema):
        obj = _make_all_primitives_dict(byteVal=0, stringVal="x", bytesVal=None)
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["bytesVal"] is None
        encoded = schema.encode_json("AllPrimitiveTypes", result)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["bytesVal"] is None

    def test_all_primitives_null_string_roundtrip(self, schema):
        obj = _make_all_primitives_dict(stringVal=None)
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["stringVal"] is None

    def test_all_primitives_msgpack_transcode(self, schema):
        b64 = base64.b64encode(bytes([0xAA, 0xBB])).decode()
        obj = _make_all_primitives_dict(
            byteVal=42,
            boolVal=True,
            stringVal="test",
            bytesVal=b64,
        )
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        named = schema.decode("AllPrimitiveTypes", compact)
        recompact = schema.encode("AllPrimitiveTypes", named)
        result = schema.decode_json("AllPrimitiveTypes", recompact)
        assert result["byteVal"] == 42
        assert result["boolVal"] is True
        assert result["stringVal"] == "test"

    def test_property_keyed_json_roundtrip(self, schema):
        obj = {"id": 1, "name": "test", "active": True}
        compact = schema.encode_json("PropertyKeyed", obj)
        result = schema.decode_json("PropertyKeyed", compact)
        assert result["id"] == 1
        assert result["name"] == "test"
        assert result["active"] is True
        encoded = schema.encode_json("PropertyKeyed", result)
        decoded = schema.decode_json("PropertyKeyed", encoded)
        assert decoded["id"] == 1

    def test_property_keyed_msgpack_roundtrip(self, schema):
        obj = {"id": 1, "name": "test", "active": True}
        compact = schema.encode_json("PropertyKeyed", obj)
        named = schema.decode("PropertyKeyed", compact)
        recompact = schema.encode("PropertyKeyed", named)
        result = schema.decode_json("PropertyKeyed", recompact)
        assert result["id"] == 1

    def test_mixed_fields_and_properties_json_roundtrip(self, schema):
        obj = {"fieldValue": 42, "PropertyValue": "hello"}
        compact = schema.encode_json("MixedFieldsAndProperties", obj)
        result = schema.decode_json("MixedFieldsAndProperties", compact)
        assert result["fieldValue"] == 42
        assert result["PropertyValue"] == "hello"

    def test_write_primitive_byte_array_null(self, schema):
        obj = _make_all_primitives_dict(stringVal=None, bytesVal=None)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["bytesVal"] is None
        assert decoded["stringVal"] is None

    def test_write_primitive_byte_array_base64(self, schema):
        b64 = base64.b64encode(bytes([1, 2, 3, 4])).decode()
        obj = _make_all_primitives_dict(bytesVal=b64)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["bytesVal"] == b64

    def test_read_primitive_uint(self, schema):
        obj = _make_all_primitives_dict(uintVal=4294967295, stringVal="x")
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["uintVal"] == 4294967295

    def test_read_primitive_ulong(self, schema):
        obj = _make_all_primitives_dict(ulongVal=18446744073709551615, stringVal="x")
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["ulongVal"] == 18446744073709551615

    def test_read_primitive_bool(self, schema):
        obj = _make_all_primitives_dict(boolVal=True, stringVal="x")
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["boolVal"] is True

    def test_read_primitive_byte(self, schema):
        obj = _make_all_primitives_dict(byteVal=255, stringVal="x")
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["byteVal"] == 255

    def test_write_primitive_uint_encode(self, schema):
        obj = _make_all_primitives_dict(uintVal=4294967295)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["uintVal"] == 4294967295

    def test_write_primitive_ulong_encode(self, schema):
        obj = _make_all_primitives_dict(ulongVal=18446744073709551615)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["ulongVal"] == 18446744073709551615

    def test_write_primitive_byte_encode(self, schema):
        obj = _make_all_primitives_dict(byteVal=200)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["byteVal"] == 200

    def test_write_primitive_bool_encode(self, schema):
        obj = _make_all_primitives_dict(boolVal=True)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["boolVal"] is True

    def test_write_primitive_float_encode(self, schema):
        obj = _make_all_primitives_dict(floatVal=1.23)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert abs(decoded["floatVal"] - 1.23) < 0.01

    def test_write_primitive_double_encode(self, schema):
        obj = _make_all_primitives_dict(doubleVal=9.87654321)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert abs(decoded["doubleVal"] - 9.87654321) < 0.0001

    def test_write_primitive_short_encode(self, schema):
        obj = _make_all_primitives_dict(shortVal=-100)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["shortVal"] == -100

    def test_write_primitive_ushort_encode(self, schema):
        obj = _make_all_primitives_dict(ushortVal=50000)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["ushortVal"] == 50000

    def test_write_primitive_long_encode(self, schema):
        long_min = -9223372036854775808
        obj = _make_all_primitives_dict(longVal=long_min)
        encoded = schema.encode_json("AllPrimitiveTypes", obj)
        decoded = schema.decode_json("AllPrimitiveTypes", encoded)
        assert decoded["longVal"] == long_min

    def test_read_primitive_string_null(self, schema):
        obj = _make_all_primitives_dict(stringVal=None)
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["stringVal"] is None

    def test_decode_json_empty_bytes(self, schema):
        b64_empty = base64.b64encode(b"").decode()
        obj = _make_all_primitives_dict(stringVal="x", bytesVal=b64_empty)
        compact = schema.encode_json("AllPrimitiveTypes", obj)
        result = schema.decode_json("AllPrimitiveTypes", compact)
        assert result["bytesVal"] == b64_empty
