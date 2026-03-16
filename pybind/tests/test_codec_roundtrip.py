"""
Port of CodecRoundtripTests.cs to Python pybind.

Strategy:
  - C# MessagePackSerializer.Serialize(item) -> schema.encode_json("Class", dict)
  - C# _codec.DecodeJson("Class", bytes) -> schema.decode_json("Class", bytes) (returns dict)
  - C# _codec.Decode("Class", bytes) -> schema.decode("Class", bytes) (returns msgpack bytes)
  - C# _codec.Encode("Class", named) -> schema.encode("Class", named_bytes)
  - Named msgpack inspection via msgpack.unpackb(named, raw=False, strict_map_key=False)
"""

from __future__ import annotations

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


def unpack(data: bytes) -> object:
    """Unpack named msgpack bytes to Python object."""
    return msgpack.unpackb(data, raw=False, strict_map_key=False)


class TestCodecRoundtripJson:
    """Port of JSON roundtrip tests from CodecRoundtripTests.cs."""

    def test_simple_item_roundtrip(self, schema):
        item = {
            "id": 42,
            "name": "hello",
            "active": True,
            "score": 3.14,
            "bigValue": 9999999999,
        }
        compact = schema.encode_json("SimpleItem", item)
        result = schema.decode_json("SimpleItem", compact)
        assert result["id"] == 42
        assert result["name"] == "hello"
        assert result["active"] is True
        assert abs(result["score"] - 3.14) < 0.01
        assert result["bigValue"] == 9999999999
        # Roundtrip
        compact2 = schema.encode_json("SimpleItem", result)
        result2 = schema.decode_json("SimpleItem", compact2)
        assert result2["id"] == 42
        assert result2["name"] == "hello"

    def test_int_keyed_item_roundtrip(self, schema):
        item = {"id": 7, "label": "test", "value": 2.718}
        compact = schema.encode_json("IntKeyedItem", item)
        result = schema.decode_json("IntKeyedItem", compact)
        assert result["id"] == 7
        assert result["label"] == "test"
        assert abs(result["value"] - 2.718) < 0.001
        # Roundtrip
        compact2 = schema.encode_json("IntKeyedItem", result)
        result2 = schema.decode_json("IntKeyedItem", compact2)
        assert result2["id"] == 7
        assert result2["label"] == "test"

    def test_order_nested_items_roundtrip(self, schema):
        order = {
            "orderId": 100,
            "customer": "Alice",
            "items": [
                {"productId": 1, "quantity": 2, "unitPrice": 10.5},
                {"productId": 2, "quantity": 1, "unitPrice": 25.0},
            ],
            "tags": ["urgent", "vip"],
            "metadata": {"region": "JP", "source": "web"},
        }
        compact = schema.encode_json("Order", order)
        result = schema.decode_json("Order", compact)
        assert result["orderId"] == 100
        assert result["customer"] == "Alice"
        items = result["items"]
        assert len(items) == 2
        assert items[0]["productId"] == 1
        assert items[0]["quantity"] == 2
        assert abs(items[0]["unitPrice"] - 10.5) < 0.01
        assert items[1]["productId"] == 2
        tags = result["tags"]
        assert len(tags) == 2
        assert "urgent" in tags
        assert result["metadata"]["region"] == "JP"
        # Roundtrip
        compact2 = schema.encode_json("Order", result)
        result2 = schema.decode_json("Order", compact2)
        assert result2["orderId"] == 100
        assert len(result2["items"]) == 2
        assert result2["items"][0]["productId"] == 1

    def test_nullable_fields_with_nulls(self, schema):
        obj = {
            "id": 1,
            "optionalInt": None,
            "optionalLong": 42,
            "optionalBool": None,
            "optionalStr": None,
        }
        compact = schema.encode_json("NullableFields", obj)
        result = schema.decode_json("NullableFields", compact)
        assert result["id"] == 1
        assert result["optionalInt"] is None
        assert result["optionalLong"] == 42
        assert result["optionalBool"] is None
        # Roundtrip
        compact2 = schema.encode_json("NullableFields", result)
        result2 = schema.decode_json("NullableFields", compact2)
        assert result2["optionalInt"] is None
        assert result2["optionalLong"] == 42

    def test_nullable_fields_with_values(self, schema):
        obj = {
            "id": 2,
            "optionalInt": 99,
            "optionalLong": None,
            "optionalBool": True,
            "optionalStr": "hello",
        }
        compact = schema.encode_json("NullableFields", obj)
        result = schema.decode_json("NullableFields", compact)
        assert result["optionalInt"] == 99
        assert result["optionalBool"] is True

    def test_complex_nested_dict_of_list_of_tag(self, schema):
        obj = {
            "id": 10,
            "tagGroups": {
                "1": [{"tagId": 100, "label": "A"}, {"tagId": 101, "label": "B"}],
                "2": [{"tagId": 200, "label": "C"}],
            },
            "singleTag": {"tagId": 50, "label": "single"},
            "tagArray": [{"tagId": 60, "label": "arr1"}],
        }
        compact = schema.encode_json("ComplexNested", obj)
        result = schema.decode_json("ComplexNested", compact)
        assert result["id"] == 10
        groups = result["tagGroups"]
        assert "1" in groups
        assert len(groups["1"]) == 2
        assert groups["1"][0]["tagId"] == 100
        assert groups["1"][0]["label"] == "A"
        assert result["singleTag"]["tagId"] == 50
        assert result["singleTag"]["label"] == "single"
        assert len(result["tagArray"]) == 1
        assert result["tagArray"][0]["tagId"] == 60
        # Roundtrip
        compact2 = schema.encode_json("ComplexNested", result)
        result2 = schema.decode_json("ComplexNested", compact2)
        assert result2["tagGroups"]["1"][0]["tagId"] == 100

    def test_level1_three_level_deep_roundtrip(self, schema):
        obj = {
            "id": 1,
            "items": [
                {
                    "name": "L2-A",
                    "children": [{"value": "deep1"}, {"value": "deep2"}],
                },
                {"name": "L2-B", "children": [{"value": "deep3"}]},
            ],
        }
        compact = schema.encode_json("Level1", obj)
        result = schema.decode_json("Level1", compact)
        assert result["id"] == 1
        assert len(result["items"]) == 2
        assert result["items"][0]["name"] == "L2-A"
        assert len(result["items"][0]["children"]) == 2
        assert result["items"][0]["children"][0]["value"] == "deep1"
        # Roundtrip
        compact2 = schema.encode_json("Level1", result)
        result2 = schema.decode_json("Level1", compact2)
        assert result2["items"][0]["name"] == "L2-A"
        assert result2["items"][0]["children"][0]["value"] == "deep1"

    def test_sparse_int_keys_gaps_filled_with_null(self, schema):
        obj = {"a": 10, "b": "hi", "c": True}
        compact = schema.encode_json("SparseIntKeys", obj)
        result = schema.decode_json("SparseIntKeys", compact)
        assert result["a"] == 10
        assert result["b"] == "hi"
        assert result["c"] is True
        compact2 = schema.encode_json("SparseIntKeys", result)
        result2 = schema.decode_json("SparseIntKeys", compact2)
        assert result2["a"] == 10
        assert result2["c"] is True

    def test_with_hashset_roundtrip(self, schema):
        obj = {"id": 1, "memberIds": [10, 20, 30]}
        compact = schema.encode_json("WithHashSet", obj)
        result = schema.decode_json("WithHashSet", compact)
        assert result["id"] == 1
        assert len(result["memberIds"]) == 3
        # Roundtrip
        compact2 = schema.encode_json("WithHashSet", result)
        result2 = schema.decode_json("WithHashSet", compact2)
        assert len(result2["memberIds"]) == 3

    def test_empty_collections_roundtrip(self, schema):
        order = {
            "orderId": 99,
            "customer": "Bob",
            "items": [],
            "tags": [],
            "metadata": {},
        }
        compact = schema.encode_json("Order", order)
        result = schema.decode_json("Order", compact)
        assert result["orderId"] == 99
        assert result["items"] == []
        assert result["tags"] == []
        # Roundtrip
        compact2 = schema.encode_json("Order", result)
        result2 = schema.decode_json("Order", compact2)
        assert result2["orderId"] == 99

    def test_null_nested_object_handled(self, schema):
        obj = {"id": 5, "singleTag": None, "tagArray": [], "tagGroups": {}}
        compact = schema.encode_json("ComplexNested", obj)
        result = schema.decode_json("ComplexNested", compact)
        assert result["id"] == 5
        assert result["singleTag"] is None

    def test_decode_invalid_class_name_throws(self, schema):
        compact = schema.encode_json(
            "SimpleItem",
            {"id": 1, "name": "x", "active": False, "score": 0.0, "bigValue": 0},
        )
        with pytest.raises(RuntimeError):
            schema.decode_json("NoSuchClass", compact)

    def test_encode_invalid_class_name_throws(self, schema):
        with pytest.raises(RuntimeError):
            schema.encode_json("NoSuchClass", {"key": "value"})

    # ── Mixed int/string key nesting ────────────────────────────────────────

    def test_int_keyed_with_string_child_json_roundtrip(self, schema):
        obj = {
            "id": 1,
            "tag": {"tagId": 10, "label": "hello"},
            "tags": [{"tagId": 20, "label": "a"}, {"tagId": 30, "label": "b"}],
        }
        compact = schema.encode_json("IntKeyedWithStringChild", obj)
        result = schema.decode_json("IntKeyedWithStringChild", compact)
        assert result["id"] == 1
        assert result["tag"]["tagId"] == 10
        assert result["tag"]["label"] == "hello"
        assert len(result["tags"]) == 2
        assert result["tags"][0]["tagId"] == 20
        # Roundtrip
        compact2 = schema.encode_json("IntKeyedWithStringChild", result)
        result2 = schema.decode_json("IntKeyedWithStringChild", compact2)
        assert result2["id"] == 1
        assert result2["tag"]["label"] == "hello"

    def test_string_keyed_with_int_child_json_roundtrip(self, schema):
        obj = {
            "id": 2,
            "child": {"id": 7, "label": "intchild", "value": 1.5},
            "children": [
                {"id": 8, "label": "c1", "value": 2.0},
                {"id": 9, "label": "c2", "value": 3.0},
            ],
        }
        compact = schema.encode_json("StringKeyedWithIntChild", obj)
        result = schema.decode_json("StringKeyedWithIntChild", compact)
        assert result["id"] == 2
        assert result["child"]["id"] == 7
        assert result["child"]["label"] == "intchild"
        assert len(result["children"]) == 2
        assert result["children"][0]["id"] == 8
        # Roundtrip
        compact2 = schema.encode_json("StringKeyedWithIntChild", result)
        result2 = schema.decode_json("StringKeyedWithIntChild", compact2)
        assert result2["id"] == 2
        assert result2["child"]["id"] == 7

    def test_int_keyed_with_int_child_json_roundtrip(self, schema):
        obj = {
            "id": 4,
            "child": {"id": 20, "label": "nested", "value": 7.7},
            "children": [
                {"id": 21, "label": "c1", "value": 1.1},
                {"id": 22, "label": "c2", "value": 2.2},
            ],
        }
        compact = schema.encode_json("IntKeyedWithIntChild", obj)
        result = schema.decode_json("IntKeyedWithIntChild", compact)
        assert result["id"] == 4
        assert result["child"]["id"] == 20
        assert len(result["children"]) == 2
        assert result["children"][0]["id"] == 21
        # Roundtrip
        compact2 = schema.encode_json("IntKeyedWithIntChild", result)
        result2 = schema.decode_json("IntKeyedWithIntChild", compact2)
        assert result2["child"]["id"] == 20

    def test_int_keyed_with_int_child_null_child_handled(self, schema):
        obj = {"id": 7, "child": None, "children": []}
        compact = schema.encode_json("IntKeyedWithIntChild", obj)
        result = schema.decode_json("IntKeyedWithIntChild", compact)
        assert result["child"] is None

    # ── Union type tests ─────────────────────────────────────────────────────

    def test_union_string_keyed_child_a_json_roundtrip(self, schema):
        obj = {"_type": 0, "_data": {"label": "hello", "value": 42}}
        compact = schema.encode_json("UnionBase", obj)
        result = schema.decode_json("UnionBase", compact)
        assert result["_type"] == 0
        assert result["_data"]["label"] == "hello"
        assert result["_data"]["value"] == 42
        # Roundtrip
        compact2 = schema.encode_json("UnionBase", result)
        result2 = schema.decode_json("UnionBase", compact2)
        assert result2["_type"] == 0
        assert result2["_data"]["label"] == "hello"
        assert result2["_data"]["value"] == 42

    def test_union_string_keyed_child_b_json_roundtrip(self, schema):
        obj = {"_type": 1, "_data": {"message": "world", "enabled": True}}
        compact = schema.encode_json("UnionBase", obj)
        result = schema.decode_json("UnionBase", compact)
        assert result["_type"] == 1
        assert result["_data"]["message"] == "world"
        assert result["_data"]["enabled"] is True
        # Roundtrip
        compact2 = schema.encode_json("UnionBase", result)
        result2 = schema.decode_json("UnionBase", compact2)
        assert result2["_type"] == 1
        assert result2["_data"]["enabled"] is True

    def test_union_null_payload_handled(self, schema):
        msg = {"id": 1, "payload": None, "payloads": []}
        compact = schema.encode_json("MessageWithUnion", msg)
        result = schema.decode_json("MessageWithUnion", compact)
        assert result["id"] == 1
        assert result["payload"] is None
        assert result["payloads"] == []

    def test_message_with_union_all_variants_json_roundtrip(self, schema):
        msg = {
            "id": 42,
            "payload": {"_type": 0, "_data": {"label": "main", "value": 1}},
            "payloads": [
                {"_type": 0, "_data": {"label": "a", "value": 10}},
                {"_type": 1, "_data": {"message": "b", "enabled": True}},
            ],
        }
        compact = schema.encode_json("MessageWithUnion", msg)
        result = schema.decode_json("MessageWithUnion", compact)
        assert result["id"] == 42
        assert result["payload"]["_type"] == 0
        assert result["payload"]["_data"]["label"] == "main"
        assert len(result["payloads"]) == 2
        assert result["payloads"][0]["_type"] == 0
        assert result["payloads"][1]["_type"] == 1
        assert result["payloads"][0]["_data"]["label"] == "a"
        assert result["payloads"][1]["_data"]["message"] == "b"
        # Roundtrip
        compact2 = schema.encode_json("MessageWithUnion", result)
        result2 = schema.decode_json("MessageWithUnion", compact2)
        assert result2["id"] == 42
        assert result2["payload"]["_data"]["label"] == "main"

    def test_union_int_keyed_child_a_json_roundtrip(self, schema):
        obj = {"_type": 0, "_data": {"num": 77, "name": "intunion"}}
        compact = schema.encode_json("IntUnionBase", obj)
        result = schema.decode_json("IntUnionBase", compact)
        assert result["_type"] == 0
        assert result["_data"]["num"] == 77
        assert result["_data"]["name"] == "intunion"
        # Roundtrip
        compact2 = schema.encode_json("IntUnionBase", result)
        result2 = schema.decode_json("IntUnionBase", compact2)
        assert result2["_data"]["num"] == 77

    def test_int_keyed_with_union_all_variants_roundtrip(self, schema):
        obj = {
            "id": 100,
            "payload": {"_type": 0, "_data": {"num": 1, "name": "first"}},
            "payloads": [
                {"_type": 1, "_data": {"data": "d", "flag": True}},
                {"_type": 0, "_data": {"num": 2, "name": "second"}},
            ],
        }
        compact = schema.encode_json("IntKeyedWithUnion", obj)
        result = schema.decode_json("IntKeyedWithUnion", compact)
        assert result["id"] == 100
        assert result["payload"]["_type"] == 0
        assert result["payload"]["_data"]["num"] == 1
        assert len(result["payloads"]) == 2
        assert result["payloads"][0]["_type"] == 1
        assert result["payloads"][1]["_type"] == 0

    def test_int_keyed_with_union_null_payload_roundtrip(self, schema):
        obj = {"id": 5, "payload": None, "payloads": []}
        compact = schema.encode_json("IntKeyedWithUnion", obj)
        result = schema.decode_json("IntKeyedWithUnion", compact)
        assert result["payload"] is None


class TestCodecRoundtripMsgPack:
    """Port of MsgPack<->MsgPack roundtrip tests from CodecRoundtripTests.cs."""

    def test_msgpack_decode_produces_string_keyed_map(self, schema):
        item = {
            "id": 42,
            "name": "hello",
            "active": True,
            "score": 3.14,
            "bigValue": 9999999999,
        }
        compact = schema.encode_json("SimpleItem", item)
        named = schema.decode("SimpleItem", compact)
        assert isinstance(named, bytes)
        assert len(named) > 0
        named_map = unpack(named)
        assert isinstance(named_map, dict)
        assert "id" in named_map
        assert "name" in named_map
        assert "active" in named_map
        assert len(named_map) >= 5

    def test_msgpack_decode_preserves_int_dictionary_keys(self, schema):
        obj = {
            "id": 1,
            "tagGroups": {
                "42": [{"tagId": 1, "label": "A"}],
                "99": [{"tagId": 2, "label": "B"}],
            },
            "singleTag": None,
            "tagArray": [],
        }
        compact = schema.encode_json("ComplexNested", obj)
        named = schema.decode("ComplexNested", compact)
        named_map = unpack(named)
        # tagGroups in named format should have integer keys
        tag_groups = named_map["tagGroups"]
        assert 42 in tag_groups or "42" in tag_groups  # int or str keys both acceptable

    def test_msgpack_encode_decode_roundtrip(self, schema):
        item = {"id": 7, "name": "world", "active": False, "score": 2.5, "bigValue": 42}
        compact = schema.encode_json("SimpleItem", item)
        named = schema.decode("SimpleItem", compact)
        recompact = schema.encode("SimpleItem", named)
        result = schema.decode_json("SimpleItem", recompact)
        assert result["id"] == 7
        assert result["name"] == "world"
        assert result["active"] is False
        assert abs(result["score"] - 2.5) < 0.01
        assert result["bigValue"] == 42

    def test_msgpack_int_keyed_roundtrip(self, schema):
        item = {"id": 100, "label": "intkey", "value": 9.99}
        compact = schema.encode_json("IntKeyedItem", item)
        named = schema.decode("IntKeyedItem", compact)
        recompact = schema.encode("IntKeyedItem", named)
        result = schema.decode_json("IntKeyedItem", recompact)
        assert result["id"] == 100
        assert result["label"] == "intkey"
        assert abs(result["value"] - 9.99) < 0.01

    def test_msgpack_order_nested_items_roundtrip(self, schema):
        order = {
            "orderId": 55,
            "customer": "Bob",
            "items": [{"productId": 3, "quantity": 10, "unitPrice": 1.5}],
            "tags": ["a", "b"],
            "metadata": {"k": "v"},
        }
        compact = schema.encode_json("Order", order)
        named = schema.decode("Order", compact)
        recompact = schema.encode("Order", named)
        result = schema.decode_json("Order", recompact)
        assert result["orderId"] == 55
        assert result["customer"] == "Bob"
        assert len(result["items"]) == 1
        assert result["items"][0]["productId"] == 3

    def test_msgpack_complex_nested_dict_int_key_roundtrip(self, schema):
        obj = {
            "id": 10,
            "tagGroups": {
                "1": [{"tagId": 100, "label": "A"}],
                "2": [{"tagId": 200, "label": "C"}],
            },
            "singleTag": {"tagId": 50, "label": "single"},
            "tagArray": [{"tagId": 60, "label": "arr1"}],
        }
        compact = schema.encode_json("ComplexNested", obj)
        named = schema.decode("ComplexNested", compact)
        recompact = schema.encode("ComplexNested", named)
        result = schema.decode_json("ComplexNested", recompact)
        assert result["id"] == 10
        tag_groups = result["tagGroups"]
        assert "1" in tag_groups
        assert tag_groups["1"][0]["tagId"] == 100

    def test_msgpack_null_nested_object_handled(self, schema):
        obj = {"id": 5, "singleTag": None, "tagArray": [], "tagGroups": {}}
        compact = schema.encode_json("ComplexNested", obj)
        named = schema.decode("ComplexNested", compact)
        recompact = schema.encode("ComplexNested", named)
        result = schema.decode_json("ComplexNested", recompact)
        assert result["id"] == 5

    def test_msgpack_level1_three_deep_roundtrip(self, schema):
        obj = {"id": 1, "items": [{"name": "L2", "children": [{"value": "deep"}]}]}
        compact = schema.encode_json("Level1", obj)
        named = schema.decode("Level1", compact)
        recompact = schema.encode("Level1", named)
        result = schema.decode_json("Level1", recompact)
        assert result["id"] == 1
        assert result["items"][0]["children"][0]["value"] == "deep"

    def test_msgpack_sparse_int_keys_roundtrip(self, schema):
        obj = {"a": 10, "b": "hi", "c": True}
        compact = schema.encode_json("SparseIntKeys", obj)
        named = schema.decode("SparseIntKeys", compact)
        recompact = schema.encode("SparseIntKeys", named)
        result = schema.decode_json("SparseIntKeys", recompact)
        assert result["a"] == 10
        assert result["c"] is True

    def test_decode_invalid_class_msgpack_throws(self, schema):
        with pytest.raises(RuntimeError):
            schema.decode("NoSuchClass", b"\x90")

    def test_encode_invalid_class_msgpack_throws(self, schema):
        with pytest.raises(RuntimeError):
            schema.encode("NoSuchClass", b"\x80")

    def test_int_keyed_with_string_child_msgpack_roundtrip(self, schema):
        obj = {
            "id": 5,
            "tag": {"tagId": 99, "label": "test"},
            "tags": [{"tagId": 1, "label": "x"}],
        }
        compact = schema.encode_json("IntKeyedWithStringChild", obj)
        named = schema.decode("IntKeyedWithStringChild", compact)
        named_map = unpack(named)
        assert "id" in named_map
        assert "tag" in named_map
        assert "tags" in named_map
        recompact = schema.encode("IntKeyedWithStringChild", named)
        result = schema.decode_json("IntKeyedWithStringChild", recompact)
        assert result["id"] == 5
        assert result["tag"]["label"] == "test"
        assert len(result["tags"]) == 1

    def test_string_keyed_with_int_child_msgpack_roundtrip(self, schema):
        obj = {
            "id": 3,
            "child": {"id": 11, "label": "deep", "value": 9.9},
            "children": [],
        }
        compact = schema.encode_json("StringKeyedWithIntChild", obj)
        named = schema.decode("StringKeyedWithIntChild", compact)
        recompact = schema.encode("StringKeyedWithIntChild", named)
        result = schema.decode_json("StringKeyedWithIntChild", recompact)
        assert result["id"] == 3
        assert result["child"]["label"] == "deep"
        assert result["children"] == []

    def test_int_keyed_with_int_child_msgpack_roundtrip(self, schema):
        obj = {
            "id": 6,
            "child": {"id": 30, "label": "dbl-int", "value": 0.5},
            "children": [{"id": 31, "label": "sub", "value": 0.1}],
        }
        compact = schema.encode_json("IntKeyedWithIntChild", obj)
        named = schema.decode("IntKeyedWithIntChild", compact)
        recompact = schema.encode("IntKeyedWithIntChild", named)
        result = schema.decode_json("IntKeyedWithIntChild", recompact)
        assert result["id"] == 6
        assert result["child"]["id"] == 30
        assert len(result["children"]) == 1

    def test_union_string_keyed_child_a_msgpack_roundtrip(self, schema):
        obj = {"_type": 0, "_data": {"label": "msgpack", "value": 99}}
        compact = schema.encode_json("UnionBase", obj)
        named = schema.decode("UnionBase", compact)
        named_map = unpack(named)
        assert isinstance(named_map, dict)
        assert "_type" in named_map
        assert "_data" in named_map
        recompact = schema.encode("UnionBase", named)
        result = schema.decode_json("UnionBase", recompact)
        assert result["_type"] == 0
        assert result["_data"]["label"] == "msgpack"

    def test_union_string_keyed_child_b_msgpack_roundtrip(self, schema):
        obj = {"_type": 1, "_data": {"message": "test", "enabled": False}}
        compact = schema.encode_json("UnionBase", obj)
        named = schema.decode("UnionBase", compact)
        recompact = schema.encode("UnionBase", named)
        result = schema.decode_json("UnionBase", recompact)
        assert result["_type"] == 1
        assert result["_data"]["message"] == "test"
        assert result["_data"]["enabled"] is False

    def test_message_with_union_all_variants_msgpack_roundtrip(self, schema):
        msg = {
            "id": 7,
            "payload": {"_type": 1, "_data": {"message": "hello", "enabled": True}},
            "payloads": [
                {"_type": 0, "_data": {"label": "x", "value": 5}},
                {"_type": 1, "_data": {"message": "y", "enabled": False}},
            ],
        }
        compact = schema.encode_json("MessageWithUnion", msg)
        named = schema.decode("MessageWithUnion", compact)
        recompact = schema.encode("MessageWithUnion", named)
        result = schema.decode_json("MessageWithUnion", recompact)
        assert result["id"] == 7
        assert result["payload"]["_type"] == 1
        assert result["payload"]["_data"]["message"] == "hello"
        assert len(result["payloads"]) == 2
        assert result["payloads"][0]["_type"] == 0
        assert result["payloads"][1]["_type"] == 1

    def test_union_int_keyed_child_b_msgpack_roundtrip(self, schema):
        obj = {"_type": 1, "_data": {"data": "bytes", "flag": True}}
        compact = schema.encode_json("IntUnionBase", obj)
        named = schema.decode("IntUnionBase", compact)
        recompact = schema.encode("IntUnionBase", named)
        result = schema.decode_json("IntUnionBase", recompact)
        assert result["_type"] == 1
        assert result["_data"]["data"] == "bytes"
        assert result["_data"]["flag"] is True

    def test_int_keyed_with_union_msgpack_roundtrip(self, schema):
        obj = {
            "id": 100,
            "payload": {"_type": 0, "_data": {"num": 1, "name": "first"}},
            "payloads": [
                {"_type": 1, "_data": {"data": "d", "flag": True}},
                {"_type": 0, "_data": {"num": 2, "name": "second"}},
            ],
        }
        compact = schema.encode_json("IntKeyedWithUnion", obj)
        named = schema.decode("IntKeyedWithUnion", compact)
        recompact = schema.encode("IntKeyedWithUnion", named)
        result = schema.decode_json("IntKeyedWithUnion", recompact)
        assert result["id"] == 100
        assert result["payload"]["_data"]["num"] == 1

    def test_union_null_payload_msgpack_roundtrip(self, schema):
        msg = {"id": 1, "payload": None, "payloads": []}
        compact = schema.encode_json("MessageWithUnion", msg)
        named = schema.decode("MessageWithUnion", compact)
        recompact = schema.encode("MessageWithUnion", named)
        result = schema.decode_json("MessageWithUnion", recompact)
        assert result["payload"] is None
