"""
Port of SchemaExtractorTests.cs to Python pybind.

Tests use schema.get_schema() which returns a dict with:
  name, isStringKeyed, fields (list of {memberName, typeName, intKey, stringKey, isProperty})
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def schema(native_lib_path: str, test_dll_path: str):
    from unity_msgpack_schema import MsgPackSchema

    s = MsgPackSchema(test_dll_path, lib_path=native_lib_path)
    yield s
    s.close()


class TestSchemaExtractor:
    def test_extracts_all_test_classes(self, schema):
        """All known TestSchemas classes should be found (>=10)."""
        assert schema.schema_count >= 10
        classes = schema.list_classes()
        short_names = {c.split(".")[-1] for c in classes}
        for name in [
            "SimpleItem",
            "IntKeyedItem",
            "Order",
            "OrderItem",
            "NullableFields",
            "IntKeyedDict",
            "ComplexNested",
            "Tag",
            "Level1",
            "Level2",
            "Level3",
            "WithHashSet",
            "SparseIntKeys",
            "WithEnumLike",
        ]:
            assert name in short_names, f"{name} not found in class list"

    def test_simple_item_string_keyed_correct_fields(self, schema):
        info = schema.get_schema("SimpleItem")
        assert info["isStringKeyed"] is True
        assert not info.get("isAbstract", False)
        assert len(info["fields"]) == 5

        by_name = {f["memberName"]: f for f in info["fields"]}
        assert by_name["id"]["typeName"] == "int"
        assert by_name["id"]["stringKey"] == "id"
        assert by_name["name"]["typeName"] == "string"
        assert by_name["active"]["typeName"] == "bool"
        assert by_name["score"]["typeName"] == "float"
        assert by_name["bigValue"]["typeName"] == "long"

    def test_int_keyed_item_integer_keys(self, schema):
        info = schema.get_schema("IntKeyedItem")
        assert info["isStringKeyed"] is False
        fields = sorted(info["fields"], key=lambda f: f["intKey"])
        assert len(fields) == 3
        assert fields[0]["intKey"] == 0
        assert fields[0]["typeName"] == "int"
        assert fields[1]["intKey"] == 1
        assert fields[1]["typeName"] == "string"
        assert fields[2]["intKey"] == 2
        assert fields[2]["typeName"] == "double"

    def test_order_nested_list_and_dictionary(self, schema):
        info = schema.get_schema("Order")
        assert info["isStringKeyed"] is True
        by_name = {f["memberName"]: f for f in info["fields"]}
        assert (
            "List" in by_name["items"]["typeName"]
            or "OrderItem" in by_name["items"]["typeName"]
        )
        assert (
            "[]" in by_name["tags"]["typeName"]
            or "Array" in by_name["tags"]["typeName"]
        )
        assert "Dictionary" in by_name["metadata"]["typeName"]

    def test_nullable_fields_detected(self, schema):
        info = schema.get_schema("NullableFields")
        by_name = {f["memberName"]: f for f in info["fields"]}
        assert "Nullable" in by_name["optionalInt"]["typeName"]
        # string is reference type — may be "string" or contain "String"
        opt_str_type = by_name["optionalStr"]["typeName"]
        assert opt_str_type == "string" or "String" in opt_str_type

    def test_complex_nested_dictionary_of_list_of_nested(self, schema):
        info = schema.get_schema("ComplexNested")
        by_name = {f["memberName"]: f for f in info["fields"]}
        assert "Dictionary" in by_name["tagGroups"]["typeName"]
        assert "Tag" in by_name["tagGroups"]["typeName"]
        assert "Tag" in by_name["singleTag"]["typeName"]
        assert "Tag" in by_name["tagArray"]["typeName"]
        assert (
            "[]" in by_name["tagArray"]["typeName"]
            or "Array" in by_name["tagArray"]["typeName"]
        )

    def test_sparse_int_keys_gaps_handled(self, schema):
        info = schema.get_schema("SparseIntKeys")
        assert info["isStringKeyed"] is False
        fields = sorted(info["fields"], key=lambda f: f["intKey"])
        assert len(fields) == 3
        assert fields[0]["intKey"] == 0
        assert fields[1]["intKey"] == 2
        assert fields[2]["intKey"] == 5

    def test_with_hashset_recognized_as_collection(self, schema):
        info = schema.get_schema("WithHashSet")
        by_name = {f["memberName"]: f for f in info["fields"]}
        assert "HashSet" in by_name["memberIds"]["typeName"]

    def test_find_schema_by_short_name(self, schema):
        info = schema.get_schema("SimpleItem")
        assert info["name"] == "SimpleItem"

    def test_find_schema_not_found_raises(self, schema):
        with pytest.raises(RuntimeError):
            schema.get_schema("NonExistentClass")

    def test_property_keyed_extracted(self, schema):
        """PropertyKeyed should be string-keyed with 3 fields."""
        # isProperty is not exposed in the ABI; only verify structural metadata
        info = schema.get_schema("PropertyKeyed")
        assert info["isStringKeyed"] is True
        assert len(info["fields"]) == 3

    def test_mixed_fields_and_properties_extracted(self, schema):
        """MixedFieldsAndProperties: int-keyed, 2 fields."""
        # isProperty is not exposed in the ABI; only verify structural metadata
        info = schema.get_schema("MixedFieldsAndProperties")
        assert info["isStringKeyed"] is False
        assert len(info["fields"]) == 2

    # SearchSchemas has no API equivalent -> skipped
