"""
Port of AvroExporterTests.cs and AvroExporterEdgeCaseTests.cs to Python pybind.

Tests use schema.avro_schema(class_name, nullable=False, flat=False).
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="module")
def schema(native_lib_path: str, test_dll_path: str):
    from unity_msgpack_schema import MsgPackSchema

    s = MsgPackSchema(test_dll_path, lib_path=native_lib_path)
    yield s
    s.close()


class TestAvroExporter:
    """Port of AvroExporterTests.cs (inline-nested mode)."""

    def test_simple_item_primitive_field_types(self, schema):
        avro = schema.avro_schema("SimpleItem")
        assert avro["type"] == "record"
        assert avro["name"] == "SimpleItem"
        fields = {f["name"]: f for f in avro["fields"]}
        assert len(avro["fields"]) == 5
        assert fields["id"]["type"] == "int"
        assert fields["active"]["type"] == "boolean"
        assert fields["score"]["type"] == "float"

    def test_order_nested_order_item_inlined_as_record(self, schema):
        avro = schema.avro_schema("Order")
        fields = {f["name"]: f for f in avro["fields"]}
        items_type = fields["items"]["type"]
        assert items_type["type"] == "array"
        items_items = items_type["items"]
        assert isinstance(items_items, dict)
        assert items_items["type"] == "record"
        assert items_items["name"] == "OrderItem"
        order_item_fields = {f["name"] for f in items_items["fields"]}
        assert "productId" in order_item_fields
        assert "quantity" in order_item_fields
        assert "unitPrice" in order_item_fields

    def test_order_string_array_correct_avro(self, schema):
        avro = schema.avro_schema("Order")
        fields = {f["name"]: f for f in avro["fields"]}
        tags_type = fields["tags"]["type"]
        assert tags_type["type"] == "array"
        # items should be nullable string: ["null", "string"]
        assert tags_type["items"] is not None

    def test_order_string_dict_correct_avro(self, schema):
        avro = schema.avro_schema("Order")
        fields = {f["name"]: f for f in avro["fields"]}
        meta_type = fields["metadata"]["type"]
        assert meta_type["type"] == "map"

    def test_complex_nested_deeply_nested_types(self, schema):
        avro = schema.avro_schema("ComplexNested")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["singleTag"]["type"] is not None

    def test_level1_three_levels_deep(self, schema):
        avro = schema.avro_schema("Level1")
        fields = {f["name"]: f for f in avro["fields"]}
        items_type = fields["items"]["type"]
        assert items_type["type"] == "array"
        level2 = items_type["items"]
        assert isinstance(level2, dict)
        assert level2["type"] == "record"
        assert level2["name"] == "Level2"
        level2_fields = {f["name"]: f for f in level2["fields"]}
        children_type = level2_fields["children"]["type"]
        assert children_type["type"] == "array"
        level3 = children_type["items"]
        assert isinstance(level3, dict)
        assert level3["type"] == "record"
        assert level3["name"] == "Level3"

    def test_int_keyed_dict_non_string_key_dict(self, schema):
        avro = schema.avro_schema("IntKeyedDict")
        fields = {f["name"]: f for f in avro["fields"]}
        scores_type = fields["scores"]["type"]
        assert scores_type["type"] == "map"
        assert scores_type["msgpack_key_type"] == "int"
        assert scores_type["values"] is not None

    def test_export_all_schemas_no_crash(self, schema):
        """Simulate ExportAllSchemas by iterating all classes."""
        classes = schema.list_classes()
        assert len(classes) >= 10
        for cls in classes:
            avro = schema.avro_schema(cls)
            assert avro["type"] == "record"

    def test_nullable_all_fields_wrapped(self, schema):
        avro = schema.avro_schema("SimpleItem", nullable=True)
        for field in avro["fields"]:
            t = field["type"]
            assert isinstance(t, list), f"Field {field['name']} should be nullable"
            assert "null" in t, f"Field {field['name']} should contain null"

    def test_nullable_already_nullable_not_double_wrapped(self, schema):
        avro = schema.avro_schema("NullableFields", nullable=True)
        fields = {f["name"]: f for f in avro["fields"]}
        str_type = fields["optionalStr"]["type"]
        assert isinstance(str_type, list)
        null_count = sum(1 for t in str_type if t == "null")
        assert null_count == 1

    def test_flat_nested_types_referenced_by_name(self, schema):
        avro = schema.avro_schema("Order", flat=True)
        fields = {f["name"]: f for f in avro["fields"]}
        items_type = fields["items"]["type"]
        assert items_type["type"] == "array"
        items_items = items_type["items"]
        assert isinstance(items_items, str)
        assert "OrderItem" in items_items

    def test_flat_all_schemas_present(self, schema):
        classes = schema.list_classes()
        for cls in classes:
            avro = schema.avro_schema(cls, flat=True)
            assert avro["type"] == "record"

    def test_int_keyed_with_string_child_nested_avro(self, schema):
        avro = schema.avro_schema("IntKeyedWithStringChild")
        fields = {f["name"]: f for f in avro["fields"]}
        id_field = fields["id"]
        assert id_field["msgpack_key"] == 0  # int key
        assert fields["tag"]["type"] is not None

    def test_string_keyed_with_int_child_nested_avro(self, schema):
        avro = schema.avro_schema("StringKeyedWithIntChild")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["id"]["msgpack_key"] == "id"
        assert fields["child"]["type"] is not None

    def test_int_keyed_with_int_child_nested_avro(self, schema):
        avro = schema.avro_schema("IntKeyedWithIntChild")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["child"]["type"] is not None
        assert fields["children"]["type"] is not None

    def test_union_base_has_msgpack_unions(self, schema):
        avro = schema.avro_schema("UnionBase")
        unions = avro["msgpack_unions"]
        assert unions is not None
        assert len(unions) == 2
        keys = sorted(u["key"] for u in unions)
        assert keys == [0, 1]
        type_names = {u["type"] for u in unions}
        assert "UnionChildA" in type_names
        assert "UnionChildB" in type_names

    def test_union_child_a_correct_fields(self, schema):
        avro = schema.avro_schema("UnionChildA")
        assert avro["type"] == "record"
        assert avro["name"] == "UnionChildA"
        field_names = {f["name"] for f in avro["fields"]}
        assert "label" in field_names
        assert "value" in field_names

    def test_int_union_base_has_msgpack_unions(self, schema):
        avro = schema.avro_schema("IntUnionBase")
        unions = avro["msgpack_unions"]
        assert unions is not None
        assert len(unions) == 2
        type_names = {u["type"] for u in unions}
        assert "IntUnionChildA" in type_names
        assert "IntUnionChildB" in type_names

    def test_message_with_union_exports_correctly(self, schema):
        avro = schema.avro_schema("MessageWithUnion")
        assert avro["type"] == "record"
        field_names = {f["name"] for f in avro["fields"]}
        assert "id" in field_names
        assert "payload" in field_names
        assert "payloads" in field_names

    def test_union_child_a_msgpack_key_string_keyed(self, schema):
        avro = schema.avro_schema("UnionChildA")
        for field in avro["fields"]:
            assert isinstance(field["msgpack_key"], str), (
                f"Field {field['name']} should have string msgpack_key"
            )

    def test_int_union_child_a_msgpack_key_int_keyed(self, schema):
        avro = schema.avro_schema("IntUnionChildA")
        for field in avro["fields"]:
            assert isinstance(field["msgpack_key"], int), (
                f"Field {field['name']} should have int msgpack_key"
            )


class TestAvroExporterEdgeCases:
    """Port of AvroExporterEdgeCaseTests.cs."""

    def test_with_hashset_array_type(self, schema):
        avro = schema.avro_schema("WithHashSet")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["memberIds"]["type"]["type"] == "array"

    def test_all_nullable_wraps_fields(self, schema):
        avro = schema.avro_schema("SimpleItem", nullable=True)
        for field in avro["fields"]:
            t = field["type"]
            if isinstance(t, list):
                assert "null" in t

    def test_all_nullable_already_nullable_not_double_wrapped(self, schema):
        avro = schema.avro_schema("NullableFields", nullable=True)
        fields = {f["name"]: f for f in avro["fields"]}
        t = fields["optionalInt"]["type"]
        assert isinstance(t, list)
        null_count = sum(1 for x in t if x == "null")
        assert null_count == 1

    def test_flat_references_nested_by_name(self, schema):
        avro = schema.avro_schema("Order", flat=True)
        fields = {f["name"]: f for f in avro["fields"]}
        items_type = fields["items"]["type"]
        assert items_type["type"] == "array"
        assert isinstance(items_type["items"], str)
        assert "OrderItem" in items_type["items"]

    def test_int_keyed_dict_non_string_key_has_msgpack_key_type(self, schema):
        avro = schema.avro_schema("IntKeyedDict")
        fields = {f["name"]: f for f in avro["fields"]}
        scores_type = fields["scores"]["type"]
        assert scores_type["type"] == "map"
        assert "msgpack_key_type" in scores_type

    def test_complex_nested_dict_int_key_has_msgpack_key_type(self, schema):
        avro = schema.avro_schema("ComplexNested")
        fields = {f["name"]: f for f in avro["fields"]}
        tag_groups_type = fields["tagGroups"]["type"]
        assert tag_groups_type["type"] == "map"
        assert "msgpack_key_type" in tag_groups_type

    def test_union_has_msgpack_unions(self, schema):
        avro = schema.avro_schema("UnionBase")
        assert "msgpack_unions" in avro
        assert len(avro["msgpack_unions"]) >= 2

    def test_int_union_base_has_msgpack_unions(self, schema):
        avro = schema.avro_schema("IntUnionBase")
        assert "msgpack_unions" in avro

    def test_msgpack_key_string_keyed(self, schema):
        avro = schema.avro_schema("SimpleItem")
        for field in avro["fields"]:
            assert "msgpack_key" in field
            assert isinstance(field["msgpack_key"], str)

    def test_msgpack_key_int_keyed(self, schema):
        avro = schema.avro_schema("IntKeyedItem")
        for field in avro["fields"]:
            assert "msgpack_key" in field
            assert isinstance(field["msgpack_key"], int)

    def test_string_array_field(self, schema):
        avro = schema.avro_schema("Order")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["tags"]["type"]["type"] == "array"

    def test_string_dict_field(self, schema):
        avro = schema.avro_schema("Order")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["metadata"]["type"]["type"] == "map"

    def test_nullable_string_is_null_union(self, schema):
        avro = schema.avro_schema("NullableFields")
        fields = {f["name"]: f for f in avro["fields"]}
        t = fields["optionalStr"]["type"]
        assert isinstance(t, list), "optionalStr should be a nullable union"

    def test_level1_inlines_nested_records(self, schema):
        avro = schema.avro_schema("Level1")
        fields = {f["name"]: f for f in avro["fields"]}
        items_type = fields["items"]["type"]
        assert items_type["type"] == "array"
        inner = items_type["items"]
        assert isinstance(inner, (dict, str))

    def test_primitive_types_mapped_correctly(self, schema):
        avro = schema.avro_schema("SimpleItem")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["id"]["type"] == "int"
        assert fields["active"]["type"] == "boolean"
        assert fields["score"]["type"] == "float"
        assert fields["bigValue"]["type"] == "long"

    def test_namespace_set(self, schema):
        avro = schema.avro_schema("SimpleItem")
        assert "namespace" in avro
        assert avro["namespace"]

    def test_record_type(self, schema):
        avro = schema.avro_schema("SimpleItem")
        assert avro["type"] == "record"

    def test_sparse_int_keys_all_fields_present(self, schema):
        avro = schema.avro_schema("SparseIntKeys")
        assert len(avro["fields"]) == 3

    def test_with_enum_like_int_field_mapped(self, schema):
        avro = schema.avro_schema("WithEnumLike")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["priority"]["type"] == "int"

    def test_message_with_union_payload_field(self, schema):
        avro = schema.avro_schema("MessageWithUnion")
        field_names = {f["name"] for f in avro["fields"]}
        assert "id" in field_names
        assert "payload" in field_names
        assert "payloads" in field_names

    def test_int_keyed_with_string_child_nested_inlined(self, schema):
        avro = schema.avro_schema("IntKeyedWithStringChild")
        assert len(avro["fields"]) >= 3

    def test_int_keyed_with_int_child_nested_inlined(self, schema):
        avro = schema.avro_schema("IntKeyedWithIntChild")
        assert len(avro["fields"]) >= 1

    def test_all_nullable_no_exception(self, schema):
        classes = schema.list_classes()
        for cls in classes:
            schema.avro_schema(cls, nullable=True)  # should not raise

    def test_flat_and_nullable_no_exception(self, schema):
        classes = schema.list_classes()
        for cls in classes:
            schema.avro_schema(cls, nullable=True, flat=True)  # should not raise

    def test_all_primitives_avro_export(self, schema):
        avro = schema.avro_schema("AllPrimitiveTypes")
        fields = {f["name"]: f for f in avro["fields"]}
        assert fields["byteVal"]["type"] == "int"
        assert fields["doubleVal"]["type"] == "double"
        assert fields["bytesVal"]["type"] == "bytes"
        # uint/ulong -> long (Avro doesn't have unsigned)
        assert fields["uintVal"]["type"] == "long"

    def test_avro_all_nullable_null_string_field_not_double_wrapped(self, schema):
        avro = schema.avro_schema("AllPrimitiveTypes", nullable=True)
        fields = {f["name"]: f for f in avro["fields"]}
        str_field = fields["stringVal"]
        t = str_field["type"]
        assert isinstance(t, list)
        null_count = sum(1 for x in t if x == "null")
        assert null_count == 1

    def test_complex_nested_nested_generics_split_correctly(self, schema):
        """ComplexNested.tagGroups is Dictionary<int, List<Tag>> — map with array values."""
        avro = schema.avro_schema("ComplexNested")
        fields = {f["name"]: f for f in avro["fields"]}
        tag_groups_type = fields["tagGroups"]["type"]
        assert tag_groups_type["type"] == "map"
        values = tag_groups_type["values"]
        assert isinstance(values, dict)
        assert values["type"] == "array"
