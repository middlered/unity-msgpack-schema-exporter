using System.Text.Json;
using System.Text.Json.Nodes;

namespace UnityMsgpackSchemaExporter.Tests;

/// <summary>
///     Tests Avro schema export — especially nested types being properly inlined.
/// </summary>
public class AvroExporterTests : IDisposable
{
    private readonly SchemaExtractor _extractor;

    public AvroExporterTests()
    {
        _extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        _extractor.LoadAssembly(dllPath);
        _extractor.Extract();
    }

    public void Dispose()
    {
        _extractor.Dispose();
    }

    [Fact]
    public void SimpleItem_PrimitiveFieldTypes()
    {
        var schema = _extractor.FindSchema("SimpleItem")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        Assert.Equal("record", avro["type"]!.ToString());
        Assert.Equal("SimpleItem", avro["name"]!.ToString());

        var fields = avro["fields"] as JsonArray;
        Assert.NotNull(fields);
        Assert.Equal(5, fields!.Count);

        // id should be "int"
        var idField = fields.First(f => f["name"]!.ToString() == "id");
        Assert.Equal("int", idField["type"]!.ToString());

        // active should be "boolean"
        var activeField = fields.First(f => f["name"]!.ToString() == "active");
        Assert.Equal("boolean", activeField["type"]!.ToString());

        // score should be "float"
        var scoreField = fields.First(f => f["name"]!.ToString() == "score");
        Assert.Equal("float", scoreField["type"]!.ToString());
    }

    [Fact]
    public void Order_NestedOrderItem_InlinedAsRecord()
    {
        var schema = _extractor.FindSchema("Order")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;
        var itemsField = fields!.First(f => f["name"]!.ToString() == "items");
        var itemsType = itemsField["type"] as JsonObject;

        Assert.Equal("array", itemsType!["type"]!.ToString());

        // items should be an inline record, not "string"
        var items = itemsType["items"] as JsonObject;
        Assert.NotNull(items);
        Assert.Equal("record", items!["type"]!.ToString());
        Assert.Equal("OrderItem", items["name"]!.ToString());

        // OrderItem should have its own fields
        var orderItemFields = items["fields"] as JsonArray;
        Assert.NotNull(orderItemFields);
        Assert.True(orderItemFields!.Count >= 3);
        Assert.NotNull(orderItemFields.FirstOrDefault(f => f["name"]!.ToString() == "productId"));
        Assert.NotNull(orderItemFields.FirstOrDefault(f => f["name"]!.ToString() == "quantity"));
        Assert.NotNull(orderItemFields.FirstOrDefault(f => f["name"]!.ToString() == "unitPrice"));
    }

    [Fact]
    public void Order_StringArray_CorrectAvro()
    {
        var schema = _extractor.FindSchema("Order")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;
        var tagsField = fields!.First(f => f["name"]!.ToString() == "tags");
        var tagsType = tagsField["type"] as JsonObject;

        Assert.Equal("array", tagsType!["type"]!.ToString());
        // items should be ["null", "string"] since string is nullable in Avro
        var tagsItems = tagsType["items"] as JsonArray;
        Assert.NotNull(tagsItems);
    }

    [Fact]
    public void Order_StringDict_CorrectAvro()
    {
        var schema = _extractor.FindSchema("Order")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;
        var metaField = fields!.First(f => f["name"]!.ToString() == "metadata");
        var metaType = metaField["type"] as JsonObject;

        Assert.Equal("map", metaType!["type"]!.ToString());
    }

    [Fact]
    public void ComplexNested_DeeplyNestedTypes()
    {
        var schema = _extractor.FindSchema("ComplexNested")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;

        // singleTag should be an inline Tag record (or reference)
        var singleTag = fields!.First(f => f["name"]!.ToString() == "singleTag");
        // It may be nullable nested or direct record
        Assert.NotNull(singleTag["type"]);
    }

    [Fact]
    public void Level1_ThreeLevelsDeep()
    {
        var schema = _extractor.FindSchema("Level1")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        // Traverse: Level1.items -> array<Level2>
        var fields = avro["fields"] as JsonArray;
        var itemsField = fields!.First(f => f["name"]!.ToString() == "items");
        var itemsType = itemsField["type"] as JsonObject;
        Assert.Equal("array", itemsType!["type"]!.ToString());

        var level2 = itemsType["items"] as JsonObject;
        Assert.Equal("record", level2!["type"]!.ToString());
        Assert.Equal("Level2", level2["name"]!.ToString());

        // Level2.children -> array<Level3>
        var level2Fields = level2["fields"] as JsonArray;
        var childrenField = level2Fields!.First(f => f["name"]!.ToString() == "children");
        var childrenType = childrenField["type"] as JsonObject;
        Assert.Equal("array", childrenType!["type"]!.ToString());

        var level3 = childrenType["items"] as JsonObject;
        Assert.Equal("record", level3!["type"]!.ToString());
        Assert.Equal("Level3", level3["name"]!.ToString());
    }

    [Fact]
    public void IntKeyedDict_NonStringKeyDict()
    {
        var schema = _extractor.FindSchema("IntKeyedDict")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;
        var scores = fields!.First(f => f["name"]!.ToString() == "scores");
        var scoresType = scores["type"] as JsonObject;

        // Non-string key: Avro map with msgpack_key_type annotation
        Assert.Equal("map", scoresType!["type"]!.ToString());
        Assert.Equal("int", scoresType["msgpack_key_type"]!.ToString());
        Assert.NotNull(scoresType["values"]);
    }

    [Fact]
    public void ExportAllSchemas_NoCrash()
    {
        var all = AvroExporter.ExportAllSchemas(_extractor.Schemas);
        Assert.True(all.Count >= 10);
    }

    [Fact]
    public void Nullable_AllFieldsWrapped()
    {
        var schema = _extractor.FindSchema("SimpleItem")!;
        var options = new AvroExportOptions { AllNullable = true };
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, options);

        var fields = avro["fields"] as JsonArray;
        foreach (var field in fields!)
        {
            var type = field["type"];
            // Every field should be a union array containing "null"
            Assert.True(type is JsonArray arr && arr.Any(t => t.ToString() == "null"),
                $"Field '{field["name"]}' should be nullable but is: {type}");
        }
    }

    [Fact]
    public void Nullable_AlreadyNullable_NotDoubleWrapped()
    {
        // string type is already ["null", "string"] — shouldn't become ["null", ["null", "string"]]
        var schema = _extractor.FindSchema("NullableFields")!;
        var options = new AvroExportOptions { AllNullable = true };
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, options);

        var fields = avro["fields"] as JsonArray;
        var strField = fields!.First(f => f["name"]!.ToString() == "optionalStr");
        var strType = strField["type"] as JsonArray;
        Assert.NotNull(strType);
        // Should be ["null", "string"], not ["null", ["null", "string"]]
        Assert.Equal(2, strType!.Count);
        Assert.Equal("null", strType[0]!.ToString());
    }

    [Fact]
    public void Flat_NestedTypesReferencedByName()
    {
        var schema = _extractor.FindSchema("Order")!;
        var options = new AvroExportOptions { Flat = true };
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, options);

        var fields = avro["fields"] as JsonArray;
        var itemsField = fields!.First(f => f["name"]!.ToString() == "items");
        var itemsType = itemsField["type"] as JsonObject;
        Assert.Equal("array", itemsType!["type"]!.ToString());

        // In flat mode, items should be a string reference, not an inline record
        var items = itemsType["items"];
        Assert.Equal(JsonValueKind.String, items.GetValueKind());
        Assert.Contains("OrderItem", items.ToString());
    }

    [Fact]
    public void Flat_ExportAll_AllSchemasPresent()
    {
        var options = new AvroExportOptions { Flat = true };
        var all = AvroExporter.ExportAllSchemas(_extractor.Schemas, options);
        Assert.True(all.Count >= 10);
        // Each entry should be a record with type="record"
        foreach (var item in all) Assert.Equal("record", item["type"]!.ToString());
    }

    // ── Mixed nesting tests ──────────────────────────────────────────────────

    [Fact]
    public void IntKeyedWithStringChild_NestedAvro()
    {
        var schema = _extractor.FindSchema("IntKeyedWithStringChild")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;
        // id field has msgpack_key = 0 (int key)
        var idField = fields!.First(f => f["name"]!.ToString() == "id");
        Assert.Equal(0, idField["msgpack_key"]!.GetValue<int>());

        // tag field should have inline Tag record
        var tagField = fields.First(f => f["name"]!.ToString() == "tag");
        // Tag is nullable (class), so type is ["null", <Tag record>]
        Assert.NotNull(tagField["type"]);
    }

    [Fact]
    public void StringKeyedWithIntChild_NestedAvro()
    {
        var schema = _extractor.FindSchema("StringKeyedWithIntChild")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;
        // id is a string-keyed field
        var idField = fields!.First(f => f["name"]!.ToString() == "id");
        Assert.Equal("id", idField["msgpack_key"]!.ToString());

        // child field should have inline IntKeyedItem record
        var childField = fields.First(f => f["name"]!.ToString() == "child");
        Assert.NotNull(childField["type"]);
    }

    [Fact]
    public void IntKeyedWithIntChild_NestedAvro()
    {
        var schema = _extractor.FindSchema("IntKeyedWithIntChild")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var fields = avro["fields"] as JsonArray;
        // child field: int-keyed child, expect an array [null, <IntKeyedItem>] or just inline record
        var childField = fields!.First(f => f["name"]!.ToString() == "child");
        Assert.NotNull(childField["type"]);

        // children field should be array type
        var childrenField = fields.First(f => f["name"]!.ToString() == "children");
        // Could be array or ["null", array]
        Assert.NotNull(childrenField["type"]);
    }

    // ── Union Avro tests ─────────────────────────────────────────────────────

    [Fact]
    public void UnionBase_HasMsgpackUnions()
    {
        var schema = _extractor.FindSchema("UnionBase")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        // UnionBase must export msgpack_unions with both children
        var unions = avro["msgpack_unions"] as JsonArray;
        Assert.NotNull(unions);
        Assert.Equal(2, unions!.Count);

        var keys = unions.Select(u => u["key"]!.GetValue<int>()).OrderBy(k => k).ToList();
        Assert.Equal(new[] { 0, 1 }, keys);

        var typeNames = unions.Select(u => u["type"]!.ToString()).ToHashSet();
        Assert.Contains("UnionChildA", typeNames);
        Assert.Contains("UnionChildB", typeNames);
    }

    [Fact]
    public void UnionChildA_CorrectFields()
    {
        var schema = _extractor.FindSchema("UnionChildA")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        Assert.Equal("record", avro["type"]!.ToString());
        Assert.Equal("UnionChildA", avro["name"]!.ToString());

        var fields = avro["fields"] as JsonArray;
        Assert.NotNull(fields);
        var fieldNames = fields!.Select(f => f["name"]!.ToString()).ToHashSet();
        Assert.Contains("label", fieldNames);
        Assert.Contains("value", fieldNames);
    }

    [Fact]
    public void IntUnionBase_HasMsgpackUnions()
    {
        var schema = _extractor.FindSchema("IntUnionBase")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        var unions = avro["msgpack_unions"] as JsonArray;
        Assert.NotNull(unions);
        Assert.Equal(2, unions!.Count);
        var typeNames = unions.Select(u => u["type"]!.ToString()).ToHashSet();
        Assert.Contains("IntUnionChildA", typeNames);
        Assert.Contains("IntUnionChildB", typeNames);
    }

    [Fact]
    public void MessageWithUnion_ExportsCorrectly()
    {
        var schema = _extractor.FindSchema("MessageWithUnion")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);

        Assert.Equal("record", avro["type"]!.ToString());
        var fields = avro["fields"] as JsonArray;
        var fieldNames = fields!.Select(f => f["name"]!.ToString()).ToHashSet();
        Assert.Contains("id", fieldNames);
        Assert.Contains("payload", fieldNames);
        Assert.Contains("payloads", fieldNames);
    }

    [Fact]
    public void UnionChildA_MsgpackKey_StringKeyed()
    {
        var schema = _extractor.FindSchema("UnionChildA")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;

        // All fields should have string msgpack_key values
        foreach (var field in fields!)
        {
            Assert.Equal(JsonValueKind.String, field["msgpack_key"].GetValueKind());
        }
    }

    [Fact]
    public void IntUnionChildA_MsgpackKey_IntKeyed()
    {
        var schema = _extractor.FindSchema("IntUnionChildA")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;

        // All fields should have integer msgpack_key values
        foreach (var field in fields!)
        {
            Assert.Equal(JsonValueKind.Number, field["msgpack_key"].GetValueKind());
        }
    }
}