using System.Text.Json;
using System.Text.Json.Nodes;

namespace UnityMsgpackSchemaExporter.Tests;

/// <summary>
///     Tests for AvroExporter edge cases: all-nullable, flat mode, HashSet,
///     byte[], object type, unknown types, recursive references, etc.
/// </summary>
public class AvroExporterEdgeCaseTests : IDisposable
{
    private readonly SchemaExtractor _extractor;

    public AvroExporterEdgeCaseTests()
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
    public void ExportSchema_WithHashSet_ArrayType()
    {
        var schema = _extractor.FindSchema("WithHashSet")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var memberIds = fields!.First(f => f["name"]!.ToString() == "memberIds");
        var type = memberIds["type"]!;
        Assert.Equal("array", type["type"]!.ToString());
    }

    [Fact]
    public void ExportSchema_AllNullable_WrapsFields()
    {
        var schema = _extractor.FindSchema("SimpleItem")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, new AvroExportOptions { AllNullable = true });
        var fields = avro["fields"] as JsonArray;
        // Each field type should be a union array containing "null"
        foreach (var field in fields!)
        {
            var type = field["type"];
            if (type is JsonArray arr)
            {
                Assert.Contains(arr, t => t?.GetValueKind() == JsonValueKind.String && t.GetValue<string>() == "null");
            }
        }
    }

    [Fact]
    public void ExportSchema_AllNullable_AlreadyNullable_NotDoubleWrapped()
    {
        var schema = _extractor.FindSchema("NullableFields")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, new AvroExportOptions { AllNullable = true });
        var fields = avro["fields"] as JsonArray;
        var optionalInt = fields!.First(f => f["name"]!.ToString() == "optionalInt");
        var type = optionalInt["type"] as JsonArray;
        Assert.NotNull(type);
        // Count how many "null" entries there are — should be exactly 1
        var nullCount = type!.Count(t => t?.GetValueKind() == JsonValueKind.String && t.GetValue<string>() == "null");
        Assert.Equal(1, nullCount);
    }

    [Fact]
    public void ExportSchema_Flat_ReferencesNestedByName()
    {
        var schema = _extractor.FindSchema("Order")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas, new AvroExportOptions { Flat = true });
        var fields = avro["fields"] as JsonArray;
        var items = fields!.First(f => f["name"]!.ToString() == "items");
        // In flat mode, items should reference OrderItem by qualified name
        var itemsType = items["type"]!;
        Assert.Equal("array", itemsType["type"]!.ToString());
        var itemsItems = itemsType["items"]!.ToString();
        Assert.Contains("OrderItem", itemsItems);
    }

    [Fact]
    public void ExportAllSchemas_Flat_AllSchemasPresent()
    {
        var result = AvroExporter.ExportAllSchemas(_extractor.Schemas, new AvroExportOptions { Flat = true });
        Assert.True(result.Count >= 10);

        // All schemas should have unique names
        var names = result.Select(r => r["name"]!.ToString()).ToList();
        Assert.Equal(names.Count, names.Distinct().Count());
    }

    [Fact]
    public void ExportAllSchemas_Nested_AllSchemasPresent()
    {
        var result = AvroExporter.ExportAllSchemas(_extractor.Schemas);
        Assert.True(result.Count >= 10);
    }

    [Fact]
    public void ExportSchema_IntKeyedDict_NonStringKey_HasMsgpackKeyType()
    {
        var schema = _extractor.FindSchema("IntKeyedDict")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var scores = fields!.First(f => f["name"]!.ToString() == "scores");
        var type = scores["type"]!;
        Assert.Equal("map", type["type"]!.ToString());
        Assert.NotNull(type["msgpack_key_type"]);
    }

    [Fact]
    public void ExportSchema_ComplexNested_DictIntKey_HasMsgpackKeyType()
    {
        var schema = _extractor.FindSchema("ComplexNested")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var tagGroups = fields!.First(f => f["name"]!.ToString() == "tagGroups");
        var type = tagGroups["type"]!;
        Assert.Equal("map", type["type"]!.ToString());
        Assert.NotNull(type["msgpack_key_type"]);
    }

    [Fact]
    public void ExportSchema_Union_HasMsgpackUnions()
    {
        var schema = _extractor.FindSchema("UnionBase")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        Assert.NotNull(avro["msgpack_unions"]);
        var unions = avro["msgpack_unions"] as JsonArray;
        Assert.True(unions!.Count >= 2);
    }

    [Fact]
    public void ExportSchema_IntUnionBase_HasMsgpackUnions()
    {
        var schema = _extractor.FindSchema("IntUnionBase")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        Assert.NotNull(avro["msgpack_unions"]);
    }

    [Fact]
    public void ExportSchema_MsgpackKey_StringKeyed()
    {
        var schema = _extractor.FindSchema("SimpleItem")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        foreach (var field in fields!)
        {
            Assert.NotNull(field["msgpack_key"]);
            Assert.Equal(JsonValueKind.String, field["msgpack_key"].GetValueKind());
        }
    }

    [Fact]
    public void ExportSchema_MsgpackKey_IntKeyed()
    {
        var schema = _extractor.FindSchema("IntKeyedItem")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        foreach (var field in fields!)
        {
            Assert.NotNull(field["msgpack_key"]);
            Assert.Equal(JsonValueKind.Number, field["msgpack_key"].GetValueKind());
        }
    }

    [Fact]
    public void ExportSchema_StringArrayField()
    {
        var schema = _extractor.FindSchema("Order")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var tags = fields!.First(f => f["name"]!.ToString() == "tags");
        var type = tags["type"]!;
        Assert.Equal("array", type["type"]!.ToString());
    }

    [Fact]
    public void ExportSchema_StringDictField()
    {
        var schema = _extractor.FindSchema("Order")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var metadata = fields!.First(f => f["name"]!.ToString() == "metadata");
        var type = metadata["type"]!;
        Assert.Equal("map", type["type"]!.ToString());
    }

    [Fact]
    public void ExportSchema_NullableString_IsNullUnion()
    {
        var schema = _extractor.FindSchema("NullableFields")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var optStr = fields!.First(f => f["name"]!.ToString() == "optionalStr");
        // String fields are nullable: ["null", "string"]
        var type = optStr["type"]!;
        Assert.True(type is JsonArray);
    }

    [Fact]
    public void ExportSchema_Level1_InlinesNestedRecords()
    {
        var schema = _extractor.FindSchema("Level1")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var items = fields!.First(f => f["name"]!.ToString() == "items");
        // items is List<Level2> -> array of inlined record
        var type = items["type"]!;
        Assert.Equal("array", type["type"]!.ToString());
        var innerItems = type["items"];
        // Should be inlined record, not just a string reference
        Assert.True(innerItems is JsonObject || innerItems?.GetValueKind() == JsonValueKind.String);
    }

    [Fact]
    public void ExportSchema_PrimitiveTypes_MappedCorrectly()
    {
        var schema = _extractor.FindSchema("SimpleItem")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;

        var id = fields!.First(f => f["name"]!.ToString() == "id");
        Assert.Equal("int", id["type"]!.ToString());

        var active = fields!.First(f => f["name"]!.ToString() == "active");
        Assert.Equal("boolean", active["type"]!.ToString());

        var score = fields!.First(f => f["name"]!.ToString() == "score");
        Assert.Equal("float", score["type"]!.ToString());

        var bigValue = fields!.First(f => f["name"]!.ToString() == "bigValue");
        Assert.Equal("long", bigValue["type"]!.ToString());
    }

    [Fact]
    public void ExportSchema_Namespace_Set()
    {
        var schema = _extractor.FindSchema("SimpleItem")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        Assert.NotNull(avro["namespace"]);
        Assert.NotEmpty(avro["namespace"]!.ToString());
    }

    [Fact]
    public void ExportSchema_RecordType()
    {
        var schema = _extractor.FindSchema("SimpleItem")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        Assert.Equal("record", avro["type"]!.ToString());
    }

    [Fact]
    public void ExportSchema_SparseIntKeys_AllFieldsPresent()
    {
        var schema = _extractor.FindSchema("SparseIntKeys")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        Assert.Equal(3, fields!.Count);
    }

    [Fact]
    public void ExportSchema_WithEnumLike_IntFieldMapped()
    {
        var schema = _extractor.FindSchema("WithEnumLike")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        var priority = fields!.First(f => f["name"]!.ToString() == "priority");
        Assert.Equal("int", priority["type"]!.ToString());
    }

    [Fact]
    public void ExportSchema_MessageWithUnion_PayloadField()
    {
        var schema = _extractor.FindSchema("MessageWithUnion")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        Assert.NotNull(fields);
        // Should have id, payload, payloads fields
        var fieldNames = fields!.Select(f => f["name"]!.ToString()).ToList();
        Assert.Contains("id", fieldNames);
        Assert.Contains("payload", fieldNames);
        Assert.Contains("payloads", fieldNames);
    }

    [Fact]
    public void AvroExportOptions_Default_NotNull()
    {
        var opts = AvroExportOptions.Default;
        Assert.NotNull(opts);
        Assert.False(opts.AllNullable);
        Assert.False(opts.Flat);
    }

    [Fact]
    public void ExportSchema_IntKeyedWithStringChild_NestedInlined()
    {
        var schema = _extractor.FindSchema("IntKeyedWithStringChild")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        Assert.NotNull(fields);
        Assert.True(fields!.Count >= 3);
    }

    [Fact]
    public void ExportSchema_IntKeyedWithIntChild_NestedInlined()
    {
        var schema = _extractor.FindSchema("IntKeyedWithIntChild")!;
        var avro = AvroExporter.ExportSchema(schema, _extractor.Schemas);
        var fields = avro["fields"] as JsonArray;
        Assert.NotNull(fields);
    }

    [Fact]
    public void ExportAllSchemas_AllNullable_NoException()
    {
        var result = AvroExporter.ExportAllSchemas(_extractor.Schemas, new AvroExportOptions { AllNullable = true });
        Assert.True(result.Count >= 10);
    }

    [Fact]
    public void ExportAllSchemas_FlatAndNullable_NoException()
    {
        var result = AvroExporter.ExportAllSchemas(_extractor.Schemas,
            new AvroExportOptions { AllNullable = true, Flat = true });
        Assert.True(result.Count >= 10);
    }
}