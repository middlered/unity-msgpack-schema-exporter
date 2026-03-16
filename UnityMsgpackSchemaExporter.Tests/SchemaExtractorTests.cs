namespace UnityMsgpackSchemaExporter.Tests;

/// <summary>
///     Tests that the SchemaExtractor correctly reads [MessagePackObject]/[Key] from a real assembly.
///     Uses UnityMsgpackSchemaExporter.TestSchemas.dll which has known classes with various key/type patterns.
/// </summary>
public class SchemaExtractorTests : IDisposable
{
    private readonly SchemaExtractor _extractor;

    public SchemaExtractorTests()
    {
        _extractor = new SchemaExtractor();
        // Load UnityMsgpackSchemaExporter.TestSchemas.dll from its build output
        var dllPath = Path.Combine(
            AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        Assert.True(File.Exists(dllPath), $"UnityMsgpackSchemaExporter.TestSchemas.dll not found at {dllPath}");
        _extractor.LoadAssembly(dllPath);
        _extractor.Extract();
    }

    public void Dispose()
    {
        _extractor.Dispose();
    }

    [Fact]
    public void ExtractsAllTestClasses()
    {
        var schemas = _extractor.Schemas;
        Assert.True(schemas.Count >= 10,
            $"Expected at least 10 schemas, got {schemas.Count}");

        // Verify specific classes are found
        Assert.NotNull(_extractor.FindSchema("SimpleItem"));
        Assert.NotNull(_extractor.FindSchema("IntKeyedItem"));
        Assert.NotNull(_extractor.FindSchema("Order"));
        Assert.NotNull(_extractor.FindSchema("OrderItem"));
        Assert.NotNull(_extractor.FindSchema("NullableFields"));
        Assert.NotNull(_extractor.FindSchema("IntKeyedDict"));
        Assert.NotNull(_extractor.FindSchema("ComplexNested"));
        Assert.NotNull(_extractor.FindSchema("Tag"));
        Assert.NotNull(_extractor.FindSchema("Level1"));
        Assert.NotNull(_extractor.FindSchema("Level2"));
        Assert.NotNull(_extractor.FindSchema("Level3"));
        Assert.NotNull(_extractor.FindSchema("WithHashSet"));
        Assert.NotNull(_extractor.FindSchema("SparseIntKeys"));
        Assert.NotNull(_extractor.FindSchema("WithEnumLike"));
    }

    [Fact]
    public void SimpleItem_StringKeyed_CorrectFields()
    {
        var s = _extractor.FindSchema("SimpleItem")!;
        Assert.True(s.IsStringKeyed);
        Assert.False(s.IsAbstract);
        Assert.Equal(5, s.Fields.Count);

        var id = s.Fields.First(f => f.MemberName == "id");
        Assert.Equal("id", id.StringKey);
        Assert.Equal("int", id.TypeName);

        var name = s.Fields.First(f => f.MemberName == "name");
        Assert.Equal("name", name.StringKey);
        Assert.Equal("string", name.TypeName);

        var active = s.Fields.First(f => f.MemberName == "active");
        Assert.Equal("bool", active.TypeName);

        var score = s.Fields.First(f => f.MemberName == "score");
        Assert.Equal("float", score.TypeName);

        var big = s.Fields.First(f => f.MemberName == "bigValue");
        Assert.Equal("long", big.TypeName);
    }

    [Fact]
    public void IntKeyedItem_IntegerKeys()
    {
        var s = _extractor.FindSchema("IntKeyedItem")!;
        Assert.False(s.IsStringKeyed);
        Assert.Equal(3, s.Fields.Count);

        Assert.Equal(0, s.Fields[0].IntKey);
        Assert.Equal("int", s.Fields[0].TypeName);

        Assert.Equal(1, s.Fields[1].IntKey);
        Assert.Equal("string", s.Fields[1].TypeName);

        Assert.Equal(2, s.Fields[2].IntKey);
        Assert.Equal("double", s.Fields[2].TypeName);
    }

    [Fact]
    public void Order_NestedListAndDictionary()
    {
        var s = _extractor.FindSchema("Order")!;
        Assert.True(s.IsStringKeyed);

        var items = s.Fields.First(f => f.MemberName == "items");
        Assert.Equal("List<OrderItem>", items.TypeName);

        var tags = s.Fields.First(f => f.MemberName == "tags");
        Assert.Contains("[]", tags.TypeName); // string[] or String[]

        var meta = s.Fields.First(f => f.MemberName == "metadata");
        Assert.Contains("Dictionary", meta.TypeName);
    }

    [Fact]
    public void NullableFields_Detected()
    {
        var s = _extractor.FindSchema("NullableFields")!;

        var optInt = s.Fields.First(f => f.MemberName == "optionalInt");
        Assert.Contains("Nullable", optInt.TypeName);

        var optStr = s.Fields.First(f => f.MemberName == "optionalStr");
        // string is reference type — can be represented as string or Nullable<String>
        Assert.True(optStr.TypeName == "string" || optStr.TypeName.Contains("String"));
    }

    [Fact]
    public void ComplexNested_DictionaryOfListOfNested()
    {
        var s = _extractor.FindSchema("ComplexNested")!;

        var tagGroups = s.Fields.First(f => f.MemberName == "tagGroups");
        Assert.Contains("Dictionary", tagGroups.TypeName);
        Assert.Contains("Tag", tagGroups.TypeName);

        var singleTag = s.Fields.First(f => f.MemberName == "singleTag");
        Assert.Contains("Tag", singleTag.TypeName);

        var tagArray = s.Fields.First(f => f.MemberName == "tagArray");
        Assert.Contains("Tag", tagArray.TypeName);
        Assert.Contains("[]", tagArray.TypeName);
    }

    [Fact]
    public void SparseIntKeys_GapsHandled()
    {
        var s = _extractor.FindSchema("SparseIntKeys")!;
        Assert.False(s.IsStringKeyed);
        Assert.Equal(3, s.Fields.Count);

        Assert.Equal(0, s.Fields[0].IntKey);
        Assert.Equal(2, s.Fields[1].IntKey);
        Assert.Equal(5, s.Fields[2].IntKey);
    }

    [Fact]
    public void WithHashSet_RecognizedAsCollection()
    {
        var s = _extractor.FindSchema("WithHashSet")!;
        var members = s.Fields.First(f => f.MemberName == "memberIds");
        Assert.Contains("HashSet", members.TypeName);
    }

    [Fact]
    public void FindSchema_PartialMatch()
    {
        // Should find by simple name
        Assert.NotNull(_extractor.FindSchema("SimpleItem"));
        // Should not find non-existent
        Assert.Null(_extractor.FindSchema("NonExistentClass"));
    }

    [Fact]
    public void SearchSchemas_Works()
    {
        var results = _extractor.SearchSchemas("Level").ToList();
        Assert.True(results.Count >= 3); // Level1, Level2, Level3
    }
}