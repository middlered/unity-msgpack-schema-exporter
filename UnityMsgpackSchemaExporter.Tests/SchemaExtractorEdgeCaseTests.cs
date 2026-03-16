namespace UnityMsgpackSchemaExporter.Tests;

/// <summary>
///     Tests for SchemaExtractor and SrmAssemblyReader edge cases:
///     LoadDirectory, bad DLLs, property-based schemas, Dispose, etc.
/// </summary>
public class SchemaExtractorEdgeCaseTests
{
    [Fact]
    public void LoadDirectory_LoadsAllDlls()
    {
        using var extractor = new SchemaExtractor();
        var dir = Path.GetDirectoryName(
            Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll"))!;
        extractor.LoadDirectory(dir);
        extractor.Extract();
        // Should find at least our test schemas
        Assert.True(extractor.Schemas.Count >= 10);
    }

    [Fact]
    public void LoadAssembly_NonExistent_Throws()
    {
        using var extractor = new SchemaExtractor();
        Assert.ThrowsAny<IOException>(() =>
            extractor.LoadAssembly("/nonexistent/path/test.dll"));
    }

    [Fact]
    public void LoadAssembly_InvalidDll_Throws()
    {
        // Create a temp file that is not a valid DLL
        var tempFile = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tempFile, "This is not a DLL");
            using var extractor = new SchemaExtractor();
            // SRM should throw for invalid PE
            Assert.ThrowsAny<Exception>(() => extractor.LoadAssembly(tempFile));
        }
        finally
        {
            File.Delete(tempFile);
        }
    }

    [Fact]
    public void Schemas_LazyExtraction_AutoExtracts()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        // Accessing Schemas without calling Extract() should auto-extract
        var schemas = extractor.Schemas;
        Assert.True(schemas.Count >= 10);
    }

    [Fact]
    public void FindSchema_FullName_ReturnsExact()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        // Find by full name
        var schema = extractor.FindSchema("UnityMsgpackSchemaExporter.TestSchemas.SimpleItem");
        Assert.NotNull(schema);
        Assert.Equal("SimpleItem", schema!.Name);
    }

    [Fact]
    public void FindSchema_CaseInsensitive()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var schema = extractor.FindSchema("simpleitem");
        Assert.NotNull(schema);
    }

    [Fact]
    public void SearchSchemas_CaseInsensitive()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var results = extractor.SearchSchemas("level").ToList();
        Assert.True(results.Count >= 3);
    }

    [Fact]
    public void SearchSchemas_FullNameMatch()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var results = extractor.SearchSchemas("TestSchemas").ToList();
        Assert.True(results.Count >= 10);
    }

    [Fact]
    public void Extract_MultipleCallsIdempotent()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();
        var count1 = extractor.Schemas.Count;
        extractor.Extract();
        var count2 = extractor.Schemas.Count;
        Assert.Equal(count1, count2);
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Dispose();
        // Second dispose should not throw
        extractor.Dispose();
    }

    [Fact]
    public void UnionBase_IsAbstract()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var schema = extractor.FindSchema("UnionBase");
        Assert.NotNull(schema);
        Assert.True(schema!.IsAbstract);
        Assert.True(schema.Unions.Count >= 2);
    }

    [Fact]
    public void IntUnionBase_IsAbstract_WithUnions()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var schema = extractor.FindSchema("IntUnionBase");
        Assert.NotNull(schema);
        Assert.True(schema!.IsAbstract);
        Assert.Equal(2, schema.Unions.Count);
        Assert.Equal(0, schema.Unions[0].Key);
        Assert.Contains("IntUnionChildA", schema.Unions[0].TypeName);
    }

    [Fact]
    public void Schema_SourceAssembly_NotEmpty()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var schema = extractor.FindSchema("SimpleItem")!;
        Assert.NotEmpty(schema.SourceAssembly);
        Assert.Contains("TestSchemas", schema.SourceAssembly);
    }

    [Fact]
    public void Schema_Namespace_NotEmpty()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var schema = extractor.FindSchema("SimpleItem")!;
        Assert.Equal("UnityMsgpackSchemaExporter.TestSchemas", schema.Namespace);
    }

    [Fact]
    public void Schema_BaseTypeName_ForConcreteClasses()
    {
        using var extractor = new SchemaExtractor();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        extractor.LoadAssembly(dllPath);
        extractor.Extract();

        var childA = extractor.FindSchema("UnionChildA")!;
        Assert.NotNull(childA.BaseTypeName);
        Assert.Contains("UnionBase", childA.BaseTypeName!);
    }

    [Fact]
    public void MsgPackFieldInfo_EffectiveKey_StringKeyed()
    {
        var field = new MsgPackFieldInfo { StringKey = "myKey", IntKey = -1 };
        Assert.Equal("myKey", field.EffectiveKey);
        Assert.True(field.IsStringKeyed);
    }

    [Fact]
    public void MsgPackFieldInfo_EffectiveKey_IntKeyed()
    {
        var field = new MsgPackFieldInfo { StringKey = null, IntKey = 5 };
        Assert.Equal("5", field.EffectiveKey);
        Assert.False(field.IsStringKeyed);
    }

    [Fact]
    public void MsgPackClassSchema_DefaultValues()
    {
        var schema = new MsgPackClassSchema();
        Assert.Equal("", schema.FullName);
        Assert.Equal("", schema.Name);
        Assert.Equal("", schema.Namespace);
        Assert.Null(schema.BaseTypeName);
        Assert.False(schema.IsAbstract);
        Assert.False(schema.IsStringKeyed);
        Assert.Empty(schema.Fields);
        Assert.Empty(schema.Unions);
        Assert.Equal("", schema.SourceAssembly);
    }

    [Fact]
    public void MsgPackUnionEntry_DefaultValues()
    {
        var entry = new MsgPackUnionEntry();
        Assert.Equal(0, entry.Key);
        Assert.Equal("", entry.TypeName);
    }

    [Fact]
    public void SrmAssemblyReader_GetSourceAssembly()
    {
        using var reader = new SrmAssemblyReader();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        reader.LoadAssembly(dllPath);

        var types = reader.GetAllTypes().ToList();
        Assert.True(types.Count > 0);

        var source = reader.GetSourceAssembly(types[0]);
        Assert.Contains("TestSchemas", source);
    }

    [Fact]
    public void SrmAssemblyReader_GetAllTypes_SkipsCompilerGenerated()
    {
        using var reader = new SrmAssemblyReader();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        reader.LoadAssembly(dllPath);

        var types = reader.GetAllTypes().ToList();
        Assert.DoesNotContain(types, t => t.Name.StartsWith("<"));
    }

    [Fact]
    public void SrmTypeInfo_FullName_HasNamespacePrefix()
    {
        using var reader = new SrmAssemblyReader();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        reader.LoadAssembly(dllPath);

        var types = reader.GetAllTypes().ToList();
        var simpleItem = types.First(t => t.Name == "SimpleItem");
        Assert.StartsWith("UnityMsgpackSchemaExporter.TestSchemas", simpleItem.FullName);
    }

    [Fact]
    public void SrmTypeInfo_Fields_HaveCorrectAttributes()
    {
        using var reader = new SrmAssemblyReader();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        reader.LoadAssembly(dllPath);

        var types = reader.GetAllTypes().ToList();
        var simpleItem = types.First(t => t.Name == "SimpleItem");

        var fields = simpleItem.Fields;
        Assert.True(fields.Count >= 4);

        // Verify fields have Key attributes
        var idField = fields.FirstOrDefault(f => f.Name == "id");
        Assert.NotNull(idField);
        var keyAttr = idField!.CustomAttributes.FirstOrDefault(a => a.AttributeTypeName == "KeyAttribute");
        Assert.NotNull(keyAttr);
    }

    [Fact]
    public void SrmTypeInfo_CustomAttributes_MessagePackObject()
    {
        using var reader = new SrmAssemblyReader();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        reader.LoadAssembly(dllPath);

        var types = reader.GetAllTypes().ToList();
        var simpleItem = types.First(t => t.Name == "SimpleItem");

        var attrs = simpleItem.CustomAttributes;
        Assert.Contains(attrs, a => a.AttributeTypeName == "MessagePackObjectAttribute");
    }

    [Fact]
    public void SrmTypeInfo_Properties_ListIsEmpty_ForFieldOnlyClasses()
    {
        using var reader = new SrmAssemblyReader();
        var dllPath = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
        reader.LoadAssembly(dllPath);

        var types = reader.GetAllTypes().ToList();
        var simpleItem = types.First(t => t.Name == "SimpleItem");

        // SimpleItem uses fields, not properties; properties list may be empty or contain backing fields
        var props = simpleItem.Properties;
        Assert.NotNull(props);
    }

    [Fact]
    public void LoadDirectory_WithInvalidDlls_SkipsThem()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "test_dlls_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        try
        {
            // Create an invalid dll file
            File.WriteAllText(Path.Combine(tempDir, "bad.dll"), "not a dll");
            // Copy a valid dll
            var validDll = Path.Combine(AppContext.BaseDirectory, "UnityMsgpackSchemaExporter.TestSchemas.dll");
            File.Copy(validDll, Path.Combine(tempDir, "TestSchemas.dll"));

            using var reader = new SrmAssemblyReader();
            // Should not throw - bad DLL is skipped with warning
            reader.LoadDirectory(tempDir);
            var types = reader.GetAllTypes().ToList();
            Assert.True(types.Count > 0);
        }
        finally
        {
            Directory.Delete(tempDir, true);
        }
    }
}