namespace UnityMsgpackSchemaExporter;

/// <summary>
///     Extracts MessagePackObject schemas from Unity DummyDll assemblies
///     using a pluggable IAssemblyReader backend (default: System.Reflection.Metadata).
/// </summary>
public class SchemaExtractor(IAssemblyReader reader) : IDisposable
{
    private readonly IAssemblyReader _reader = reader;
    private readonly Dictionary<string, MsgPackClassSchema> _schemas = new();
    private bool _extracted;

    public SchemaExtractor() : this(new SrmAssemblyReader())
    {
    }

    /// <summary>All discovered schemas, keyed by full type name.</summary>
    public IReadOnlyDictionary<string, MsgPackClassSchema> Schemas
    {
        get
        {
            if (!_extracted) Extract();
            return _schemas;
        }
    }

    public void Dispose()
    {
        _reader.Dispose();
    }

    /// <summary>
    ///     Load all DLL files from a DummyDll directory.
    /// </summary>
    public void LoadDirectory(string dummyDllPath)
    {
        _reader.LoadDirectory(dummyDllPath);
        _extracted = false;
    }

    /// <summary>
    ///     Load a single DLL file.
    /// </summary>
    public void LoadAssembly(string dllPath)
    {
        _reader.LoadAssembly(dllPath);
        _extracted = false;
    }

    /// <summary>
    ///     Extract all MessagePackObject schemas from loaded assemblies.
    /// </summary>
    public void Extract()
    {
        _schemas.Clear();
        foreach (var type in _reader.GetAllTypes())
        {
            if (!HasMessagePackObjectAttribute(type)) continue;

            var schema = ExtractSchema(type);
            if (schema != null)
            {
                _schemas[schema.FullName] = schema;
            }
        }

        _extracted = true;
    }

    private MsgPackClassSchema? ExtractSchema(ITypeInfo type)
    {
        var schema = new MsgPackClassSchema
        {
            FullName = type.FullName,
            Name = type.Name,
            Namespace = type.Namespace,
            BaseTypeName = type.BaseTypeFullName,
            IsAbstract = type.IsAbstract,
            SourceAssembly = _reader.GetSourceAssembly(type)
        };

        // Extract Union attributes
        foreach (var attr in type.CustomAttributes)
        {
            if (attr.AttributeTypeName != "UnionAttribute" || attr.ConstructorArguments.Count < 2) continue;

            var keyVal = attr.ConstructorArguments[0].Value;
            var unionTypeVal = attr.ConstructorArguments[1].Value;
            schema.Unions.Add(new MsgPackUnionEntry
            {
                Key = keyVal is int k ? k : 0,
                TypeName = unionTypeVal?.ToString() ?? "unknown"
            });
        }

        // Extract fields with [Key(...)]
        foreach (var field in type.Fields)
        {
            var keyInfo = GetKeyAttribute(field.CustomAttributes);
            if (keyInfo == null) continue;

            schema.Fields.Add(new MsgPackFieldInfo
            {
                StringKey = keyInfo.Value.stringKey,
                IntKey = keyInfo.Value.intKey,
                MemberName = field.Name,
                TypeName = field.FormattedTypeName,
                IsProperty = false
            });
        }

        // Extract properties with [Key(...)]
        foreach (var prop in type.Properties)
        {
            var keyInfo = GetKeyAttribute(prop.CustomAttributes);
            if (keyInfo == null) continue;

            // Avoid duplicates if field and property share the same key
            var duplicate = schema.Fields.Any(f =>
                (f.StringKey != null && f.StringKey == keyInfo.Value.stringKey) ||
                (f.StringKey == null && keyInfo.Value.stringKey == null && f.IntKey == keyInfo.Value.intKey));
            if (duplicate) continue;

            schema.Fields.Add(new MsgPackFieldInfo
            {
                StringKey = keyInfo.Value.stringKey,
                IntKey = keyInfo.Value.intKey,
                MemberName = prop.Name,
                TypeName = prop.FormattedTypeName,
                IsProperty = true
            });
        }

        // Determine if string-keyed or int-keyed
        schema.IsStringKeyed = schema.Fields.Any(f => f.IsStringKeyed);

        // Sort fields by key order
        if (schema.IsStringKeyed)
        {
            schema.Fields.Sort((a, b) => string.Compare(a.EffectiveKey, b.EffectiveKey, StringComparison.Ordinal));
        }
        else
        {
            schema.Fields.Sort((a, b) => a.IntKey.CompareTo(b.IntKey));
        }

        return schema;
    }

    private static (string? stringKey, int intKey)? GetKeyAttribute(IReadOnlyList<ICustomAttributeInfo> attrs)
    {
        foreach (var attr in attrs)
        {
            if (attr is not { AttributeTypeName: "KeyAttribute", ConstructorArguments.Count: >= 1 }) continue;

            var arg = attr.ConstructorArguments[0];
            switch (arg.TypeFullName)
            {
                case "System.String":
                    return ((string?)arg.Value ?? "", -1);
                case "System.Int32":
                    return (null, arg.Value is int i ? i : 0);
            }
        }

        return null;
    }

    private static bool HasMessagePackObjectAttribute(ITypeInfo type)
    {
        return type.CustomAttributes.Any(a => a.AttributeTypeName == "MessagePackObjectAttribute");
    }

    /// <summary>
    ///     Find schema by class name (supports partial matching).
    /// </summary>
    public MsgPackClassSchema? FindSchema(string name)
    {
        if (Schemas.TryGetValue(name, out var exact))
        {
            return exact;
        }

        // Try by simple name
        var matches = Schemas.Values.Where(s =>
                s.Name.Equals(name, StringComparison.OrdinalIgnoreCase) ||
                s.FullName.EndsWith("." + name, StringComparison.OrdinalIgnoreCase))
            .ToList();

        return matches.Count == 1 ? matches[0] : null;
    }

    /// <summary>
    ///     Search schemas by pattern (glob-like).
    /// </summary>
    public IEnumerable<MsgPackClassSchema> SearchSchemas(string pattern)
    {
        var lower = pattern.ToLowerInvariant();
        return Schemas.Values.Where(s =>
            s.Name.ToLowerInvariant().Contains(lower) ||
            s.FullName.ToLowerInvariant().Contains(lower));
    }
}