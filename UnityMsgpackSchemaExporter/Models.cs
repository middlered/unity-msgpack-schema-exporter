namespace UnityMsgpackSchemaExporter;

/// <summary>
///     Represents a single field/property in a MessagePackObject class.
/// </summary>
public class MsgPackFieldInfo
{
    /// <summary>String key name (from [Key("name")]) or null if integer-keyed.</summary>
    public string? StringKey { get; set; }

    /// <summary>Integer key (from [Key(0)]) or -1 if string-keyed.</summary>
    public int IntKey { get; set; } = -1;

    /// <summary>C# field/property name in the class.</summary>
    public string MemberName { get; set; } = "";

    /// <summary>C# type string (e.g., "int", "string", "Dictionary&lt;int, MasterCard&gt;").</summary>
    public string TypeName { get; set; } = "";

    /// <summary>Whether this is a property (true) or field (false).</summary>
    public bool IsProperty { get; set; }

    /// <summary>Effective key for sorting: string key or int key converted to string.</summary>
    public string EffectiveKey => StringKey ?? IntKey.ToString();

    public bool IsStringKeyed => StringKey != null;
}

/// <summary>
///     Union sub-type entry: [Union(key, typeof(SubType))]
/// </summary>
public class MsgPackUnionEntry
{
    public int Key { get; set; }
    public string TypeName { get; set; } = "";
}

/// <summary>
///     Represents one [MessagePackObject] class schema.
/// </summary>
public class MsgPackClassSchema
{
    public string FullName { get; set; } = "";
    public string Name { get; set; } = "";
    public string Namespace { get; set; } = "";
    public string? BaseTypeName { get; set; }
    public bool IsAbstract { get; set; }
    public bool IsStringKeyed { get; set; }

    public List<MsgPackFieldInfo> Fields { get; set; } = new();
    public List<MsgPackUnionEntry> Unions { get; set; } = new();

    /// <summary>Assembly file this was loaded from.</summary>
    public string SourceAssembly { get; set; } = "";
}