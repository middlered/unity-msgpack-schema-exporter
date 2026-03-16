namespace UnityMsgpackSchemaExporter;

/// <summary>
///     Abstraction layer for reading .NET assembly metadata.
///     Replaces direct Mono.Cecil dependency with a pluggable interface.
/// </summary>
public interface IAssemblyReader : IDisposable
{
    /// <summary>Load all DLL files from a directory.</summary>
    void LoadDirectory(string directoryPath);

    /// <summary>Load a single DLL file.</summary>
    void LoadAssembly(string dllPath);

    /// <summary>Get all types across all loaded assemblies.</summary>
    IEnumerable<ITypeInfo> GetAllTypes();

    /// <summary>Get the source assembly file name for a type.</summary>
    string GetSourceAssembly(ITypeInfo type);
}

/// <summary>Represents a type definition in a loaded assembly.</summary>
public interface ITypeInfo
{
    string FullName { get; }
    string Name { get; }
    string Namespace { get; }
    string? BaseTypeFullName { get; }
    bool IsAbstract { get; }
    IReadOnlyList<ICustomAttributeInfo> CustomAttributes { get; }
    IReadOnlyList<IFieldMemberInfo> Fields { get; }
    IReadOnlyList<IPropertyMemberInfo> Properties { get; }
}

/// <summary>Represents a custom attribute on a type or member.</summary>
public interface ICustomAttributeInfo
{
    string AttributeTypeName { get; }
    IReadOnlyList<IAttributeArgument> ConstructorArguments { get; }
}

/// <summary>Represents a constructor argument of a custom attribute.</summary>
public interface IAttributeArgument
{
    string TypeFullName { get; }
    object? Value { get; }
}

/// <summary>Represents a field member of a type.</summary>
public interface IFieldMemberInfo
{
    string Name { get; }
    string FormattedTypeName { get; }
    IReadOnlyList<ICustomAttributeInfo> CustomAttributes { get; }
}

/// <summary>Represents a property member of a type.</summary>
public interface IPropertyMemberInfo
{
    string Name { get; }
    string FormattedTypeName { get; }
    IReadOnlyList<ICustomAttributeInfo> CustomAttributes { get; }
}