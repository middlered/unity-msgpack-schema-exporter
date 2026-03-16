using System.Collections.Immutable;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.Metadata.Ecma335;
using System.Reflection.PortableExecutable;

namespace UnityMsgpackSchemaExporter;

/// <summary>
///     Reads .NET assembly metadata using System.Reflection.Metadata (SRM).
///     Using Mono.Cecil stylish interface.
/// </summary>
public sealed class SrmAssemblyReader : IAssemblyReader
{
    private readonly List<LoadedAssembly> _assemblies = [];

    public void LoadDirectory(string directoryPath)
    {
        foreach (var dll in Directory.GetFiles(directoryPath, "*.dll"))
        {
            try
            {
                LoadAssembly(dll);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[WARN] Skipping {Path.GetFileName(dll)}: {ex.Message}");
            }
        }
    }

    public void LoadAssembly(string dllPath)
    {
        var stream = File.OpenRead(dllPath);
        try
        {
            var peReader = new PEReader(stream);
            if (!peReader.HasMetadata)
            {
                peReader.Dispose();
                throw new BadImageFormatException($"No metadata in {dllPath}");
            }

            var mdReader = peReader.GetMetadataReader();
            _assemblies.Add(new LoadedAssembly(dllPath, stream, peReader, mdReader));
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public IEnumerable<ITypeInfo> GetAllTypes()
    {
        foreach (var asm in _assemblies)
        foreach (var typeHandle in asm.MetadataReader.TypeDefinitions)
        {
            var typeDef = asm.MetadataReader.GetTypeDefinition(typeHandle);
            var name = asm.MetadataReader.GetString(typeDef.Name);

            // Skip compiler-generated types like <Module>
            if (name == "<Module>" || name.StartsWith("<")) continue;
            
            yield return new SrmTypeInfo(asm, typeDef, typeHandle);
        }
    }

    public string GetSourceAssembly(ITypeInfo type)
    {
        if (type is SrmTypeInfo srmType)
        {
            return Path.GetFileName(srmType.Assembly.FilePath);
        }

        return "";
    }

    public void Dispose()
    {
        foreach (var asm in _assemblies)
        {
            asm.Dispose();
        }

        _assemblies.Clear();
    }

    internal sealed class LoadedAssembly(string filePath, FileStream stream, PEReader peReader, MetadataReader mdReader)
        : IDisposable
    {
        public string FilePath { get; } = filePath;
        public FileStream Stream { get; } = stream;
        public PEReader PEReader { get; } = peReader;
        public MetadataReader MetadataReader { get; } = mdReader;

        public void Dispose()
        {
            PEReader.Dispose();
            Stream.Dispose();
        }
    }
}

internal sealed class SrmTypeInfo : ITypeInfo
{
    private readonly TypeDefinitionHandle _handle;
    private readonly MetadataReader _reader;
    private readonly TypeDefinition _typeDef;

    private IReadOnlyList<ICustomAttributeInfo>? _attrs;
    private IReadOnlyList<IFieldMemberInfo>? _fields;
    private IReadOnlyList<IPropertyMemberInfo>? _properties;

    public SrmTypeInfo(SrmAssemblyReader.LoadedAssembly assembly, TypeDefinition typeDef, TypeDefinitionHandle handle)
    {
        Assembly = assembly;
        _typeDef = typeDef;
        _handle = handle;
        _reader = assembly.MetadataReader;

        FullName = BuildFullName();
        Name = _reader.GetString(_typeDef.Name);
        Namespace = _reader.GetString(_typeDef.Namespace);
        IsAbstract = (_typeDef.Attributes & TypeAttributes.Abstract) != 0;
        BaseTypeFullName = ResolveBaseType();
    }

    internal SrmAssemblyReader.LoadedAssembly Assembly { get; }

    public string FullName { get; }
    public string Name { get; }
    public string Namespace { get; }
    public string? BaseTypeFullName { get; }
    public bool IsAbstract { get; }

    public IReadOnlyList<ICustomAttributeInfo> CustomAttributes =>
        _attrs ??= ReadCustomAttributes(_typeDef.GetCustomAttributes());

    public IReadOnlyList<IFieldMemberInfo> Fields =>
        _fields ??= ReadFields();

    public IReadOnlyList<IPropertyMemberInfo> Properties =>
        _properties ??= ReadProperties();

    private string BuildFullName()
    {
        var ns = _reader.GetString(_typeDef.Namespace);
        var name = _reader.GetString(_typeDef.Name);
        return string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";
    }

    private string? ResolveBaseType()
    {
        var baseTypeHandle = _typeDef.BaseType;
        if (baseTypeHandle.IsNil) return null;
        return ResolveEntityName(baseTypeHandle);
    }

    private string ResolveEntityName(EntityHandle handle)
    {
        switch (handle.Kind)
        {
            case HandleKind.TypeReference:
                var typeRef = _reader.GetTypeReference((TypeReferenceHandle)handle);
                var ns = _reader.GetString(typeRef.Namespace);
                var name = _reader.GetString(typeRef.Name);
                return string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";

            case HandleKind.TypeDefinition:
                var typeDef = _reader.GetTypeDefinition((TypeDefinitionHandle)handle);
                var ns2 = _reader.GetString(typeDef.Namespace);
                var name2 = _reader.GetString(typeDef.Name);
                return string.IsNullOrEmpty(ns2) ? name2 : $"{ns2}.{name2}";

            case HandleKind.TypeSpecification:
                var typeSpec = _reader.GetTypeSpecification((TypeSpecificationHandle)handle);
                return DecodeTypeSignature(typeSpec.Signature);

            default:
                return "unknown";
        }
    }

    private IReadOnlyList<ICustomAttributeInfo> ReadCustomAttributes(CustomAttributeHandleCollection attrHandles)
    {
        var result = new List<ICustomAttributeInfo>();
        foreach (var attrHandle in attrHandles)
        {
            var attr = _reader.GetCustomAttribute(attrHandle);
            var attrTypeName = GetAttributeTypeName(attr);
            var args = DecodeAttributeArguments(attr);
            result.Add(new SrmCustomAttributeInfo(attrTypeName, args));
        }

        return result;
    }

    private string GetAttributeTypeName(CustomAttribute attr)
    {
        switch (attr.Constructor.Kind)
        {
            case HandleKind.MemberReference:
                var memberRef = _reader.GetMemberReference((MemberReferenceHandle)attr.Constructor);
                return ResolveEntityName(memberRef.Parent);

            case HandleKind.MethodDefinition:
                var methodDef = _reader.GetMethodDefinition((MethodDefinitionHandle)attr.Constructor);
                var declaringType = methodDef.GetDeclaringType();
                var typeDef = _reader.GetTypeDefinition(declaringType);
                var ns = _reader.GetString(typeDef.Namespace);
                var name = _reader.GetString(typeDef.Name);
                return string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";

            default:
                return "unknown";
        }
    }

    private IReadOnlyList<IAttributeArgument> DecodeAttributeArguments(CustomAttribute attr)
    {
        var result = new List<IAttributeArgument>();
        try
        {
            var provider = new SrmAttributeTypeProvider();
            var value = attr.DecodeValue(provider);
            var typeArgIndex = 0;
            foreach (var fixedArg in value.FixedArguments)
            {
                var typeFullName = MapAttributeArgType(fixedArg.Type);
                // For typeof(T) args, the provider captures the simple type name
                var argValue = fixedArg.Value;
                if (fixedArg.Type == typeof(Type) && typeArgIndex < provider.CapturedTypeNames.Count)
                {
                    argValue = provider.CapturedTypeNames[typeArgIndex++];
                }

                result.Add(new SrmAttributeArgument(typeFullName, argValue));
            }
        }
        catch
        {
            // Some attributes may not be decodable; skip them
        }

        return result;
    }

    private static string MapAttributeArgType(Type type)
    {
        if (type == typeof(string)) return "System.String";
        if (type == typeof(int)) return "System.Int32";
        if (type == typeof(bool)) return "System.Boolean";
        if (type == typeof(Type)) return "System.Type";
        return type.FullName ?? type.Name;
    }

    private IReadOnlyList<IFieldMemberInfo> ReadFields()
    {
        var result = new List<IFieldMemberInfo>();
        foreach (var fieldHandle in _typeDef.GetFields())
        {
            var fieldDef = _reader.GetFieldDefinition(fieldHandle);
            var name = _reader.GetString(fieldDef.Name);
            var typeName = DecodeFieldSignature(fieldDef);
            var attrs = ReadCustomAttributes(fieldDef.GetCustomAttributes());
            result.Add(new SrmFieldMemberInfo(name, typeName, attrs));
        }

        return result;
    }

    private IReadOnlyList<IPropertyMemberInfo> ReadProperties()
    {
        var result = new List<IPropertyMemberInfo>();
        foreach (var propHandle in _typeDef.GetProperties())
        {
            var propDef = _reader.GetPropertyDefinition(propHandle);
            var name = _reader.GetString(propDef.Name);
            var typeName = DecodePropertySignature(propDef);
            var attrs = ReadCustomAttributes(propDef.GetCustomAttributes());
            result.Add(new SrmPropertyMemberInfo(name, typeName, attrs));
        }

        return result;
    }

    private string DecodeFieldSignature(FieldDefinition fieldDef)
    {
        var blobReader = _reader.GetBlobReader(fieldDef.Signature);
        var sigReader = new SignatureDecoder<string, object?>(
            new SrmTypeNameProvider(_reader), _reader, null);
        return sigReader.DecodeFieldSignature(ref blobReader);
    }

    private string DecodePropertySignature(PropertyDefinition propDef)
    {
        var blobReader = _reader.GetBlobReader(propDef.Signature);
        var header = blobReader.ReadSignatureHeader();
        if (header.Kind != SignatureKind.Property) return "unknown";

        var paramCount = blobReader.ReadCompressedInteger();
        var sigReader = new SignatureDecoder<string, object?>(
            new SrmTypeNameProvider(_reader), _reader, null);
        return sigReader.DecodeType(ref blobReader);
    }

    private string DecodeTypeSignature(BlobHandle sigHandle)
    {
        var blobReader = _reader.GetBlobReader(sigHandle);
        var sigReader = new SignatureDecoder<string, object?>(
            new SrmTypeNameProvider(_reader), _reader, null);
        return sigReader.DecodeType(ref blobReader);
    }
}

/// <summary>
///     Provides type name formatting for SignatureDecoder, mapping CLR types
///     to C# keyword names (matching Mono.Cecil FormatTypeName behavior).
/// </summary>
internal sealed class SrmTypeNameProvider(MetadataReader reader) : ISignatureTypeProvider<string, object?>
{
    private readonly MetadataReader _reader = reader;

    public string GetPrimitiveType(PrimitiveTypeCode typeCode)
    {
        return typeCode switch
        {
            PrimitiveTypeCode.Boolean => "bool",
            PrimitiveTypeCode.Byte => "byte",
            PrimitiveTypeCode.SByte => "sbyte",
            PrimitiveTypeCode.Char => "char",
            PrimitiveTypeCode.Int16 => "short",
            PrimitiveTypeCode.UInt16 => "ushort",
            PrimitiveTypeCode.Int32 => "int",
            PrimitiveTypeCode.UInt32 => "uint",
            PrimitiveTypeCode.Int64 => "long",
            PrimitiveTypeCode.UInt64 => "ulong",
            PrimitiveTypeCode.Single => "float",
            PrimitiveTypeCode.Double => "double",
            PrimitiveTypeCode.String => "string",
            PrimitiveTypeCode.Object => "object",
            PrimitiveTypeCode.Void => "void",
            PrimitiveTypeCode.IntPtr => "IntPtr",
            PrimitiveTypeCode.UIntPtr => "UIntPtr",
            _ => typeCode.ToString()
        };
    }

    public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind)
    {
        var typeDef = reader.GetTypeDefinition(handle);
        return reader.GetString(typeDef.Name);
    }

    public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind)
    {
        var typeRef = reader.GetTypeReference(handle);
        var name = reader.GetString(typeRef.Name);
        var ns = reader.GetString(typeRef.Namespace);

        // Map well-known CLR types to C# keywords
        var fullName = string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";
        return fullName switch
        {
            "System.String" => "string",
            "System.Int32" => "int",
            "System.Int64" => "long",
            "System.Boolean" => "bool",
            "System.Single" => "float",
            "System.Double" => "double",
            "System.Byte" => "byte",
            "System.UInt32" => "uint",
            "System.UInt64" => "ulong",
            "System.Int16" => "short",
            "System.UInt16" => "ushort",
            "System.SByte" => "sbyte",
            "System.Char" => "char",
            "System.Object" => "object",
            "System.Void" => "void",
            _ => name
        };
    }

    public string GetTypeFromSpecification(MetadataReader reader, object? genericContext,
        TypeSpecificationHandle handle, byte rawTypeKind)
    {
        var typeSpec = reader.GetTypeSpecification(handle);
        var blobReader = reader.GetBlobReader(typeSpec.Signature);
        var decoder = new SignatureDecoder<string, object?>(this, reader, genericContext);
        return decoder.DecodeType(ref blobReader);
    }

    public string GetSZArrayType(string elementType)
    {
        return $"{elementType}[]";
    }

    public string GetArrayType(string elementType, ArrayShape shape)
    {
        return $"{elementType}[]";
    }

    public string GetByReferenceType(string elementType)
    {
        return $"{elementType}&";
    }

    public string GetPointerType(string elementType)
    {
        return $"{elementType}*";
    }

    public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments)
    {
        // Remove `N suffix from generic type name
        var baseName = genericType;
        var idx = baseName.IndexOf('`');
        if (idx > 0) baseName = baseName[..idx];
        var args = string.Join(", ", typeArguments);
        return $"{baseName}<{args}>";
    }

    public string GetGenericMethodParameter(object? genericContext, int index)
    {
        return $"!!{index}";
    }

    public string GetGenericTypeParameter(object? genericContext, int index)
    {
        return $"!{index}";
    }

    public string GetFunctionPointerType(MethodSignature<string> signature)
    {
        return "IntPtr";
    }

    public string GetModifiedType(string modifier, string unmodifiedType, bool isRequired)
    {
        return unmodifiedType;
    }

    public string GetPinnedType(string elementType)
    {
        return elementType;
    }

    public string GetTypeFromHandle(MetadataReader reader, object? genericContext, EntityHandle handle)
    {
        switch (handle.Kind)
        {
            case HandleKind.TypeDefinition:
                return GetTypeFromDefinition(reader, (TypeDefinitionHandle)handle, 0);
            case HandleKind.TypeReference:
                return GetTypeFromReference(reader, (TypeReferenceHandle)handle, 0);
            case HandleKind.TypeSpecification:
                return GetTypeFromSpecification(reader, genericContext, (TypeSpecificationHandle)handle, 0);
            default:
                return "unknown";
        }
    }
}

/// <summary>
///     Decodes attribute constructor argument types for DecodeValue.
///     Maps attribute arguments to their CLR Type equivalents.
///     Captures type names from typeof(T) arguments in CapturedTypeNames.
/// </summary>
internal sealed class SrmAttributeTypeProvider : ICustomAttributeTypeProvider<Type>
{
    /// <summary>Simple type names extracted from typeof(T) constructor arguments, in order.</summary>
    public readonly List<string> CapturedTypeNames = new();

    public Type GetPrimitiveType(PrimitiveTypeCode typeCode)
    {
        return typeCode switch
        {
            PrimitiveTypeCode.Boolean => typeof(bool),
            PrimitiveTypeCode.Byte => typeof(byte),
            PrimitiveTypeCode.SByte => typeof(sbyte),
            PrimitiveTypeCode.Char => typeof(char),
            PrimitiveTypeCode.Int16 => typeof(short),
            PrimitiveTypeCode.UInt16 => typeof(ushort),
            PrimitiveTypeCode.Int32 => typeof(int),
            PrimitiveTypeCode.UInt32 => typeof(uint),
            PrimitiveTypeCode.Int64 => typeof(long),
            PrimitiveTypeCode.UInt64 => typeof(ulong),
            PrimitiveTypeCode.Single => typeof(float),
            PrimitiveTypeCode.Double => typeof(double),
            PrimitiveTypeCode.String => typeof(string),
            PrimitiveTypeCode.Object => typeof(object),
            _ => typeof(object)
        };
    }

    public Type GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind)
    {
        return typeof(Type);
    }

    public Type GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind)
    {
        return typeof(Type);
    }

    public Type GetSZArrayType(Type elementType)
    {
        return elementType.MakeArrayType();
    }

    public Type GetSystemType()
    {
        return typeof(Type);
    }

    public bool IsSystemType(Type type)
    {
        return type == typeof(Type);
    }

    public Type GetTypeFromSerializedName(string name)
    {
        // Assembly-qualified name — extract the simple type name
        var comma = name.IndexOf(',');
        var qualifiedName = comma >= 0 ? name[..comma].Trim() : name;
        var dot = qualifiedName.LastIndexOf('.');
        CapturedTypeNames.Add(dot >= 0 ? qualifiedName[(dot + 1)..] : qualifiedName);
        return typeof(Type);
    }

    public PrimitiveTypeCode GetUnderlyingEnumType(Type type)
    {
        return PrimitiveTypeCode.Int32;
    }
}

internal sealed class SrmCustomAttributeInfo : ICustomAttributeInfo
{
    public SrmCustomAttributeInfo(string attrTypeName, IReadOnlyList<IAttributeArgument> args)
    {
        // Extract simple name from full name
        // e.g., "MessagePack.MessagePackObjectAttribute" -> "MessagePackObjectAttribute")
        var lastDot = attrTypeName.LastIndexOf('.');
        AttributeTypeName = lastDot >= 0 ? attrTypeName[(lastDot + 1)..] : attrTypeName;
        ConstructorArguments = args;
    }

    public string AttributeTypeName { get; }
    public IReadOnlyList<IAttributeArgument> ConstructorArguments { get; }
}

internal sealed class SrmAttributeArgument(
    string typeFullName,
    object? value)
    : IAttributeArgument
{
    public string TypeFullName { get; } = typeFullName;
    public object? Value { get; } = value;
}

internal sealed class SrmFieldMemberInfo(
    string name,
    string formattedTypeName,
    IReadOnlyList<ICustomAttributeInfo> attrs)
    : IFieldMemberInfo
{
    public string Name { get; } = name;
    public string FormattedTypeName { get; } = formattedTypeName;
    public IReadOnlyList<ICustomAttributeInfo> CustomAttributes { get; } = attrs;
}

internal sealed class SrmPropertyMemberInfo(
    string name,
    string formattedTypeName,
    IReadOnlyList<ICustomAttributeInfo> attrs)
    : IPropertyMemberInfo
{
    public string Name { get; } = name;
    public string FormattedTypeName { get; } = formattedTypeName;
    public IReadOnlyList<ICustomAttributeInfo> CustomAttributes { get; } = attrs;
}