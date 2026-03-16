using MessagePack;

namespace UnityMsgpackSchemaExporter.TestSchemas;

// ==============================
// 1. Simple string-keyed class
// ==============================
[MessagePackObject]
public class SimpleItem
{
    [Key("active")] public bool active;

    [Key("bigValue")] public long bigValue;

    [Key("id")] public int id;

    [Key("name")] public string name = "";

    [Key("score")] public float score;
}

// ==============================
// 2. Integer-keyed class
// ==============================
[MessagePackObject]
public class IntKeyedItem
{
    [Key(0)] public int id;

    [Key(1)] public string label = "";

    [Key(2)] public double value;
}

// ==============================
// 3. Nested types (string-keyed)
// ==============================
[MessagePackObject]
public class OrderItem
{
    [Key("productId")] public int productId;

    [Key("quantity")] public int quantity;

    [Key("unitPrice")] public float unitPrice;
}

[MessagePackObject]
public class Order
{
    [Key("customer")] public string customer = "";

    [Key("items")] public List<OrderItem> items = new();

    [Key("metadata")] public Dictionary<string, string> metadata = new();

    [Key("orderId")] public int orderId;

    [Key("tags")] public string[] tags = Array.Empty<string>();
}

// ==============================
// 4. Nullable fields
// ==============================
[MessagePackObject]
public class NullableFields
{
    [Key("id")] public int id;

    [Key("optionalBool")] public bool? optionalBool;

    [Key("optionalInt")] public int? optionalInt;

    [Key("optionalLong")] public long? optionalLong;

    [Key("optionalStr")] public string? optionalStr;
}

// ==============================
// 5. Dictionary with int keys
// ==============================
[MessagePackObject]
public class IntKeyedDict
{
    [Key("id")] public int id;

    [Key("scores")] public Dictionary<int, float> scores = new();
}

// ==============================
// 6. Complex nested: Dict<int, List<NestedObj>>
// ==============================
[MessagePackObject]
public class Tag
{
    [Key("label")] public string label = "";

    [Key("tagId")] public int tagId;
}

[MessagePackObject]
public class ComplexNested
{
    [Key("id")] public int id;

    [Key("singleTag")] public Tag? singleTag;

    [Key("tagArray")] public Tag[] tagArray = Array.Empty<Tag>();

    [Key("tagGroups")] public Dictionary<int, List<Tag>> tagGroups = new();
}

// ==============================
// 7. Deeply nested (3 levels)
// ==============================
[MessagePackObject]
public class Level3
{
    [Key("value")] public string value = "";
}

[MessagePackObject]
public class Level2
{
    [Key("children")] public List<Level3> children = new();

    [Key("name")] public string name = "";
}

[MessagePackObject]
public class Level1
{
    [Key("id")] public int id;

    [Key("items")] public List<Level2> items = new();
}

// ==============================
// 8. HashSet field
// ==============================
[MessagePackObject]
public class WithHashSet
{
    [Key("id")] public int id;

    [Key("memberIds")] public HashSet<int> memberIds = new();
}

// ==============================
// 9. Mixed int-key with gaps
// ==============================
[MessagePackObject]
public class SparseIntKeys
{
    [Key(0)] public int a;

    [Key(2)] public string b = "";

    [Key(5)] public bool c;
}

// ==============================
// 10. Enum-like string field (no real enum type)
// ==============================
[MessagePackObject]
public class WithEnumLike
{
    [Key("id")] public int id;

    [Key("priority")] public int priority;

    [Key("status")] public string status = ""; // e.g. "active", "inactive"
}

// ==============================
// 11. Int-keyed class containing string-keyed nested class
// ==============================
[MessagePackObject]
public class IntKeyedWithStringChild
{
    [Key(0)] public int id;
    [Key(1)] public Tag? tag; // string-keyed child
    [Key(2)] public List<Tag> tags = new();
}

// ==============================
// 12. String-keyed class containing int-keyed nested class
// ==============================
[MessagePackObject]
public class StringKeyedWithIntChild
{
    [Key("child")] public IntKeyedItem? child; // int-keyed child
    [Key("children")] public List<IntKeyedItem> children = new();
    [Key("id")] public int id;
}

// ==============================
// 13. Int-keyed class containing int-keyed nested class
// ==============================
[MessagePackObject]
public class IntKeyedWithIntChild
{
    [Key(1)] public IntKeyedItem? child; // int-keyed child
    [Key(2)] public List<IntKeyedItem> children = new();
    [Key(0)] public int id;
}

// ==============================
// 14. Union type — string-keyed children
// ==============================
[MessagePackObject]
[Union(0, typeof(UnionChildA))]
[Union(1, typeof(UnionChildB))]
public abstract class UnionBase
{
}

[MessagePackObject]
public class UnionChildA : UnionBase
{
    [Key("label")] public string label = "";
    [Key("value")] public int value;
}

[MessagePackObject]
public class UnionChildB : UnionBase
{
    [Key("enabled")] public bool enabled;
    [Key("message")] public string message = "";
}

[MessagePackObject]
public class MessageWithUnion
{
    [Key("id")] public int id;
    [Key("payload")] public UnionBase? payload;
    [Key("payloads")] public List<UnionBase> payloads = new();
}

// ==============================
// 15. Union type — int-keyed children
// ==============================
[MessagePackObject]
[Union(0, typeof(IntUnionChildA))]
[Union(1, typeof(IntUnionChildB))]
public abstract class IntUnionBase
{
}

[MessagePackObject]
public class IntUnionChildA : IntUnionBase
{
    [Key(1)] public string name = "";
    [Key(0)] public int num;
}

[MessagePackObject]
public class IntUnionChildB : IntUnionBase
{
    [Key(0)] public string data = "";
    [Key(1)] public bool flag;
}

[MessagePackObject]
public class IntKeyedWithUnion
{
    [Key(0)] public int id;
    [Key(1)] public IntUnionBase? payload;
    [Key(2)] public List<IntUnionBase> payloads = new();
}

// ==============================
// 16. All primitive types
// ==============================
[MessagePackObject]
public class AllPrimitiveTypes
{
    [Key("boolVal")] public bool boolVal;
    [Key("bytesVal")] public byte[]? bytesVal;
    [Key("byteVal")] public byte byteVal;
    [Key("doubleVal")] public double doubleVal;
    [Key("floatVal")] public float floatVal;
    [Key("intVal")] public int intVal;
    [Key("longVal")] public long longVal;
    [Key("sbyteVal")] public sbyte sbyteVal;
    [Key("shortVal")] public short shortVal;
    [Key("stringVal")] public string stringVal = "";
    [Key("uintVal")] public uint uintVal;
    [Key("ulongVal")] public ulong ulongVal;
    [Key("ushortVal")] public ushort ushortVal;
}

// ==============================
// 17. Property-keyed class (uses properties instead of fields)
// ==============================
[MessagePackObject]
public class PropertyKeyed
{
    [Key("id")] public int Id { get; set; }

    [Key("name")] public string Name { get; set; } = "";

    [Key("active")] public bool Active { get; set; }
}

// ==============================
// 18. Mixed fields and properties
// ==============================
[MessagePackObject]
public class MixedFieldsAndProperties
{
    [Key(0)] public int fieldValue;

    [Key(1)] public string PropertyValue { get; set; } = "";
}