import json

import msgpack
from unity_msgpack_schema import MsgPackSchema

schema = MsgPackSchema("UnityMsgpackSchemaExporter.TestSchemas.dll", lib_path=r"/workspaces/dump/MsgPackSchemaTools/pybind/UnityMsgpackSchemaExporter.Native.a")
print("Schema Loaded")

# List all MessagePackObject classes
classes = schema.list_classes()
print("schema:", classes, "\n")

# Get schema for a specific class
info = schema.get_schema("UnityMsgpackSchemaExporter.TestSchemas.IntKeyedWithIntChild")
print("info:", info, "\n")

avro = schema.avro_schema("IntKeyedWithIntChild")  # support get class without namespace
print("avro:\n" + json.dumps(avro, indent=4), "\n")

# Doing encode/decode jobs
sample_dict = {
    "id": 1024,
    "child": None,
    "children": [{"id": 1, "label": "test1", "value": 1.1}],
}

sample_key_msgpack: bytes = msgpack.packb(sample_dict)  # type: ignore
print("sample key_msgpack:", sample_key_msgpack, "\n")

encoded = schema.encode("IntKeyedWithIntChild", sample_key_msgpack)
print("encode:", encoded, "\n")

raw_decoded = msgpack.unpackb(encoded, strict_map_key=False)
print("raw_decoded:", raw_decoded, "\n")

decoded = schema.decode("IntKeyedWithIntChild", encoded)
print("decoded:", decoded, "\n")

print("is_same:", sample_key_msgpack == decoded)

# Clean up
schema.close()
