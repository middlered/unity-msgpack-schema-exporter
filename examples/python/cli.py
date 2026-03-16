#!/usr/bin/env python3
"""CLI entry-point for unity-msgpack-avro."""
import argparse
import base64
import json
import sys
from pathlib import Path

from unity_msgpack_avro import decode, encode, load_bytes


def _load_schema(schema_file: str, class_name: str | None):
    reg, root = load_bytes(Path(schema_file).read_bytes())
    schema = reg.get(class_name) if class_name else root
    if schema is None:
        print(f"Error: class '{class_name}' not found in {schema_file}", file=sys.stderr)
        sys.exit(1)
    return schema


def cmd_decode(args):
    schema = _load_schema(args.schema, args.cls)

    if args.hex:
        data = bytes.fromhex(args.hex)
    elif args.base64:
        data = base64.b64decode(args.base64)
    elif args.file:
        data = Path(args.file).read_bytes()
    else:
        data = sys.stdin.buffer.read()

    result = decode(schema, data)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def cmd_encode(args):
    schema = _load_schema(args.schema, args.cls)

    if args.json_arg:
        obj = json.loads(args.json_arg)
    elif args.json_file:
        obj = json.loads(Path(args.json_file).read_text(encoding="utf-8"))
    else:
        obj = json.load(sys.stdin)

    data = encode(schema, obj)

    if args.output_file:
        Path(args.output_file).write_bytes(data)
        print(f"Written {len(data)} bytes to {args.output_file}", file=sys.stderr)
    elif args.output_base64:
        print(base64.b64encode(data).decode())
    else:
        # Default: hex output
        print(data.hex().upper())


def main():
    parser = argparse.ArgumentParser(description="Decode/encode compact Unity msgpack using Avro schema")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # ── decode ────────────────────────────────────────────────────────────────
    dec = sub.add_parser("decode", help="Decode compact msgpack to JSON")
    dec.add_argument("--schema", required=True, help="Avro schema JSON file")
    dec.add_argument("--class", dest="cls", help="Class name to look up in schema")
    in_grp = dec.add_mutually_exclusive_group()
    in_grp.add_argument("--hex", help="Hex-encoded msgpack input bytes")
    in_grp.add_argument("--base64", help="Base64-encoded msgpack input bytes")
    in_grp.add_argument("--file", help="Binary msgpack input file")
    dec.set_defaults(func=cmd_decode)

    # ── encode ────────────────────────────────────────────────────────────────
    enc = sub.add_parser("encode", help="Encode JSON to compact msgpack")
    enc.add_argument("--schema", required=True, help="Avro schema JSON file")
    enc.add_argument("--class", dest="cls", help="Class name to look up in schema")
    json_grp = enc.add_mutually_exclusive_group()
    json_grp.add_argument("--json", dest="json_arg", metavar="JSON",
                          help="JSON string to encode (inline)")
    json_grp.add_argument("--json-file", dest="json_file", metavar="FILE",
                          help="JSON file to encode")
    out_grp = enc.add_mutually_exclusive_group()
    out_grp.add_argument("--output-file", metavar="FILE",
                         help="Write binary msgpack to file (default: print hex to stdout)")
    out_grp.add_argument("--output-base64", action="store_true",
                         help="Print base64-encoded output instead of hex")
    enc.set_defaults(func=cmd_encode)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
