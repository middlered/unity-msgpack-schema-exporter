// go example: read Unity MsgpackSchemaExporter Avro schemas and decode/encode
// compact msgpack without needing the C# CLI at runtime.
//
// Usage:
//   go run . --schema <schema.json> --class <ClassName> --hex <hex>
//   go run . --schema <schema.json> --demo
//
// Generate schemas with:
//   dotnet <cli.dll> avro <ClassName> --dll <assembly.dll> > schema.json
//   dotnet <cli.dll> avro --all --dll <assembly.dll> > all_schemas.json

// go example: read Unity MsgpackSchemaExporter Avro schemas and decode/encode
// compact msgpack without needing the C# CLI at runtime.
//
// Usage:
//   go run . --schema <schema.json> --class <ClassName> --hex <hex>   (decode)
//   go run . --schema <schema.json> --class <ClassName> --json <json> (encode)
//   go run . --schema <schema.json> --demo
//
// Generate schemas with:
//   dotnet <cli.dll> avro <ClassName> --dll <assembly.dll> > schema.json
//   dotnet <cli.dll> avro --all --dll <assembly.dll> > all_schemas.json

package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/example/unity-msgpack-avro/avroschema"
)

func main() {
	schemaFile := flag.String("schema", "", "Avro schema JSON file (single or --all output)")
	className := flag.String("class", "", "Class name to use as root schema")
	hexData := flag.String("hex", "", "Compact msgpack bytes as hex string to decode")
	jsonData := flag.String("json", "", "JSON string to encode to compact msgpack")
	demo := flag.Bool("demo", false, "Run built-in demo with hard-coded test data")
	flag.Parse()

	if *schemaFile == "" {
		flag.Usage()
		os.Exit(1)
	}

	reg, _, err := avroschema.LoadFile(*schemaFile)
	if err != nil {
		log.Fatalf("load schema: %v", err)
	}

	if *demo {
		runDemo(reg)
		return
	}

	if *className == "" {
		flag.Usage()
		os.Exit(1)
	}

	schema := reg[*className]
	if schema == nil {
		log.Fatalf("class %q not found in schema file; available: %v", *className, schemaNames(reg))
	}

	switch {
	case *hexData != "":
		// ── Decode mode: hex msgpack → JSON ──────────────────────────────────
		data, err := hex.DecodeString(*hexData)
		if err != nil {
			log.Fatalf("hex decode: %v", err)
		}

		value, err := avroschema.Decode(schema, data)
		if err != nil {
			log.Fatalf("decode: %v", err)
		}

		out, _ := json.MarshalIndent(value, "", "  ")
		fmt.Println(string(out))

		// Round-trip: re-encode back to compact msgpack.
		reEncoded, err := avroschema.Encode(schema, value)
		if err != nil {
			log.Fatalf("re-encode: %v", err)
		}
		fmt.Printf("\nRe-encoded: %s\n", hex.EncodeToString(reEncoded))
		fmt.Printf("Match:      %v\n", hex.EncodeToString(reEncoded) == strings.ToUpper(*hexData) ||
			hex.EncodeToString(reEncoded) == strings.ToLower(*hexData))

	case *jsonData != "":
		// ── Encode mode: JSON → compact msgpack hex ───────────────────────────
		var obj any
		if err := json.Unmarshal([]byte(*jsonData), &obj); err != nil {
			log.Fatalf("json parse: %v", err)
		}

		data, err := avroschema.Encode(schema, obj)
		if err != nil {
			log.Fatalf("encode: %v", err)
		}

		fmt.Printf("%s\n", strings.ToUpper(hex.EncodeToString(data)))

		// Show round-trip decode for verification.
		value, err := avroschema.Decode(schema, data)
		if err != nil {
			log.Fatalf("round-trip decode: %v", err)
		}
		out, _ := json.MarshalIndent(value, "", "  ")
		fmt.Printf("\nDecoded back: %s\n", string(out))

	default:
		flag.Usage()
		os.Exit(1)
	}
}

func runDemo(reg avroschema.Registry) {
	fmt.Println("=== UnityMsgpackSchemaExporter Avro/Go demo ===")
	fmt.Println()

	// ── Demo 1: String-keyed record ───────────────────────────────────────────
	// SimpleItem: {name, id, score, active, bigValue} — string keys
	// Produced by: dotnet CLI encode-json SimpleItem < '{"name":"Sword","id":1,"score":9.5,"active":true,"bigValue":9999}'
	demo1Hex := "85A6616374697665C3A862696756616C7565CD270FA2696401A46E616D65A553776F7264A573636F7265CA41180000"
	demoSchema(reg, "SimpleItem", demo1Hex, "String-keyed record (SimpleItem)")

	// ── Demo 2: Int-keyed record ──────────────────────────────────────────────
	// IntKeyedItem: array[id, label, value] at positions 0,1,2
	demo2Hex := "9301A56C6162656LCBXXXXXXXX"
	_ = demo2Hex
	// Build manually: [1, "label", 3.14]
	demo2 := buildIntKeyedItem(1, "hello", 3.14)
	demoSchemaRaw(reg, "IntKeyedItem", demo2, "Int-keyed record (IntKeyedItem)")

	// ── Demo 3: Map with int keys ─────────────────────────────────────────────
	// IntKeyedDict: {id, scores: map<int,float>}
	demo3 := buildIntKeyedDict("obj42", map[int]float32{10: 1.5, 20: 2.5})
	demoSchemaRaw(reg, "IntKeyedDict", demo3, "Map with int keys (IntKeyedDict)")

	fmt.Println("=== done ===")
}

func demoSchema(reg avroschema.Registry, className, hexStr, title string) {
	schema := reg[className]
	if schema == nil {
		fmt.Printf("[%s] schema not found\n\n", title)
		return
	}
	data, err := hex.DecodeString(hexStr)
	if err != nil {
		fmt.Printf("[%s] bad hex: %v\n\n", title, err)
		return
	}
	demoSchemaRaw(reg, className, data, title)
}

func demoSchemaRaw(reg avroschema.Registry, className string, data []byte, title string) {
	schema := reg[className]
	if schema == nil {
		fmt.Printf("[%s] schema not found\n\n", title)
		return
	}
	fmt.Printf("── %s ──\n", title)
	fmt.Printf("Input hex: %s\n", hex.EncodeToString(data))

	value, err := avroschema.Decode(schema, data)
	if err != nil {
		fmt.Printf("decode error: %v\n\n", err)
		return
	}
	out, _ := json.MarshalIndent(value, "", "  ")
	fmt.Println("Decoded:", string(out))

	reEncoded, err := avroschema.Encode(schema, value)
	if err != nil {
		fmt.Printf("encode error: %v\n\n", err)
		return
	}
	match := hex.EncodeToString(reEncoded) == hex.EncodeToString(data)
	fmt.Printf("Re-encoded: %s  (match=%v)\n\n", hex.EncodeToString(reEncoded), match)
}

func schemaNames(reg avroschema.Registry) []string {
	var names []string
	for k, v := range reg {
		if v.Type == "record" && k == v.Name {
			names = append(names, k)
		}
	}
	return names
}

// ── Test data builders ────────────────────────────────────────────────────────

// buildIntKeyedItem encodes [id, label, value] as compact array msgpack.
func buildIntKeyedItem(id int, label string, value float64) []byte {
	// array(3): id(int), label(str|nil), value(float64)
	// Use a simple hand-encoding for the demo.
	schema := &avroschema.Schema{
		Type:  "array",
		Items: &avroschema.Schema{Type: "null"},
	}
	_ = schema
	// Easier: just use the library to encode.
	reg, _, err := avroschema.LoadBytes([]byte(`[
		{"type":"record","name":"IntKeyedItem","namespace":"UnityMsgpackSchemaExporter.TestSchemas",
		 "fields":[
		   {"name":"id","type":"int","msgpack_key":0},
		   {"name":"label","type":["null","string"],"msgpack_key":1},
		   {"name":"value","type":"double","msgpack_key":2}
		 ]}
	]`))
	if err != nil {
		panic(err)
	}
	s := reg["IntKeyedItem"]
	b, err := avroschema.Encode(s, map[string]any{
		"id": id, "label": label, "value": value,
	})
	if err != nil {
		panic(err)
	}
	return b
}

func buildIntKeyedDict(id string, scores map[int]float32) []byte {
	reg, _, err := avroschema.LoadBytes([]byte(`[
		{"type":"record","name":"IntKeyedDict","namespace":"UnityMsgpackSchemaExporter.TestSchemas",
		 "fields":[
		   {"name":"id","type":"string","msgpack_key":"id"},
		   {"name":"scores","type":{"type":"map","values":"float","msgpack_key_type":"int"},"msgpack_key":"scores"}
		 ]}
	]`))
	if err != nil {
		panic(err)
	}
	s := reg["IntKeyedDict"]

	// Convert map[int]float32 -> map[any]any
	scoreMap := make(map[any]any, len(scores))
	for k, v := range scores {
		scoreMap[k] = v
	}

	b, err := avroschema.Encode(s, map[string]any{
		"id": id, "scores": scoreMap,
	})
	if err != nil {
		panic(err)
	}
	return b
}
