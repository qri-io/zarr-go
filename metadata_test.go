package zarr

import (
	"encoding/json"
	"os"
	"testing"
)

// https://zarr.readthedocs.io/en/stable/spec/v2.html#metadata
const specExample = `{
  "chunks": [
    1000,
    1000
  ],
	"compressor": {
			"id": "blosc",
			"cname": "lz4",
			"clevel": 5,
			"shuffle": 1
	},
	"dtype": "<f8",
	"fill_value": "NaN",
	"filters": [
			{"id": "delta", "dtype": "<f8", "astype": "<f4"}
	],
	"order": "C",
	"shape": [
			10000,
			10000
	],
	"zarr_format": 2
}`

func TestMetadataSerialization(t *testing.T) {
	m := &ArrayMeta{}
	err := json.Unmarshal([]byte(specExample), m)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsolidatedMetadata(t *testing.T) {
	cm := &ConsolidatedMetadata{}
	f, err := os.Open("./testdata/barbados.zmetadata")
	if err != nil {
		t.Fatal(err)
	}

	if err := json.NewDecoder(f).Decode(cm); err != nil {
		t.Fatal(err)
	}
}
