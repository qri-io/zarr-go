package zarr

import (
	"encoding/json"
	"fmt"
)

type MetaType string

const (
	// MTAttributes stores userland metadata keyed by array name
	MTAttributes MetaType = ".zattrs"
	// MTArray is the key for storing metadata on an array store
	MTArray MetaType = ".zarray"
	// MTGroup is the key for storing group definitions on an array store
	MTGroup MetaType = ".zgroup"
	// MTMetadata is the key for composite metadata
	MTMetadata MetaType = ".zmetadata"
)

type MetaTyper interface {
	MetaType() MetaType
}

var metaTypes = map[MetaType]struct{}{
	MTAttributes: {},
	MTArray:      {},
	MTGroup:      {},
}

// relies on the fact that all keynames are 7 characters long
func KeyMetaType(s string) (mt MetaType, ok bool) {
	if len(s) < 7 {
		return mt, false
	}
	mt = MetaType(s[len(s)-7:])
	_, ok = metaTypes[mt]
	return mt, ok
}

type Attributes map[string]interface{}

func (Attributes) MetaType() MetaType { return MTAttributes }

type ConsolidatedMetadata struct {
	ConsolidatedFormat int                  `json:"zarr_consolidated_format"`
	Metadata           map[string]MetaTyper `json:"metadata"`
}

type consolidatedMetaDecoder struct {
	ConsolidatedFormat int
	Metadata           map[string]json.RawMessage
}

func (m *ConsolidatedMetadata) UnmarshalJSON(d []byte) error {
	cd := consolidatedMetaDecoder{}
	if err := json.Unmarshal(d, &cd); err != nil {
		return err
	}
	cm := ConsolidatedMetadata{
		ConsolidatedFormat: cd.ConsolidatedFormat,
		Metadata:           map[string]MetaTyper{},
	}

	for key, data := range cd.Metadata {
		kt, ok := KeyMetaType(key)
		if !ok {
			return fmt.Errorf("invalid consoldated metadata key: %q", key)
		}

		switch kt {
		case MTArray:
			arr := &ArrayMeta{}
			if err := json.Unmarshal(data, arr); err != nil {
				return fmt.Errorf("reading %q metadata: %w", key, err)
			}
			cm.Metadata[key] = arr
		case MTAttributes:
			attr := Attributes{}
			if err := json.Unmarshal(data, &attr); err != nil {
				return fmt.Errorf("reading %q attributes: %w", key, err)
			}
		case MTGroup:
			grp := Group{}
			if err := json.Unmarshal(data, &grp); err != nil {
				return fmt.Errorf("reading %q group: %w", key, err)
			}
		}
	}

	*m = cm
	return nil
}

// Each array requires essential configuration metadata to be stored,
// enabling correct interpretation of the stored data.
// This metadata is encoded using JSON and stored as the value of the
// “.zarray” key within an array store.
type ArrayMeta struct {
	// An integer defining the version of the storage specification to which
	// the array store adheres.
	ZarrFormat int `json:"zarr_format"`
	// A list of integers defining the length of each dimension of the array.
	Shape []int `json:"shape"`
	// A list of integers defining the length of each dimension of a chunk of the
	// array. Note that all chunks within a Zarr array have the same shape.
	Chunks [2]int `json:"chunks"`
	// A string or list defining a valid data type for the array. See also the
	// subsection below on data type encoding.
	Dtype StructuredType `json:"dtype"`
	// A JSON object identifying the primary compression codec and providing
	// configuration parameters, or null if no compressor is to be used. The
	// object MUST contain an "id" key identifying the codec to be used.
	Compressor CompressionMeta `json:"compressor"`

	// A scalar value providing the default value to use for uninitialized
	// portions of the array, or null if no fill_value is to be used.
	// If an array has a fixed length byte string data type (e.g., "|S12"), or a
	// structured data type, and if the fill value is not null, then the fill
	// value MUST be encoded as an ASCII string using the standard Base64
	// alphabet.
	FillValue interface{} `json:"fill_value"`
	// Either “C” or “F”, defining the layout of bytes within each chunk of the
	// array. “C” means row-major order, i.e., the last dimension varies fastest;
	// “F” means column-major order, i.e., the first dimension varies fastest.
	Order string `json:"order"`
	// A list of JSON objects providing codec configurations, or null if no
	// filters are to be applied. Each codec configuration object MUST contain a
	// "id" key identifying the codec to be used.
	Filters []Filter `json:"filters"`

	// optional fields

	// If present, either the string "." or "/"" definining the separator placed
	// between the dimensions of a chunk. If the value is not set, then the
	// default MUST be assumed to be ".", leading to chunk keys of the form “0.0”.
	// Arrays defined with "/" as the dimension separator can be considered to
	// have nested, or hierarchical, keys of the form “0/0” that SHOULD where
	// possible produce a directory-like structure.
	DimensionSeparator string `json:"dimension_separator"`
}

func (a ArrayMeta) MetaType() MetaType { return MTArray }

type Filter struct {
	ID     string `json:"ID"`
	Delta  string `json:"Delta"`
	Dtype  string `json:"Dtype"`
	AsType string `json:"AsType"`
}

const (
	// Not a Number
	FillValueNaN = "NaN"
	// Infinity
	FillValueInfinity = "Infinity"
	// -Infinity
	FillValueNegativeInfinity = "-Infinity"
)
