package zarr

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Dtype is the set of all zarr data types
// Simple data types as a string following the NumPy array protocol type string
// (typestr) format. The format consists of 3 parts:
//  * One character describing the byteorder of the data:
//    "<": little-endian; ">": big-endian; "|": not-relevant)
//  * One character code giving the basic type of the array:
//    * "b": Boolean (integer type where all values are only True or False)
//    * "i": integer;
//    * "u": unsigned integer
//    * "f": floating point
//    * "c": complex floating point
//    * "m": timedelta;
//    * "M": datetime
//    * "S": string (fixed-length sequence of char)
//    * "U": unicode (fixed-length sequence of Py_UNICODE)
//    * "V": other (void * – each item is a fixed-size chunk of memory))
//  * An integer specifying the number of bytes the type uses.
//
// The byte order is optional in some circumstances, within the zarr format
// byte order MUST be specified
type Dtype struct {
	ByteOrder ByteOrder
	BasicType BasicType
	ByteSize  int
	Units     string
}

var (
	_ json.Unmarshaler = (*Dtype)(nil)
	_ json.Marshaler   = (*Dtype)(nil)
)

func ParseDtype(s string) (dt Dtype, err error) {
	// bug in python implementation uses HTML escape sequences when serializaing JSON
	s = strings.Replace(s, "&lt;", "<", 1)
	s = strings.Replace(s, "&gt;", ">", 1)

	if len(s) < 3 {
		return dt, fmt.Errorf("invalid Dtype string. %q is too short", s)
	}

	boByte, s := s[0], s[1:]
	dt.ByteOrder, err = ParseByteOrder(rune(boByte))
	if err != nil {
		return dt, err
	}

	typeByte, s := s[0], s[1:]
	dt.BasicType, err = ParseBasicType(rune(typeByte))
	if err != nil {
		return dt, err
	}

	var sizeStr, unitStr string
	for i, b := range s {
		if b == '[' {
			unitStr = s[i:]
			break
		}
		sizeStr += string(b)
	}

	size, err := strconv.ParseInt(sizeStr, 10, 0)
	if err != nil {
		return dt, err
	}
	dt.ByteSize = int(size)

	// TODO(b5): validate unit string
	dt.Units = unitStr

	return dt, nil
}

func (dt Dtype) String() string {
	s := fmt.Sprintf("%s%s%d", string(dt.ByteOrder), string(dt.BasicType), dt.ByteSize)
	if dt.Units != "" {
		s += dt.Units
	}
	return s
}

func (dt Dtype) MarshalJSON() ([]byte, error) {
	return []byte(`"` + dt.String() + `"`), nil
}

func (dt *Dtype) UnmarshalJSON(d []byte) error {
	var s string
	if err := json.Unmarshal(d, &s); err != nil {
		return err
	}
	t, err := ParseDtype(s)
	if err != nil {
		return err
	}

	*dt = t
	return nil
}

type ByteOrder rune

func ParseByteOrder(r rune) (ByteOrder, error) {
	o := ByteOrder(r)
	if _, ok := byteOrders[o]; !ok {
		return o, fmt.Errorf("unsupported byte order format: %q", r)
	}
	return o, nil
}

const (
	BONotRelevant  ByteOrder = '|'
	BOLittleEndian ByteOrder = '<'
	BOBigEndian    ByteOrder = '>'
)

var byteOrders = map[ByteOrder]struct{}{
	BONotRelevant:  {},
	BOLittleEndian: {},
	BOBigEndian:    {},
}

type BasicType rune

func ParseBasicType(r rune) (BasicType, error) {
	t := BasicType(r)
	if _, ok := supportedBasicTypes[t]; !ok {
		return t, fmt.Errorf("unsupported byte order format: %q", r)
	}
	return t, nil
}

func (bt BasicType) Human() string {
	return supportedBasicTypes[bt]
}

const (
	BTBoolean       BasicType = 'b'
	BTInteger       BasicType = 'i'
	BTUnsigned      BasicType = 'u'
	BTFloatingPoint BasicType = 'f'
	BTComplex       BasicType = 'c'
	BTTimedelta     BasicType = 'm'
	BTDatetime      BasicType = 'M'
	BTString        BasicType = 'S'
	BTUnicode       BasicType = 'U'
	BTOther         BasicType = 'V'
)

// TODO(b5): human names need to be matched to the python implementation?
var supportedBasicTypes = map[BasicType]string{
	BTBoolean:       "bool",
	BTInteger:       "int",
	BTUnsigned:      "uint",
	BTFloatingPoint: "float64",
	BTComplex:       "complex",
	BTTimedelta:     "timeDelta",
	BTDatetime:      "dateTime",
	BTString:        "string",
	BTUnicode:       "unicode",
	BTOther:         "other",
}

type StructuredType struct {
	Fieldname string
	Dtype     Dtype
	Shape     interface{}
	Children  []StructuredType
}

var (
	_ json.Unmarshaler = (*Dtype)(nil)
	_ json.Marshaler   = (*Dtype)(nil)
)

func ParseStructuredType(d interface{}) (StructuredType, error) {
	switch v := d.(type) {
	case string:
		// string is a Dtype literal
		dt, err := ParseDtype(v)
		if err != nil {
			return StructuredType{}, err
		}
		return StructuredType{Dtype: dt}, nil
	case []interface{}:
		return parseStructuredTypeSlice(v)
	default:
		return StructuredType{}, fmt.Errorf("unexpected type %T", d)
	}
}

func parseStructuredTypeSlice(d []interface{}) (StructuredType, error) {
	if len(d) == 1 {
		childSlice, ok := d[0].([]interface{})
		if !ok {
			return StructuredType{}, fmt.Errorf("expected single element array to contain an array of structure types")
		}
		parent := StructuredType{}
		for i, el := range childSlice {
			ch, err := ParseStructuredType(el)
			if err != nil {
				return StructuredType{}, fmt.Errorf("element %d: %w", i, err)
			}
			parent.Children = append(parent.Children, ch)
		}
		// return StructuredType{}, fmt.Errorf("invalid structured Dtype: %q is too short", string(d))
		return parent, nil
	} else if len(d) < 2 {
		return StructuredType{}, fmt.Errorf("invalid structured Dtype: %q is too short", len(d))
	}

	t := StructuredType{}
	fieldName, ok := d[0].(string)
	if !ok {
		return StructuredType{}, fmt.Errorf("invalid structured Dtype: field name must be a string. got %T", d[0])
	}
	t.Fieldname = fieldName

	switch x := d[1].(type) {
	case string:
		dtype, err := ParseDtype(x)
		if err != nil {
			return StructuredType{}, err
		}
		t.Dtype = dtype
	case []interface{}:
		ch, err := ParseStructuredType(x)
		if err != nil {
			return StructuredType{}, err
		}
		t.Children = append(t.Children, ch)
	default:
		return t, fmt.Errorf("invalid structured Dtype: want either string or Structured Type. got %T", d[1])
	}

	// TODO (b5): shape parsing
	if len(d) > 2 {
		t.Shape = d[2]
	}

	return t, nil
}

func (st *StructuredType) IsBasic() bool {
	return st.Fieldname == "" && st.Shape == nil
}

func (st *StructuredType) Human() string {
	if st.IsBasic() {
		return st.Dtype.BasicType.Human()
	}
	return "struct"
}

func (st *StructuredType) MarshalJSON() ([]byte, error) {
	if st.IsBasic() {
		return st.Dtype.MarshalJSON()
	}

	d := []interface{}{
		st.Fieldname,
		st.Dtype,
	}
	if st.Shape != nil {
		d = append(d, st.Shape)
	}

	return json.Marshal(d)
}

func (st *StructuredType) UnmarshalJSON(d []byte) error {
	var v interface{}
	if err := json.Unmarshal(d, &v); err != nil {
		return err
	}

	t, err := ParseStructuredType(v)
	if err != nil {
		return err
	}

	*st = t
	return nil
}
