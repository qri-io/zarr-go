package zarr

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	// Version is the current version of this library.
	Version = -1
)

type Array struct {
	path  Path
	store Store
	mode  PersistenceMode
	meta  *ArrayMeta
}

func Create(m *ArrayMeta) (*Array, error) {
	return nil, fmt.Errorf("unfinished: Array.Create")
}

func Empty(m *ArrayMeta) (*Array, error) {
	return nil, fmt.Errorf("unfinished: Array.Empty")
}

func Zeros(m *ArrayMeta) (*Array, error) {
	return nil, fmt.Errorf("unfinished: Array.Zeros")
}

func Ones(m *ArrayMeta) (*Array, error) {
	return nil, fmt.Errorf("unfinished: Array.Ones")
}

func Open(store Store, path string, mode PersistenceMode) (*Array, error) {
	p, err := NewPath(path)
	if err != nil {
		return nil, err
	}

	a := &Array{
		path:  p,
		store: store,
		mode:  mode,
	}

	mp := p.Join(string(MTArray)).String()
	if f, err := store.Get(mp); err == nil {
		a.meta = &ArrayMeta{}
		if err := json.NewDecoder(f).Decode(a.meta); err != nil {
			return nil, err
		}
	}

	return a, nil
}

func (a *Array) Info() string {
	return "<zarr-go.Array>"
}

func (a *Array) Slice(start, stop int) ([]interface{}, error) {
	return nil, fmt.Errorf("unfinished: Array.Slice")
}

func (a *Array) Path() string {
	return strings.Join(a.path, "/")
}

func (a *Array) ReadAll() (interface{}, error) {
	chunkLength := 100
	bo, fac := a.newValueFunc(chunkLength)
	for i := 0; i < a.meta.Chunks[0]; i++ {
		for j := 0; j < a.meta.Chunks[1]; j++ {
			f, err := a.openChunk([2]int{i, j})
			if err != nil {
				return nil, err
			}

			// data, err := ioutil.ReadAll(f)
			// if err != nil {
			// 	return nil, err
			// }
			// fmt.Printf("%d.%d: %x\n\n", i, j, data)
			v := fac()
			if err := binary.Read(f, bo, v); err != nil {
				return nil, err
			}
			fmt.Printf("%d.%d chunk vals: %#v\n", i, j, v)
		}
	}

	return nil, nil
}

func (a *Array) newValueFunc(size int) (binary.ByteOrder, func() interface{}) {
	var order binary.ByteOrder
	switch a.meta.Dtype.Dtype.ByteOrder {
	case BOBigEndian, BONotRelevant:
		order = binary.BigEndian
	case BOLittleEndian:
		order = binary.LittleEndian
	}

	var factory func() interface{}
	switch a.meta.Dtype.Dtype.BasicType {
	case BTBoolean:
		factory = func() interface{} { return make([]bool, size, size) }
	case BTInteger:
		switch a.meta.Dtype.Dtype.ByteSize {
		case 1:
			factory = func() interface{} { return make([]int8, size, size) }
		case 2:
			factory = func() interface{} { return make([]int16, size, size) }
		case 4:
			factory = func() interface{} { return make([]int32, size, size) }
		case 8:
			factory = func() interface{} { return make([]int64, size, size) }
		default:
			factory = func() interface{} { return make([]int, size, size) }
		}
	case BTUnsigned:
		switch a.meta.Dtype.Dtype.ByteSize {
		case 1:
			factory = func() interface{} { return make([]uint8, size, size) }
		case 2:
			factory = func() interface{} { return make([]uint16, size, size) }
		case 4:
			factory = func() interface{} { return make([]uint32, size, size) }
		case 8:
			factory = func() interface{} { return make([]uint64, size, size) }
		default:
			factory = func() interface{} { return make([]uint, size, size) }
		}
	case BTFloatingPoint:
		switch a.meta.Dtype.Dtype.ByteSize {
		case 4:
			factory = func() interface{} { return make([]float32, size, size) }
		case 8:
			factory = func() interface{} { return make([]float64, size, size) }
		default:
			factory = func() interface{} { return make([]float64, size, size) }
		}
	case BTComplex:
		switch a.meta.Dtype.Dtype.ByteSize {
		case 8:
			factory = func() interface{} { return make([]complex64, size, size) }
		case 16:
			factory = func() interface{} { return make([]complex128, size, size) }
		default:
			factory = func() interface{} { return make([]complex128, size, size) }
		}
	// case BTTimedelta:
	// case BTDatetime:
	// case BTString:
	// case BTUnicode:
	// case BTOther:
	default:
		panic("unsupported decoding type")
	}

	return order, factory
}

func (a *Array) openChunk(ch [2]int) (io.ReadCloser, error) {
	f, err := a.store.Get(a.chunkPath(ch).String())
	if err != nil {
		return nil, err
	}
	return a.meta.Compressor.Decompressor(f)
}

func (a *Array) chunkPath(ch [2]int) Path {
	return a.path.Join(fmt.Sprintf("%d.%d", ch[0], ch[1]))
}

type PersistenceMode string

const (
	// Persistence mode:
	// ‘r’ means read only (must exist);
	ModeRead PersistenceMode = "r"
	//‘r+’ means read/write (must exist)
	ModeReadWrite PersistenceMode = "r+"
	// ‘a’ means read/write (create if doesn’t exist)
	ModeReadWriteCreate PersistenceMode = "a"
	// ‘w’ means create (overwrite if exists)
	ModeWrite PersistenceMode = "w"
	// ‘w-’ means create (fail if exists).
	ModeWriteFail PersistenceMode = "w-"
)

// Arrays can be organized into groups which can also contain other groups.
// A group is created by storing group ArrayMeta under the “.zgroup” key under
// some logical path. E.g., a group exists at the root of an array store if the
// “.zgroup” key exists in the store, and a group exists at logical path
// “foo/bar” if the “foo/bar/.zgroup” key exists in the store.
type Group struct {
	ZarrFormat int `json:"zarr_format"`
}

type Path []string

// TODO(b5):
// To ensure consistent behaviour across different storage systems,
// logical paths MUST be normalized as follows:
// * Replace all backward slash characters (”\”) with forward slash characters (“/”)
// * Strip any leading “/” characters
// * Strip any trailing “/” characters
// * Collapse any sequence of more than one “/” character into a single “/” character
func NewPath(posix string) (Path, error) {
	return strings.Split(posix, "/"), nil
}

func (p Path) String() string {
	return strings.Join(p, "/")
}

func (p Path) Shift() (head string, ch Path) {
	switch len(p) {
	case 0:
		return "", nil
	case 1:
		return p[0], nil
	default:
		return p[0], p[1:]
	}
}

func (p Path) Join(elems ...string) Path {
	return append(p, elems...)
}
