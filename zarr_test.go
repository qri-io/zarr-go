package zarr

import "testing"

var metaOne = &ArrayMeta{
	ZarrFormat: Version,
	Shape:      []int{10000, 10000},
	Chunks:     [2]int{1000, 1000},
}

func TestZarr(t *testing.T) {
	s := NewMemoryStore()
	z, err := Open(s, "foo/bar", ModeReadWrite)
	if err != nil {
		t.Fatal(err)
	}

	res, err := z.Slice(0, 1)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(res)
}

/*
import zarr
import numpy as np
from numcodecs import Zstd
z1 = zarr.open('testdata/int32_100x100_chunk_10x10_.zarr', mode='w', shape=(100, 100), chunks=(10, 10), dtype='i4', compressor=Zstd())
z1[:] = 20
*/
func TestReadAll(t *testing.T) {
	s, err := NewLocalStore("./testdata")
	if err != nil {
		t.Fatal(err)
	}

	a, err := Open(s, "int32_100x100_chunk_10x10_.zarr", ModeReadWrite)
	if err != nil {
		t.Fatal(err)
	}

	_, err = a.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
}
