package zarr

import (
	"io"

	"github.com/qri-io/dataset/compression"
)

// CompressionMeta defines compression settings zarr-go understands
type CompressionMeta struct {
	ID      string `json:"id"`
	Cname   string `json:"cname,omitempty"`
	Clevel  int    `json:"clevel,omitempty"`
	Shuffle int    `json:"shuffle,omitempty"`
}

func (m *CompressionMeta) Decompressor(r io.ReadCloser) (io.ReadCloser, error) {
	return compression.Decompressor(m.ID, r)
}
