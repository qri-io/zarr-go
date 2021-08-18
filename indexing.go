package zarr

type chunkDimProjection struct {
	// Index of chunk.
	DimChunkIX int
	// Selection of items from chunk array.
	DimChunkSel int
	// Selection of items in target (output) array.
	DimOutSel int
}

type IntDimIndexer struct {
}

// A mapping of items from chunk to output array. Can be used to extract items
// from the chunk array for loading into an output array. Can also be used to
// extract items from a value array for setting/updating in a chunk array.
type chunkProjection struct {
	// Indices of chunk
	ChunkCoords []int
	// Selection of items from chunk array.
	ChunkSelection []int
	// Selection of items in target (output) array.
	OutSelection []int
}
