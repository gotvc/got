package chunking

// Chunker is the common interface exposed by chunkers
type Chunker interface {
	// Write writes bytes to the chunker, chunks can be produced anywhere in the slice
	Write(p []byte) (int, error)
	// Buffered returns the number of bytes buffered, but not yet made into a chunk.
	Buffered() int
	// Flush forces the production of a chunk, if there is any buffered data.
	Flush() error
}

// ChunkHandler is the type of the function called to recieve chunks.
// the buffer should not be used outside of the function call.
type ChunkHandler = func([]byte) error
