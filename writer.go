package zchunk

// from desync

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/folbricht/desync"
)

type writer struct {
	io.Writer
}

// WriteUint64 converts a number of uint64 values into bytes and writes them
// into the stream. Simplifies working with the zstd format since almost
// everything is expressed as uint32.
func (w writer) WriteUint32(values ...uint32) (int64, error) {
	b := make([]byte, 4*len(values))
	for i, v := range values {
		binary.LittleEndian.PutUint32(b[i*4:i*4+4], v)
	}
	return io.Copy(w, bytes.NewReader(b))
}

// WriteID serializes a ChunkID into a stream
func (w writer) WriteID(c desync.ChunkID) (int64, error) {
	return io.Copy(w, bytes.NewReader(c[:]))
}
