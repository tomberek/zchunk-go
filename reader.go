package zchunk

import (
	"encoding/binary"
	"io"

	"github.com/folbricht/desync"
)

type reader struct {
	io.Reader
}

// ReadUint32 reads the next 4 bytes from the reader and returns it as little
// endian Uint32
func (r reader) ReadUint32() (uint32, error) {
	b := make([]byte, 4)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

// ReadN returns the next n bytes from the reader or an error if there are not
// enough left
func (r reader) ReadN(n uint64) ([]byte, error) {
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadID reads and returns a ChunkID
func (r reader) ReadID() (desync.ChunkID, error) {
	b := make([]byte, 32)
	if _, err := io.ReadFull(r, b); err != nil {
		return desync.ChunkID{}, err
	}
	return desync.ChunkIDFromSlice(b)
}

// ReadHeader returns the size and type of the element or an error if there
// aren't enough bytes left in the stream
func (r reader) ReadEntry() (h Entry, err error) {
	h.CompressedSize, err = r.ReadUint32()
	if err != nil {
		return
	}
	h.DecompressedSize, err = r.ReadUint32()
	if err != nil {
		return
	}

	h.Checksum, err = r.ReadID()
	return
}
