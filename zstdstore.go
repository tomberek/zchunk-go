package zchunk

import (
	"context"
	"io"
	"os"

	"github.com/folbricht/desync"
)

var _ desync.WriteStore = ZstdStore{}

const (
	tmpChunkPrefix = ".tmp-cacnk"
)

// LocalStore casync store
type ZstdStore struct {
	Base string
	fd   io.ReadWriteCloser
}

// NewZstdStore creates an instance of a seekable Zstandard file, it only checks presence
// of the store
func NewZstdStore(file string) (ret ZstdStore, err error) {
	if _, err = os.Stat(file); err == nil {
		//return ZstdStore{}, fmt.Errorf("%s exists", file)
	} else if os.IsNotExist(err) {
		ret = ZstdStore{Base: file}

	} else {
		return ZstdStore{}, err
	}
	var fd *os.File
	if file == "-" {
	} else {
		fd, err = os.Create(file)
		if err != nil {
			return ZstdStore{}, err
		}
	}
	ret.fd = fd
	return ret, nil
}

// GetChunk reads and returns one (compressed!) chunk from the store
func (s ZstdStore) GetChunk(id desync.ChunkID) (*desync.Chunk, error) {
	panic("not needed")
}

// RemoveChunk deletes a chunk, typically an invalid one, from the filesystem.
// Used when verifying and repairing caches.
func (s ZstdStore) RemoveChunk(id desync.ChunkID) error {
	panic("can't remove")
}

// StoreChunk adds a new chunk to the store
func (s ZstdStore) StoreChunk(chunk *desync.Chunk) error {
	b, err := chunk.Data()
	if err != nil {
		return err
	}
	b, err = desync.Compress(b)
	if err != nil {
		return err
	}
	if _, err = s.fd.Write(b); err != nil {
		s.fd.Close()
		os.Remove(s.Base) // clean up
		return err
	}
	return nil
}

// StoreChunkOnly adds a new chunk to the store
func (s ZstdStore) StoreChunkOnly(b []byte) error {
	if _, err := s.fd.Write(b); err != nil {
		s.fd.Close()
		os.Remove(s.Base) // clean up
		return err
	}
	return nil
}

// CompressChunk adds a new chunk to the store
func CompressChunk(chunk *desync.Chunk) ([]byte, error) {
	b, err := chunk.Data()
	if err != nil {
		return nil, err
	}
	return desync.Compress(b)
}

// Verify all chunks in the store. If repair is set true, bad chunks are deleted.
// n determines the number of concurrent operations. w is used to write any messages
// intended for the user, typically os.Stderr.
func (s ZstdStore) Verify(ctx context.Context, n int, repair bool, w io.Writer) error {
	panic("verify not implemented")
}

// Prune removes any chunks from the store that are not contained in a list
// of chunks
func (s ZstdStore) Prune(ctx context.Context, ids map[desync.ChunkID]struct{}) error {
	panic("prune not implemented")
}

// HasChunk returns true if the chunk is in the store
func (s ZstdStore) HasChunk(id desync.ChunkID) (bool, error) {
	panic("haschunk not implemented")
}

func (s ZstdStore) String() string {
	return s.Base
}

// Close the store. NOP opertation, needed to implement Store interface.
func (s ZstdStore) Close() error {
	if s.fd == nil {
		return nil
	}
	return s.fd.Close()
}
