package zchunk

import (
	"context"
	"fmt"
	"sync"

	"github.com/folbricht/desync"
	concurrently "github.com/tejzpr/ordered-concurrently/v2"
	"golang.org/x/sync/errgroup"
)

type chunkJob struct {
	num          int
	start        uint64
	b            []byte
	recordResult func(num int, clen uint32, dlen uint32, checksum desync.ChunkID)
}

func (c chunkJob) Run() interface{} {
	chunk := desync.NewChunk(c.b)
	res, err := CompressChunk(chunk)
	c.recordResult(c.num, uint32(len(res)), uint32(len(c.b)), chunk.ID())
	if err != nil {
		return nil
	}
	return res
}

// ChunkStream split a file according to a list of chunks obtained from an Index
// and stores them in the provided store
func ChunkStream(ctx context.Context, c desync.Chunker, s ZstdStore, n int) error {
	var (
		mu      sync.Mutex
		in      = make(chan concurrently.WorkFunction, n)
		results = make(map[int]Entry, n)
	)

	// All the chunks are processed in parallel, but we need to preserve the
	// order for later. So add the chunking results to a map, indexed by
	// the chunk number so we can rebuild it in the right order when done
	recordResult := func(num int, clen uint32, dlen uint32, checksum desync.ChunkID) {
		mu.Lock()
		defer mu.Unlock()
		results[num] = Entry{num, clen, dlen, checksum}
	}

	g, ctx := errgroup.WithContext(ctx)
	output := concurrently.Process(in, &concurrently.Options{PoolSize: n, OutChannelBuffer: n * 2})

	// Feed the workers, stop if there are any errors. To keep the index list in
	// order, we calculate the checksum here before handing	them over to the
	// workers for compression and storage. That could probablybe optimized further
	g.Go(func() error {
		var num int // chunk #, so we can re-assemble the index in the right order later
	loop:
		for {
			start, b, err := c.Next()
			if err != nil {
				return err
			}
			if len(b) == 0 {
				break
			}

			// Send it off for compression and storage
			select {
			case <-ctx.Done():
				break loop
			case in <- chunkJob{num: num, start: start, b: b, recordResult: recordResult}:
			}
			num++
		}
		close(in)

		return nil
	})

	// Write frames in-order to output file
	for out := range output {
		v, ok := out.Value.([]byte)
		if !ok {
			return fmt.Errorf("invalid type assertion")
		}
		err := s.StoreChunkOnly(v)
		if err != nil {
			return err
		}
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// All the chunks have been processed and are stored in a map. Now build a
	// list in the correct order to be used in the index below
	entries := make([]Entry, len(results))
	for i := 0; i < len(results); i++ {
		entries[i] = results[i]
	}

	// Build and return the index
	frameSize := uint32(9 + len(results)*(4+4+32))
	index := Frame{
		Magic:     0x184D2A5E,
		FrameSize: frameSize,
		Entries:   entries,
		Footer:    Footer{},
		// FeatureFlags: CaFormatExcludeNoDump | CaFormatSHA512256,
		// ChunkSizeMin: c.Min(),
		// ChunkSizeAvg: c.Avg(),
		// ChunkSizeMax: c.Max(),
		// },
		// Chunks: chunks,
	}
	w := writer{s.fd}
	w.WriteUint32(index.Magic)
	w.WriteUint32(index.FrameSize)

	for i := 0; i < len(results); i++ {
		w.WriteUint32(index.Entries[i].CompressedSize)
		w.WriteUint32(index.Entries[i].DecompressedSize)
		w.Write(index.Entries[i].Checksum[:])
	}

	// fmt.Printf("%+v\n", index.Entries[0])
	// fmt.Printf("%+x\n", index.Entries[0].Checksum)
	w.WriteUint32(uint32(len(results)))
	w.Write([]byte{0x40})
	w.WriteUint32(0x8F92EAB1)
	s.Close()
	return nil
}
