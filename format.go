package zchunk

import (
	"io"
	"io/ioutil"
)

const (
	FrameMagic uint32 = 0x184D2A5E
)

type Frame struct {
	Magic     uint32
	FrameSize uint32
	Entries   []Entry
	Footer
}
type Footer struct {
	Number     uint32
	Descriptor byte
	Magic      uint32
}

type Entry struct {
	Num              int
	CompressedSize   uint32
	DecompressedSize uint32
	// The hash
	Checksum [32]byte
}

// FrameDecoder is used to parse and break up a stream of casync format elements
// found in archives or index files.

type FrameDecoder struct {
	r       reader
	advance io.Reader
}

func NewFrameDecoder(r io.Reader) FrameDecoder {
	return FrameDecoder{r: reader{r}}
}

// Next returns the next frame element from the stream. If an element
// contains a reader, that reader should be used before any subsequent calls as
// it'll be invalidated then. Returns nil when the end is reached.
func (d *FrameDecoder) Next() (interface{}, error) {
	// If we previously returned a reader, make sure we advance all the way in
	// case the caller didn't read it all.
	if d.advance != nil {
		io.Copy(ioutil.Discard, d.advance)
		d.advance = nil
	}
	switch d {
	// hdr, err := d.r.ReadHeader()
	// if err != nil {
	// 	if err == io.EOF {
	// 		return nil, nil
	// 	}
	// 	return nil, err
	// }
	// switch hdr.Type
	// case CaFormatEntry:
	// 	if hdr.Size != 64 {
	// 		return nil, desync.InvalidFormat{}
	// 	}
	// 	e := FormatEntry{FormatHeader: hdr}
	// 	e.FeatureFlags, err = d.r.ReadUint64()
	// 	if err != nil {
	// 		return nil, err
	// 	}

	default:
		return nil, nil
		//return nil, fmt.Errorf("unsupported header type %x", hdr.Type)
	}
}

// FormatEncoder takes casync format elements and encodes them into a stream.
type FrameEncoder struct {
	w writer
}

func NewFrameEncoder(w io.Writer) FrameEncoder {
	return FrameEncoder{w: writer{w}}
}

func (e *FrameEncoder) Encode(v interface{}) (int64, error) {
	panic("not implemented")
	// switch t := v.(type) {
	// case FormatEntry:
	// 	return e.w.WriteUint64(
	// 		t.Size,
	// 		t.Type,
	// 		t.FeatureFlags,
	// 		uint64(FilemodeToStatMode(t.Mode)),
	// 		t.Flags,
	// 		uint64(t.UID),
	// 		uint64(t.GID),
	// 		uint64(t.MTime.UnixNano()),
	// 	)

	// case FormatPayload:
	// 	n, err := e.w.WriteUint64(t.Size, t.Type)
	// 	if err != nil {
	// 		return n, err
	// 	}
	// 	n1, err := io.Copy(e.w, t.Data)
	// 	return n + n1, err

	// default:
	// 	return 0, fmt.Errorf("unsupported format element '%s'", reflect.TypeOf(v))
	// }
}
