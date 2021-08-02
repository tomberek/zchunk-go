package zchunk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/folbricht/desync"
	log "github.com/sirupsen/logrus"
)

func Fetch(loc *url.URL, dataFile string) error {
	remote, err := NewRemoteHTTPStoreBase(loc, desync.StoreOptions{})
	if err != nil {
		return err
	}
	_, b, err := remote.IssueRetryableHttpRequest("GET", loc, "bytes= -9", func() io.Reader { return nil })
	if err != nil {
		return err
	}
	var footer Footer
	err = binary.Read(bytes.NewReader(b), binary.LittleEndian, &footer)
	if err != nil {
		return err
	}

	log.Debugf("footer: %+v\n", footer)

	size := (4 + 4) + footer.Number*(4+4+32) + (4 + 1 + 4)
	rangeHeader := fmt.Sprintf("bytes= -%d", size)
	_, b, err = remote.IssueRetryableHttpRequest("GET", loc, rangeHeader, func() io.Reader { return nil })
	if err != nil {
		return err
	}
	reader := reader{bytes.NewReader(b)}
	magic, err := reader.ReadUint32()
	if err != nil {
		return err
	}
	if magic != FrameMagic {
		return fmt.Errorf("Skippable_Magic_Number not found")
	}
	frameSize, err := reader.ReadUint32()
	if err != nil {
		return err
	}
	if frameSize != size-8 {
		return fmt.Errorf("Frame_size does not match values in header/footer")
	}

	fd, err := os.Create(dataFile)
	if err != nil {
		return err
	}
	defer fd.Close()

	// Now parse the Index
	var offset uint32 = 0
	for i := 0; i < int(footer.Number); i++ {
		entry, err := reader.ReadEntry()
		if err != nil {
			return err
		}
		log.Printf("%+v\n", entry)

		rangeHeader = fmt.Sprintf("bytes= %d-%d", offset, entry.CompressedSize)
		_, b, err = remote.IssueRetryableHttpRequest("GET", loc, rangeHeader, func() io.Reader { return nil })
		if err != nil {
			return err
		}
		log.Debug("zstd header: %+x\n", b[0:8])
		res, err := desync.Decompress(nil, b)
		if err != nil {
			return err
		}
		log.Printf("%+s\n", res)
		_, err = fd.Write(res)
		if err != nil {
			return err
		}

		offset += entry.CompressedSize
	}

	return nil
}
