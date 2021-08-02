package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/folbricht/desync"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/tomberek/zchunk"
)

type makeOptions struct {
	chunkSize  string
	printStats bool
}

func newMakeCommand(ctx context.Context) *cobra.Command {
	var opt makeOptions

	cmd := &cobra.Command{
		Use:     "make <index> <file>",
		Short:   "Chunk input file and create index",
		Long:    `Creates chunks from the input file and builds an index along with compressing.`,
		Example: `  zchunk make -m /path/to/local file.zstd largefile.bin`,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMake(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "128:512:2048", "min:avg:max chunk size in kb")
	// 8 * casync
	//flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "16:64:256", "min:avg:max chunk size in kb")
	return cmd
}

func runMake(ctx context.Context, opt makeOptions, args []string) error {
	min, avg, max, err := parseChunkSizeParam(opt.chunkSize)
	if err != nil {
		return err
	}

	indexFile := args[0]
	dataFile := args[1]

	var s zchunk.ZstdStore
	s, err = zchunk.NewZstdStore(indexFile)
	if err != nil {
		return err
	}
	defer s.Close()
	var f *os.File
	if dataFile == "-" {
		f = os.Stdin
	} else {
		f, err = os.Open(dataFile)
		if err != nil {
			return err
		}
	}

	chunker, err := desync.NewChunker(f, min, avg, max)
	if err != nil {
		return err
	}

	// TODO: use all cores or expose arg
	if err := zchunk.ChunkStream(ctx, chunker, s, 10); err != nil {
		return err
	}
	return nil
}

func parseChunkSizeParam(s string) (min, avg, max uint64, err error) {
	sizes := strings.Split(s, ":")
	if len(sizes) != 3 {
		return 0, 0, 0, fmt.Errorf("invalid chunk size '%s'", s)
	}
	num, err := strconv.Atoi(sizes[0])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "min chunk size")
	}
	min = uint64(num) * 1024
	num, err = strconv.Atoi(sizes[1])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "avg chunk size")
	}
	avg = uint64(num) * 1024
	num, err = strconv.Atoi(sizes[2])
	if err != nil {
		return 0, 0, 0, errors.Wrap(err, "max chunk size")
	}
	max = uint64(num) * 1024
	return
}
