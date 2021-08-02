package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/spf13/cobra"
	"github.com/tomberek/zchunk"
)

type fetchOptions struct {
	chunkSize  string
	printStats bool
}

func newFetchCommand(ctx context.Context) *cobra.Command {
	var opt fetchOptions

	cmd := &cobra.Command{
		Use:     "fetch <remote-file> <file>",
		Short:   "Fetch remote file",
		Long:    `Creates chunks from the input file and builds an index along with compressing.`,
		Example: `  zchunk fetch http://file.zst largefile.bin`,
		Args:    cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runFetch(ctx, opt, args)
		},
		SilenceUsage: true,
	}
	flags := cmd.Flags()
	flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "128:512:2048", "min:avg:max chunk size in kb")
	// 8 * casync
	//flags.StringVarP(&opt.chunkSize, "chunk-size", "m", "16:64:256", "min:avg:max chunk size in kb")
	return cmd
}

func runFetch(ctx context.Context, opt fetchOptions, args []string) error {
	min, avg, max, err := parseChunkSizeParam(opt.chunkSize)
	if err != nil {
		return err
	}
	_, _, _ = min, avg, max

	remoteFile := args[0]
	dataFile := args[1]

	loc, err := url.Parse(remoteFile)
	if err != nil {
		return fmt.Errorf("Unable to parse store location %s : %s", remoteFile, err)
	}

	return zchunk.Fetch(loc, dataFile)
}
