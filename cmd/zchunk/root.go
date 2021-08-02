package main

import (
	"github.com/spf13/cobra"
)

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "zchunk",
		Short: "A file format designed for highly efficient deltas while maintaining good compression.",
	}
	// cmd.PersistentFlags().StringVar(&digestAlgorithm, "digest", "sha512-256", "digest algorithm, sha512-256 or sha256")
	cmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "verbose mode")
	return cmd
}
