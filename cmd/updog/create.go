package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/akrennmair/updog"
)

type createConfig struct {
	outputFile string
	inputFile  string
}

func createCmd(cfg *createConfig) error {
	f, err := os.Open(cfg.inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)

	header, err := r.Read()
	if err != nil {
		return fmt.Errorf("failed to read input file header: %w", err)
	}

	header = normalizeHeader(header)

	idx := updog.NewIndexWriter()

	for {
		record, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("failed to read record: %w", err)
		}

		values := map[string]string{}

		for idx, v := range record {
			k := header[idx]
			values[k] = v
		}

		idx.AddRow(values)
	}

	if err := idx.WriteToFile(cfg.outputFile); err != nil {
		return fmt.Errorf("failed to write output to %s: %w", cfg.outputFile, err)
	}

	return nil
}

func normalizeHeader(header []string) []string {
	newHeader := make([]string, 0, len(header))

	for _, hdr := range header {
		hdr = strings.ToLower(hdr)
		hdr = strings.Map(func(r rune) rune {
			if r == ' ' {
				return '_'
			}
			if r >= 'a' && r <= 'z' {
				return r
			}

			return '_'
		}, hdr)

		newHeader = append(newHeader, hdr)
	}

	return newHeader
}
