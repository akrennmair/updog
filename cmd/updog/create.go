package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/akrennmair/updog"
	"github.com/akrennmair/updog/internal/openfile"
	"go.etcd.io/bbolt"
)

type createConfig struct {
	outputFile string
	inputFile  string
	big        bool
}

type indexWriter interface {
	AddRow(values map[string]string) (uint32, error)
	Flush() error
}

func createCmd(globalCfg *globalConfig, cfg *createConfig) error {
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

	var iw indexWriter

	if cfg.big {
		tempFile, err := os.CreateTemp("", "updog_*.tmp")
		if err != nil {
			return fmt.Errorf("failed to create temporary file: %w", err)
		}
		tempFile.Close()
		defer os.Remove(tempFile.Name())

		tempDB, err := bbolt.Open(tempFile.Name(), 0600, &bbolt.Options{OpenFile: openfile.OpenFile(openfile.Options{FailIfFileDoesntExist: true})})
		if err != nil {
			return fmt.Errorf("failed to create temporary database: %w", err)
		}
		defer tempDB.Close()

		db, err := bbolt.Open(cfg.outputFile, 0644, &bbolt.Options{OpenFile: openfile.OpenFile(openfile.Options{FailIfFileExists: true})})
		if err != nil {
			return fmt.Errorf("failed to open output file: %w", err)
		}
		defer db.Close()

		idx, err := updog.NewBigIndexWriter(db, tempDB)
		if err != nil {
			return fmt.Errorf("failed to create big index writer: %w", err)
		}

		iw = idx
	} else {
		idx := updog.NewIndexWriter(cfg.outputFile)
		iw = idx
	}

	idx := 0

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

		if _, err := iw.AddRow(values); err != nil {
			return fmt.Errorf("failed to add row: %w", err)
		}

		idx++

		if globalCfg.verbose && idx%10000 == 0 {
			fmt.Printf("Added %d rows...\n", idx)
		}
	}

	if globalCfg.verbose {
		fmt.Printf("Flushing data...\n")
	}

	if err := iw.Flush(); err != nil {
		return fmt.Errorf("failed to flush big index writer: %w", err)
	}

	if globalCfg.verbose {
		fmt.Printf("Flushing done")
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
