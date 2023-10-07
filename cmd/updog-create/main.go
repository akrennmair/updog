package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"io"
	"log"
	"os"

	"github.com/akrennmair/updog"
)

func main() {
	var (
		inputFile  string
		outputFile string
	)

	flag.StringVar(&inputFile, "input", "", "input file (CSV)")
	flag.StringVar(&outputFile, "output", "out.updog", "output file")

	flag.Parse()

	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("Failed to open input file: %v", err)
	}
	defer f.Close()

	r := csv.NewReader(f)

	header, err := r.Read()
	if err != nil {
		log.Fatalf("failed to read input file header: %v", err)
	}

	idx := updog.NewIndex()

	for {
		record, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatalf("failed to read record: %v", err)
		}

		values := map[string]string{}

		for idx, v := range record {
			k := header[idx]
			values[k] = v
		}

		idx.AddRow(values)
	}

	wf, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("failed to open output file: %v", err)
	}

	if err := idx.WriteTo(wf); err != nil {
		log.Fatalf("failed to write index to file: %v", err)
	}

	if err := wf.Close(); err != nil {
		log.Fatalf("failed to close output file: %v", err)
	}
}
