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
	flag.StringVar(&outputFile, "output", "out.updog", "output directory")

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

	idx := updog.NewIndexWriter()

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

	fi, err := os.Stat(outputFile)
	if err != nil {
		if err := os.MkdirAll(outputFile, 0755); err != nil {
			log.Fatalf("failed to create %s: %v", outputFile, err)
		}
	} else {
		if !fi.IsDir() {
			log.Fatalf("error: %s is not a directory", outputFile)
		}
	}

	if err := idx.WriteToDirectory(outputFile); err != nil {
		log.Fatalf("Failed to write output to %s: %v", outputFile, err)
	}
}
