package main

import (
	"fmt"

	"github.com/akrennmair/updog"
	"github.com/fraugster/cli"
)

type schemaConfig struct {
	indexFile string
	full      bool
}

type schemaRecord struct {
	Column string `table:"COLUMN"`
	Values int    `table:"UNIQUE VALUES"`
}

type fullSchemaRecord struct {
	Column string `table:"COLUMN"`
	Value  string `table:"VALUE"`
}

func schemaCmd(schemaCfg *schemaConfig) error {
	idx, err := updog.OpenIndex(schemaCfg.indexFile)
	if err != nil {
		return fmt.Errorf("failed to open index file: %w", err)
	}

	schema := idx.GetSchema()

	if schemaCfg.full {
		var fullTable []fullSchemaRecord

		for _, col := range schema.Columns {
			for _, v := range col.Values {
				fullTable = append(fullTable, fullSchemaRecord{Column: col.Name, Value: v.Value})
			}
		}

		return cli.Print("table", fullTable)
	}

	var table []schemaRecord

	for _, col := range schema.Columns {
		table = append(table, schemaRecord{Column: col.Name, Values: len(col.Values)})
	}

	return cli.Print("table", table)
}
