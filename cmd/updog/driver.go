package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/akrennmair/updog/driver"
	"github.com/olekukonko/tablewriter"
)

type driverConfig struct {
	dsn string
}

func driverCmd(cfg *driverConfig, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("at least one query required")
	}

	db, err := sql.Open("updog", cfg.dsn)
	if err != nil {
		return fmt.Errorf("connecting to updog using data source name %q failed: %w", cfg.dsn, err)
	}
	defer db.Close()

	for idx, query := range args {
		rows, err := db.Query(query)
		if err != nil {
			return fmt.Errorf("failed to run query %q: %w", query, err)
		}

		if err := printRows(idx+1, rows); err != nil {
			return fmt.Errorf("failed to show data for query %d: %w", idx+1, err)
		}
	}

	return nil
}

func printRows(idx int, rows *sql.Rows) error {
	var (
		data   [][]string
		header []string
	)

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	for _, col := range colTypes {
		header = append(header, col.Name())
	}

	for rows.Next() {
		row := []interface{}{}

		for _, col := range colTypes {
			switch col.DatabaseTypeName() {
			case "TEXT":
				row = append(row, new(string))
			case "BIGINT":
				row = append(row, new(int64))
			default:
				return fmt.Errorf("unsupported column type %q", col.DatabaseTypeName())
			}
		}

		if err := rows.Scan(row...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		var rowData []string

		for idx, col := range colTypes {
			switch col.DatabaseTypeName() {
			case "TEXT":
				rowData = append(rowData, *(row[idx].(*string)))
			case "BIGINT":
				rowData = append(rowData, fmt.Sprint(*(row[idx].(*int64))))
			}
		}

		data = append(data, rowData)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)
	table.AppendBulk(data)

	fmt.Printf("Result for query %d:\n", idx)

	table.Render()

	fmt.Println("")

	return nil
}
