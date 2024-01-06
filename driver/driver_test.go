package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/akrennmair/updog"
	"github.com/stretchr/testify/require"
)

func TestDriverQuery(t *testing.T) {
	filename := fmt.Sprintf("driver_test_%x.updog", rand.Int31())
	defer os.Remove(filename)

	writer := updog.NewIndexWriter(filename)

	testData := []map[string]string{
		{"a": "1", "b": "2", "c": "foo"},
		{"a": "1", "b": "3", "c": "bar"},
		{"a": "5", "b": "2", "c": "foo"},
		{"c": "quux"},
	}

	for _, row := range testData {
		_, err := writer.AddRow(row)
		require.NoError(t, err)
	}

	require.NoError(t, writer.Flush())

	db, err := sql.Open("updog", "file:"+filename+"?preload=true&lrucache=true&lrucachesize=10000000")
	require.NoError(t, err)

	rows, err := db.Query(`a = $1 | b = $2 ; c`, "1", "2")
	require.NoError(t, err)

	var cValues []string
	var counts []int64

	for rows.Next() {
		var (
			c     string
			count int64
		)

		require.NoError(t, rows.Scan(&c, &count))

		cValues = append(cValues, c)
		counts = append(counts, count)
	}
	require.NoError(t, rows.Close())

	require.Equal(t, []string{"bar", "foo"}, cValues)
	require.Equal(t, []int64{1, 2}, counts)

	require.NoError(t, db.Close())
}

func TestDriverPrepare(t *testing.T) {
	ctx := context.Background()

	filename := fmt.Sprintf("driver_test_%x.updog", rand.Int31())
	defer os.Remove(filename)

	writer := updog.NewIndexWriter(filename)

	testData := []map[string]string{
		{"a": "1", "b": "2", "c": "foo"},
		{"a": "1", "b": "3", "c": "bar"},
		{"a": "5", "b": "2", "c": "foo"},
		{"c": "quux"},
	}

	for _, row := range testData {
		_, err := writer.AddRow(row)
		require.NoError(t, err)
	}

	require.NoError(t, writer.Flush())

	db, err := sql.Open("updog", "file:"+filename+"?preload=true&lrucache=true&lrucachesize=10000000")
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	stmt, err := tx.PrepareContext(ctx, `a = $1 | b = $2 ; c`)
	require.NoError(t, err)

	rows, err := stmt.Query("1", "2")
	require.NoError(t, err)

	var cValues []string
	var counts []int64

	colTypes, err := rows.ColumnTypes()
	require.NoError(t, err)

	require.Equal(t, 2, len(colTypes))

	require.Equal(t, "c", colTypes[0].Name())
	require.Equal(t, "TEXT", colTypes[0].DatabaseTypeName())
	require.Equal(t, "count", colTypes[1].Name())
	require.Equal(t, "BIGINT", colTypes[1].DatabaseTypeName())

	for rows.Next() {
		var (
			c     string
			count int64
		)

		require.NoError(t, rows.Scan(&c, &count))

		cValues = append(cValues, c)
		counts = append(counts, count)
	}
	require.NoError(t, rows.Close())

	require.Equal(t, []string{"bar", "foo"}, cValues)
	require.Equal(t, []int64{1, 2}, counts)

	require.NoError(t, tx.Rollback())

	require.NoError(t, db.Close())
}
