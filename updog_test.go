package updog

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestCreate(t *testing.T) {
	idx := NewIndexWriter()

	idx.AddRow(map[string]string{"a": "1", "b": "2", "c": "3"})
	idx.AddRow(map[string]string{"a": "1", "b": "3", "c": "4"})
	idx.AddRow(map[string]string{"a": "1", "b": "3", "c": "4"})
	idx.AddRow(map[string]string{"a": "2"})

	require.NotNil(t, idx.schema.Columns["a"])
	require.NotNil(t, idx.schema.Columns["b"])
	require.NotNil(t, idx.schema.Columns["c"])
	require.Equal(t, 3, len(idx.schema.Columns))

	require.Equal(t, uint64(3), idx.getValueBitmap(getValueIndex("a", "1")).GetCardinality())
	require.Equal(t, uint64(2), idx.getValueBitmap(getValueIndex("b", "3")).GetCardinality())
	require.Equal(t, uint64(1), idx.getValueBitmap(getValueIndex("a", "2")).GetCardinality())
	require.Equal(t, uint64(1), idx.getValueBitmap(getValueIndex("c", "3")).GetCardinality())
	require.Equal(t, uint64(2), idx.getValueBitmap(getValueIndex("c", "4")).GetCardinality())
	require.Equal(t, uint64(0), idx.getValueBitmap(getValueIndex("a", "3")).GetCardinality())

	testFile, err := os.CreateTemp(os.TempDir(), "updog_test_")
	require.NoError(t, err)

	db, err := bbolt.Open("", 0644, &bbolt.Options{
		OpenFile: func(string, int, fs.FileMode) (*os.File, error) {
			return testFile, nil
		},
	})
	require.NoError(t, err)

	require.NoError(t, idx.WriteToBoltDatabase(db))

	newIdx, err := OpenIndexFromBoltDatabase(db)
	require.NoError(t, err)
	require.NotNil(t, newIdx)

	require.NotNil(t, newIdx.schema.Columns["a"])
	require.NotNil(t, newIdx.schema.Columns["b"])
	require.NotNil(t, newIdx.schema.Columns["c"])
	require.Equal(t, 3, len(newIdx.schema.Columns))

	testData := []struct {
		Expr          Expression
		ExpectedCount uint64
	}{
		{
			Expr:          &ExprEqual{Column: "a", Value: "1"},
			ExpectedCount: 3,
		},
		{
			Expr:          &ExprEqual{Column: "b", Value: "3"},
			ExpectedCount: 2,
		},
		{
			Expr:          &ExprEqual{Column: "a", Value: "2"},
			ExpectedCount: 1,
		},
		{
			Expr:          &ExprEqual{Column: "c", Value: "3"},
			ExpectedCount: 1,
		},
		{
			Expr:          &ExprEqual{Column: "c", Value: "4"},
			ExpectedCount: 2,
		},
		{
			Expr:          &ExprEqual{Column: "a", Value: "3"},
			ExpectedCount: 0,
		},
	}

	for _, tt := range testData {
		t.Run(tt.Expr.String(), func(t *testing.T) {
			q := &Query{
				Expr: tt.Expr,
			}
			result, err := q.Execute(newIdx)
			require.NoError(t, err)
			require.Equal(t, tt.ExpectedCount, result.Count)
		})
	}
}
