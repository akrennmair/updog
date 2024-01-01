package updog_test

import (
	"os"
	"testing"

	"github.com/akrennmair/updog"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestBigWriter(t *testing.T) {
	f, err := os.CreateTemp("", "updog_test_*")
	require.NoError(t, err)
	tf, err := os.CreateTemp("", "updog_tmp_*")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	defer os.Remove(tf.Name())

	db, err := bbolt.Open(f.Name(), 0600, nil)
	require.NoError(t, err)

	tempDB, err := bbolt.Open(tf.Name(), 0600, nil)
	require.NoError(t, err)

	idx, err := updog.NewBigIndexWriter(db, tempDB)
	require.NoError(t, err)

	require.NoError(t, tempDB.Update(func(tx *bbolt.Tx) error {
		_, err := idx.AddRow(tx, map[string]string{"a": "1", "b": "2"})
		return err
	}))

	require.NoError(t, idx.Flush())

	newIdx, err := updog.OpenIndexFromBoltDatabase(db)
	require.NoError(t, err)

	schema := newIdx.GetSchema()

	require.Equal(t, &updog.Schema{
		Columns: []updog.SchemaColumn{
			{
				Name: "a",
				Values: []updog.SchemaColumnValue{
					{
						Value: "1",
					},
				},
			},
			{
				Name: "b",
				Values: []updog.SchemaColumnValue{
					{
						Value: "2",
					},
				},
			},
		},
	}, schema)

	result, err := newIdx.Execute(&updog.Query{
		Expr: &updog.ExprEqual{
			Column: "a",
			Value:  "1",
		},
		GroupBy: []string{"b"},
	})
	require.NoError(t, err)
	require.Equal(t, &updog.Result{
		Count: 1,
		Groups: []updog.ResultGroup{
			{
				Fields: []updog.ResultField{
					{
						Column: "b",
						Value:  "2",
					},
				},
				Count: 1,
			},
		},
	}, result)

	result2, err := newIdx.Execute(&updog.Query{
		Expr: &updog.ExprEqual{
			Column: "b",
			Value:  "2",
		},
	})
	require.NoError(t, err)
	require.Equal(t, &updog.Result{
		Count: 1,
	}, result2)

}
