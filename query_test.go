package updog

import (
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestQuery(t *testing.T) {
	idxWriter := NewIndexWriter()

	idxWriter.AddRow(map[string]string{"a": "1", "b": "2", "c": "3"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "2", "c": "3"})
	idxWriter.AddRow(map[string]string{"a": "3", "b": "4", "c": "5"})

	testFile, err := os.CreateTemp(os.TempDir(), "updog_test_")
	require.NoError(t, err)

	db, err := bbolt.Open("", 0644, &bbolt.Options{
		OpenFile: func(string, int, fs.FileMode) (*os.File, error) {
			return testFile, nil
		},
	})
	require.NoError(t, err)

	require.NoError(t, idxWriter.WriteToBoltDatabase(db))

	testData := []struct {
		name           string
		query          *Query
		expectedResult uint64
	}{
		{
			name: "a=1 should return 1",
			query: &Query{
				Expr: &ExprEqual{
					Column: "a",
					Value:  "1",
				},
			},
			expectedResult: 1,
		},
		{
			name: "a=xxx should return 0",
			query: &Query{
				Expr: &ExprEqual{
					Column: "a",
					Value:  "xxx",
				},
			},
			expectedResult: 0,
		},
		{
			name: "a=1 or a=2 should return 2",
			query: &Query{
				Expr: &ExprOr{
					Exprs: []Expression{
						&ExprEqual{
							Column: "a",
							Value:  "1",
						},
						&ExprEqual{
							Column: "a",
							Value:  "2",
						},
					},
				},
			},
			expectedResult: 2,
		},
		{
			name: "b=2 and c=3 should return 2",
			query: &Query{
				Expr: &ExprAnd{
					Exprs: []Expression{
						&ExprEqual{
							Column: "b",
							Value:  "2",
						},
						&ExprEqual{
							Column: "c",
							Value:  "3",
						},
					},
				},
			},
			expectedResult: 2,
		},
		{
			name: "a=1 and a=2 should return 0",
			query: &Query{
				Expr: &ExprAnd{
					Exprs: []Expression{
						&ExprEqual{
							Column: "a",
							Value:  "1",
						},
						&ExprEqual{
							Column: "a",
							Value:  "2",
						},
					},
				},
			},
			expectedResult: 0,
		},
		{
			name: "not (a=1 or a=2) should return 1",
			query: &Query{
				Expr: &ExprNot{
					Expr: &ExprOr{
						Exprs: []Expression{
							&ExprEqual{
								Column: "a",
								Value:  "1",
							},
							&ExprEqual{
								Column: "a",
								Value:  "2",
							},
						},
					},
				},
			},
			expectedResult: 1,
		},
	}

	runTests := func(t *testing.T, idx *Index) {
		for _, tt := range testData {
			t.Run(tt.name, func(t *testing.T) {
				t.Logf("query = %s", tt.query.Expr.String())
				result, err := tt.query.Execute(idx)
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, tt.expectedResult, result.Count)
			})
		}
	}

	t.Run("normal", func(t *testing.T) {
		idx, err := OpenIndexFromBoltDatabase(db)
		require.NoError(t, err)
		runTests(t, idx)
	})

	t.Run("preloaded", func(t *testing.T) {
		idx, err := OpenIndexFromBoltDatabase(db, WithPreloadedData())
		require.NoError(t, err)
		runTests(t, idx)
	})

	t.Run("lrucache", func(t *testing.T) {
		idx, err := OpenIndexFromBoltDatabase(db, WithLRUCache(100*1024*1024))
		require.NoError(t, err)
		runTests(t, idx)
	})

	t.Run("lrucache_smallcache", func(t *testing.T) {
		idx, err := OpenIndexFromBoltDatabase(db, WithLRUCache(100))
		require.NoError(t, err)
		runTests(t, idx)
	})
}

func TestQueryGroupBy(t *testing.T) {
	idxWriter := NewIndexWriter()

	idxWriter.AddRow(map[string]string{"a": "1", "b": "2", "c": "3", "x": "true"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "2", "c": "3", "x": "true"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "5", "c": "3", "x": "false"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "6", "c": "8", "x": "false"})
	idxWriter.AddRow(map[string]string{"a": "3", "b": "2", "c": "7", "x": "false"})
	idxWriter.AddRow(map[string]string{"a": "3", "b": "4", "c": "5"})

	testFile, err := os.CreateTemp(os.TempDir(), "updog_test_")
	require.NoError(t, err)

	db, err := bbolt.Open("", 0644, &bbolt.Options{
		OpenFile: func(string, int, fs.FileMode) (*os.File, error) {
			return testFile, nil
		},
	})
	require.NoError(t, err)

	require.NoError(t, idxWriter.WriteToBoltDatabase(db))

	idx, err := OpenIndexFromBoltDatabase(db)
	require.NoError(t, err)

	testData := []struct {
		name           string
		query          *Query
		expectedResult *Result
	}{
		{
			name: "a=2 group by x",
			query: &Query{
				Expr: &ExprEqual{
					Column: "a",
					Value:  "2",
				},
				GroupBy: []string{"x"},
			},
			expectedResult: &Result{
				Count: 3,
				Groups: []ResultGroup{
					{
						Fields: []ResultField{
							{
								Column: "x",
								Value:  "false",
							},
						},
						Count: 2,
					},
					{
						Fields: []ResultField{
							{
								Column: "x",
								Value:  "true",
							},
						},
						Count: 1,
					},
				},
			},
		},
		{
			name: "a=2 group by c",
			query: &Query{
				Expr: &ExprEqual{
					Column: "a",
					Value:  "2",
				},
				GroupBy: []string{"c"},
			},
			expectedResult: &Result{
				Count: 3,
				Groups: []ResultGroup{
					{
						Fields: []ResultField{
							{
								Column: "c",
								Value:  "3",
							},
						},
						Count: 2,
					},
					{
						Fields: []ResultField{
							{
								Column: "c",
								Value:  "8",
							},
						},
						Count: 1,
					},
				},
			},
		},
		{
			name: "a=yyy group by x",
			query: &Query{
				Expr: &ExprEqual{
					Column: "a",
					Value:  "yyy",
				},
				GroupBy: []string{"x"},
			},
			expectedResult: &Result{},
		},
		{
			name: "a=2 group by x, c",
			query: &Query{
				Expr: &ExprEqual{
					Column: "a",
					Value:  "2",
				},
				GroupBy: []string{"x", "c"},
			},
			expectedResult: &Result{
				Count: 3,
				Groups: []ResultGroup{
					{
						Fields: []ResultField{
							{
								Column: "x",
								Value:  "false",
							},
							{
								Column: "c",
								Value:  "3",
							},
						},
						Count: 1,
					},
					{
						Fields: []ResultField{
							{
								Column: "x",
								Value:  "false",
							},
							{
								Column: "c",
								Value:  "8",
							},
						},
						Count: 1,
					},
					{
						Fields: []ResultField{
							{
								Column: "x",
								Value:  "true",
							},
							{
								Column: "c",
								Value:  "3",
							},
						},
						Count: 1,
					},
				},
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.query.Execute(idx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

const x = 7324239828

func BenchmarkQuery(b *testing.B) {
	idxWriter := NewIndexWriter()

	getRandomRow := func() map[string]string {
		row := make(map[string]string)
		for i := 0; i < 1000; i++ {
			k := fmt.Sprintf("%4x", i)
			v := fmt.Sprintf("%8o", (x^i)^(x>>1))

			row[k] = v
		}

		row["is_cool"] = fmt.Sprint(rand.Int31() % 2)

		return row
	}

	for i := 0; i < 10_000; i++ {
		randomRow := getRandomRow()
		idxWriter.AddRow(randomRow)
	}

	testFile, err := os.CreateTemp(os.TempDir(), "updog_test_")
	require.NoError(b, err)

	db, err := bbolt.Open("", 0644, &bbolt.Options{
		OpenFile: func(string, int, fs.FileMode) (*os.File, error) {
			return testFile, nil
		},
	})
	require.NoError(b, err)

	require.NoError(b, idxWriter.WriteToBoltDatabase(db))

	idx, err := OpenIndexFromBoltDatabase(db)
	require.NoError(b, err)

	preloadedIdx, err := OpenIndexFromBoltDatabase(db, WithPreloadedData())
	require.NoError(b, err)

	idxWithLRUCache, err := OpenIndexFromBoltDatabase(db, WithLRUCache(100*1024*1024)) // 100 MiB cache
	require.NoError(b, err)

	preloadedIdxWithLRUCache, err := OpenIndexFromBoltDatabase(db, WithPreloadedData(), WithLRUCache(100*1024*1024))
	require.NoError(b, err)

	b.ResetTimer()

	b.Run("default", func(b *testing.B) {
		runBenchmarkQuery(b, idx)
	})

	b.Run("preloaded", func(b *testing.B) {
		runBenchmarkQuery(b, preloadedIdx)
	})

	b.Run("lrucache", func(b *testing.B) {
		runBenchmarkQuery(b, idxWithLRUCache)
	})

	b.Run("preloaded_lrucache", func(b *testing.B) {
		runBenchmarkQuery(b, preloadedIdxWithLRUCache)
	})
}

func runBenchmarkQuery(b *testing.B, idx *Index) {
	for i := 0; i < b.N; i++ {
		j := i % 1000
		k := fmt.Sprintf("%4x", (j))
		v := fmt.Sprintf("%8o", (x^j)^(x>>1))
		w := fmt.Sprintf("%8o", (x^((i+1)%1000))^(x>>1))

		q := &Query{
			Expr: &ExprOr{
				Exprs: []Expression{
					&ExprEqual{
						Column: k,
						Value:  v,
					},
					&ExprEqual{
						Column: k,
						Value:  w,
					},
				},
			},
			GroupBy: []string{"is_cool"},
		}

		_, err := q.Execute(idx)
		require.NoError(b, err)
	}
}
