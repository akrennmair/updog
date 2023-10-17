package updog

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	idxWriter := NewIndexWriter()

	idxWriter.AddRow(map[string]string{"a": "1", "b": "2", "c": "3"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "2", "c": "3"})
	idxWriter.AddRow(map[string]string{"a": "3", "b": "4", "c": "5"})

	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)

	require.NoError(t, idxWriter.WriteToBadgerDatabase(db))

	idx, err := OpenIndexFromBadgerDatabase(db)
	require.NoError(t, err)

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

	for _, tt := range testData {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.query.Execute(idx)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tt.expectedResult, result.Count)
		})
	}
}

func TestQueryGroupBy(t *testing.T) {
	idxWriter := NewIndexWriter()

	idxWriter.AddRow(map[string]string{"a": "1", "b": "2", "c": "3", "x": "true"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "2", "c": "3", "x": "true"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "5", "c": "3", "x": "false"})
	idxWriter.AddRow(map[string]string{"a": "2", "b": "6", "c": "8", "x": "false"})
	idxWriter.AddRow(map[string]string{"a": "3", "b": "2", "c": "7", "x": "false"})
	idxWriter.AddRow(map[string]string{"a": "3", "b": "4", "c": "5"})

	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(t, err)

	require.NoError(t, idxWriter.WriteToBadgerDatabase(db))

	idx, err := OpenIndexFromBadgerDatabase(db)
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

	for i := 0; i < 100_000; i++ {
		randomRow := getRandomRow()
		idxWriter.AddRow(randomRow)
	}

	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	require.NoError(b, err)

	require.NoError(b, idxWriter.WriteToBadgerDatabase(db))

	idx, err := OpenIndexFromBadgerDatabase(db)
	require.NoError(b, err)

	preloadedIdx, err := OpenIndexFromBadgerDatabase(db, WithPreloadedData())
	require.NoError(b, err)

	b.ResetTimer()

	b.Run("default", func(b *testing.B) {
		runBenchmarkQuery(b, idx)
	})

	b.Run("preloaded", func(b *testing.B) {
		runBenchmarkQuery(b, preloadedIdx)
	})
}

func runBenchmarkQuery(b *testing.B, idx *Index) {
	for i := 0; i < b.N; i++ {
		j := i % 1000
		k := fmt.Sprintf("%4x", (j))
		v := fmt.Sprintf("%8o", (x^j)^(x>>1))

		q := &Query{
			Expr: &ExprEqual{
				Column: k,
				Value:  v,
			},
			GroupBy: []string{"is_cool"},
		}

		_, err := q.Execute(idx)
		require.NoError(b, err)
	}
}

/*
func BenchmarkQueryCustomers(b *testing.B) {
	f, err := os.Open("testdata/out.updog")

	if err != nil {
		b.Skipf("failed to open test file")
	}

	idx, err := NewIndexFromReader(f)
	require.NoError(b, err)

	countries := []string{
		"Eritrea",
		"Germany",
		"American Samoa",
		"Mozambique",
		"Panama",
		"Mauritania",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {

		q := &Query{
			Expr: &ExprOr{
				Exprs: []Expression{
					&ExprEqual{
						Column: "Country",
						Value:  countries[i%len(countries)],
					},
					&ExprEqual{
						Column: "Country",
						Value:  countries[(i*(i-1))%len(countries)],
					},
				},
			},
		}

		_, err := q.Execute(idx)
		require.NoError(b, err)
	}
}
*/
