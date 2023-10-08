package updog

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	idx := NewIndex()

	idx.AddRow(map[string]string{"a": "1", "b": "2", "c": "3"})
	idx.AddRow(map[string]string{"a": "2", "b": "2", "c": "3"})
	idx.AddRow(map[string]string{"a": "3", "b": "4", "c": "5"})

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
					Left: &ExprEqual{
						Column: "a",
						Value:  "1",
					},
					Right: &ExprEqual{
						Column: "a",
						Value:  "2",
					},
				},
			},
			expectedResult: 2,
		},
		{
			name: "b=2 and c=3 should return 2",
			query: &Query{
				Expr: &ExprAnd{
					Left: &ExprEqual{
						Column: "b",
						Value:  "2",
					},
					Right: &ExprEqual{
						Column: "c",
						Value:  "3",
					},
				},
			},
			expectedResult: 2,
		},
		{
			name: "a=1 and a=2 should return 0",
			query: &Query{
				Expr: &ExprAnd{
					Left: &ExprEqual{
						Column: "a",
						Value:  "1",
					},
					Right: &ExprEqual{
						Column: "a",
						Value:  "2",
					},
				},
			},
			expectedResult: 0,
		},
		{
			name: "not (a=1 or a=2) should return 1",
			query: &Query{
				Expr: &ExprNot{
					Expression: &ExprOr{
						Left: &ExprEqual{
							Column: "a",
							Value:  "1",
						},
						Right: &ExprEqual{
							Column: "a",
							Value:  "2",
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
	idx := NewIndex()

	idx.AddRow(map[string]string{"a": "1", "b": "2", "c": "3", "x": "true"})
	idx.AddRow(map[string]string{"a": "2", "b": "2", "c": "3", "x": "true"})
	idx.AddRow(map[string]string{"a": "2", "b": "5", "c": "3", "x": "false"})
	idx.AddRow(map[string]string{"a": "2", "b": "6", "c": "8", "x": "false"})
	idx.AddRow(map[string]string{"a": "3", "b": "2", "c": "7", "x": "false"})
	idx.AddRow(map[string]string{"a": "3", "b": "4", "c": "5"})

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
