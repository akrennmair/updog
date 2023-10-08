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
