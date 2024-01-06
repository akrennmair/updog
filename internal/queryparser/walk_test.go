package queryparser_test

import (
	"fmt"
	"testing"

	"github.com/akrennmair/updog/internal/queryparser"
	updogv1 "github.com/akrennmair/updog/proto/updog/v1"
	"github.com/stretchr/testify/require"
)

func TestWalk(t *testing.T) {
	q, err := queryparser.ParseQuery(`foo = "bar" | ( bar = "baz" & ^ baz = "quux" )`)
	require.NoError(t, err)

	var types []string

	queryparser.Walk(q, func(e *updogv1.Query_Expression) bool {
		types = append(types, fmt.Sprintf("%T", e.Value))
		return true
	})

	expectedTypes := []string{
		"*updogv1.Query_Expression_Or_",
		"*updogv1.Query_Expression_Eq",
		"*updogv1.Query_Expression_And_",
		"*updogv1.Query_Expression_Eq",
		"*updogv1.Query_Expression_Not_",
		"*updogv1.Query_Expression_Eq",
	}

	require.Equal(t, expectedTypes, types)
}

func TestReplacePlaceholders(t *testing.T) {
	q, err := queryparser.ParseQuery(`foo = $1 | ( bar = $2 & ^ baz = $3 )`)
	require.NoError(t, err)

	q2 := queryparser.ReplacePlaceholders(q, []string{"1", "2", "3"})

	require.Equal(t, `foo = "1" | ( bar = "2" & ^ baz = "3" )`, queryparser.QueryToString(q2))
}
