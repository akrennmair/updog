package queryparser

import (
	updogv1 "github.com/akrennmair/updog/proto/updog/v1"
	"google.golang.org/protobuf/proto"
)

func Walk(query *updogv1.Query, f func(e *updogv1.Query_Expression) bool) bool {
	return walk(query.Expr, f)
}

func walk(e *updogv1.Query_Expression, f func(e *updogv1.Query_Expression) bool) bool {
	if !f(e) {
		return false
	}

	switch v := e.Value.(type) {
	case *updogv1.Query_Expression_And_:
		for _, ee := range v.And.Exprs {
			if !walk(ee, f) {
				return false
			}
		}
	case *updogv1.Query_Expression_Or_:
		for _, ee := range v.Or.Exprs {
			if !walk(ee, f) {
				return false
			}
		}
	case *updogv1.Query_Expression_Not_:
		if !walk(v.Not.Expr, f) {
			return false
		}
	case *updogv1.Query_Expression_Eq:
		// nothing
	}

	return true
}

func ReplacePlaceholders(query *updogv1.Query, values []string) *updogv1.Query {
	q := proto.Clone(query).(*updogv1.Query)

	_ = Walk(q, func(e *updogv1.Query_Expression) bool {
		if v, ok := e.Value.(*updogv1.Query_Expression_Eq); ok {
			if v.Eq.Placeholder > 0 {
				v.Eq.Value = values[v.Eq.Placeholder-1]
				v.Eq.Placeholder = 0
			}
		}
		return true
	})

	return q
}
