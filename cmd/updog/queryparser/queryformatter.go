package queryparser

import (
	"fmt"
	"strings"

	proto "github.com/akrennmair/updog/proto/updog/v1"
)

func QueryToString(q *proto.Query) string {
	var b strings.Builder

	exprToString(&b, q.Expr)

	if len(q.GroupBy) > 0 {
		fmt.Fprintf(&b, " ; %s", strings.Join(q.GroupBy, ", "))
	}

	return b.String()
}

func exprToString(b *strings.Builder, expr *proto.Query_Expression) {
	switch v := expr.Value.(type) {
	case *proto.Query_Expression_Eq:
		equalExprToString(b, v.Eq)
	case *proto.Query_Expression_Not_:
		notExprToString(b, v.Not)
	case *proto.Query_Expression_And_:
		andExprToString(b, v.And)
	case *proto.Query_Expression_Or_:
		orExprToString(b, v.Or)
	}
}

func equalExprToString(b *strings.Builder, expr *proto.Query_Expression_Equal) {
	if expr.Placeholder > 0 {
		fmt.Fprintf(b, "%s = $%d", expr.Column, expr.Placeholder)
		return
	}
	fmt.Fprintf(b, "%s = %s", expr.Column, formatString(expr.Value))
}

func formatString(s string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(s, `"`, `""`))
}

func notExprToString(b *strings.Builder, expr *proto.Query_Expression_Not) {
	b.WriteString("^ ")

	requiresParens := expr.Expr.GetAnd() != nil || expr.Expr.GetOr() != nil

	if requiresParens {
		b.WriteString("( ")
	}

	exprToString(b, expr.Expr)

	if requiresParens {
		b.WriteString(" )")
	}
}

func andExprToString(b *strings.Builder, expr *proto.Query_Expression_And) {
	for idx, expr := range expr.Exprs {
		if idx > 0 {
			fmt.Fprintf(b, " & ")
		}

		requiresParens := expr.GetOr() != nil

		if requiresParens {
			b.WriteString("( ")
		}

		exprToString(b, expr)

		if requiresParens {
			b.WriteString(" )")
		}
	}
}

func orExprToString(b *strings.Builder, expr *proto.Query_Expression_Or) {
	for idx, expr := range expr.Exprs {
		if idx > 0 {
			fmt.Fprintf(b, " | ")
		}

		requiresParens := expr.GetAnd() != nil

		if requiresParens {
			b.WriteString("( ")
		}

		exprToString(b, expr)

		if requiresParens {
			b.WriteString(" )")
		}
	}
}
