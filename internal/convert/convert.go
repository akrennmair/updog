package convert

import (
	"github.com/akrennmair/updog"
	proto "github.com/akrennmair/updog/proto/updog/v1"
)

func ToQuery(pbq *proto.Query) *updog.Query {
	return &updog.Query{
		Expr:    toExpr(pbq.Expr),
		GroupBy: pbq.GroupBy,
	}
}

func toExpr(pbe *proto.Query_Expression) updog.Expression {
	switch v := pbe.Value.(type) {
	case *proto.Query_Expression_Eq:
		return &updog.ExprEqual{
			Column: v.Eq.Column,
			Value:  v.Eq.Value,
		}
	case *proto.Query_Expression_Not_:
		return &updog.ExprNot{
			Expr: toExpr(v.Not.Expr),
		}
	case *proto.Query_Expression_And_:
		e := &updog.ExprAnd{}
		for _, ee := range v.And.Exprs {
			e.Exprs = append(e.Exprs, toExpr(ee))
		}
		return e
	case *proto.Query_Expression_Or_:
		e := &updog.ExprOr{}
		for _, ee := range v.Or.Exprs {
			e.Exprs = append(e.Exprs, toExpr(ee))
		}
		return e
	default:
		return nil
	}
}

func ToProtobufResult(result *updog.Result, qid int32) *proto.Result {
	pbr := &proto.Result{QueryId: qid, TotalCount: result.Count}

	for _, g := range result.Groups {
		fields := []*proto.Result_Group_ResultField{}

		for _, f := range g.Fields {
			fields = append(fields, &proto.Result_Group_ResultField{
				Column: f.Column,
				Value:  f.Value,
			})
		}

		pbr.Groups = append(pbr.Groups, &proto.Result_Group{
			Count:  g.Count,
			Fields: fields,
		})
	}

	return pbr
}
