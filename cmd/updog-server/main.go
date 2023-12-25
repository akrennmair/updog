package main

import (
	"context"
	"flag"
	"log"
	"net"

	"github.com/akrennmair/updog"
	"github.com/akrennmair/updog/proto"
	"google.golang.org/grpc"
)

func main() {
	var (
		indexFile string
		addr      string
		opts      []updog.IndexOption
	)

	flag.StringVar(&indexFile, "index-file", "", "index file to load")
	flag.StringVar(&addr, "addr", ":5113", "listen address")
	flag.Parse()

	idx, err := updog.OpenIndex(indexFile, opts...)
	if err != nil {
		log.Fatalf("Failed to open index: %v", err)
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()

	proto.RegisterQueryServiceServer(s, &server{idx: idx})

	if err := s.Serve(l); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

type server struct {
	proto.UnimplementedQueryServiceServer
	idx *updog.Index
}

func (s *server) Query(ctx context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	var resp proto.QueryResponse

	// TODO: execute queries concurrently.

	for idx, pbq := range req.Queries {
		q := convertToQuery(pbq)

		qid := pbq.Id
		if qid == 0 {
			qid = int32(idx + 1)
		}

		result, err := s.idx.Execute(q)
		if err != nil {
			return nil, err
		}

		pbr := convertToProtobufResult(result, qid)

		resp.Results = append(resp.Results, pbr)
	}

	return &resp, nil
}

func convertToQuery(pbq *proto.Query) *updog.Query {
	return &updog.Query{
		Expr:    convertToExpr(pbq.Expr),
		GroupBy: pbq.GroupBy,
	}
}

func convertToExpr(pbe *proto.Query_Expression) updog.Expression {
	switch v := pbe.Value.(type) {
	case *proto.Query_Expression_Eq:
		return &updog.ExprEqual{
			Column: v.Eq.Column,
			Value:  v.Eq.Value,
		}
	case *proto.Query_Expression_Not_:
		return &updog.ExprNot{
			Expr: convertToExpr(v.Not.Expr),
		}
	case *proto.Query_Expression_And_:
		e := &updog.ExprAnd{}
		for _, ee := range v.And.Exprs {
			e.Exprs = append(e.Exprs, convertToExpr(ee))
		}
		return e
	case *proto.Query_Expression_Or_:
		e := &updog.ExprOr{}
		for _, ee := range v.Or.Exprs {
			e.Exprs = append(e.Exprs, convertToExpr(ee))
		}
		return e
	default:
		return nil
	}
}

func convertToProtobufResult(result *updog.Result, qid int32) *proto.Result {
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

	return nil
}
