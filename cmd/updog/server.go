package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"

	"github.com/akrennmair/updog"
	"github.com/akrennmair/updog/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

type serverConfig struct {
	addr                string
	debugAddr           string
	indexFile           string
	enableCache         bool
	maxCacheSize        uint64
	enablePreloadedData bool
}

func serverCmd(cfg *serverConfig) error {
	var opts []updog.IndexOption

	reg := prometheus.NewRegistry()

	if cfg.enableCache {
		cacheHitCounter := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "updog_server_cache_hits_total",
				Help: "Number of cache hits.",
			},
		)

		cacheMissCounter := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "updog_server_cache_misses_total",
				Help: "Number of cache misses.",
			},
		)

		getCallCounter := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "updog_server_cache_get_calls_total",
				Help: "Number of cache get calls.",
			},
		)

		putCallCounter := prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: "updog_server_cache_put_calls_total",
				Help: "Number of cache put calls.",
			},
		)

		if err := reg.Register(cacheHitCounter); err != nil {
			return err
		}
		if err := reg.Register(cacheMissCounter); err != nil {
			return err
		}
		if err := reg.Register(getCallCounter); err != nil {
			return err
		}
		if err := reg.Register(putCallCounter); err != nil {
			return err
		}

		opts = append(opts, updog.WithCache(updog.NewLRUCache(cfg.maxCacheSize, updog.WithCacheMetrics(&updog.CacheMetrics{
			CacheHit:  cacheHitCounter,
			CacheMiss: cacheMissCounter,
			GetCall:   getCallCounter,
			PutCall:   putCallCounter,
		}))))
	}

	if cfg.enablePreloadedData {
		opts = append(opts, updog.WithPreloadedData())
	}

	executeDurationHistogram := prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "updog_server_query_exec_duration_seconds",
			Help:    "Histogram of query execution duration.",
			Buckets: prometheus.ExponentialBucketsRange(0.00001, 0.1, 6), // 6 buckets from 10µs to 100ms.
		},
	)
	if err := reg.Register(executeDurationHistogram); err != nil {
		return err
	}

	opts = append(opts, updog.WithIndexMetrics(&updog.IndexMetrics{
		ExecuteDuration: executeDurationHistogram,
	}))

	go func() {
		// TODO: change this to also expose pprof metrics.
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))
		if err := http.ListenAndServe(cfg.debugAddr, nil); err != nil {
			log.Printf("Error: failed to listen and serve prometheus metrics: %v", err)
		}
	}()

	idx, err := updog.OpenIndex(cfg.indexFile, opts...)
	if err != nil {
		return fmt.Errorf("failed to open index file: %w", err)
	}

	l, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	s := grpc.NewServer()

	proto.RegisterQueryServiceServer(s, &server{idx: idx})

	if err := s.Serve(l); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
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
