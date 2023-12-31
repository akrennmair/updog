package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/akrennmair/updog"
	"github.com/akrennmair/updog/internal/convert"
	proto "github.com/akrennmair/updog/proto/updog/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
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
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

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
		mux := http.NewServeMux()

		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		mux.Handle("/metrics",
			promhttp.InstrumentMetricHandler(
				reg,
				promhttp.HandlerFor(
					reg,
					promhttp.HandlerOpts{Registry: reg},
				),
			),
		)

		s := &http.Server{
			Addr:    cfg.debugAddr,
			Handler: mux,
		}

		if err := s.ListenAndServe(); err != nil {
			log.Printf("Error: failed to listen and serve debug endpoints: %v", err)
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
		q := convert.ToQuery(pbq)

		qid := pbq.Id
		if qid == 0 {
			qid = int32(idx + 1)
		}

		result, err := s.idx.Execute(q)
		if err != nil {
			return nil, err
		}

		pbr := convert.ToProtobufResult(result, qid)

		resp.Results = append(resp.Results, pbr)
	}

	return &resp, nil
}
