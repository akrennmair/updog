package main

import (
	"context"
	"errors"
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
	return nil, errors.New("not implemented")
}
