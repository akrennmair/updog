package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/akrennmair/updog/cmd/updog/queryparser"
	"github.com/akrennmair/updog/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clientConfig struct {
	addr string
}

func clientCmd(cfg *clientConfig, queries []string) error {
	if len(queries) == 0 {
		return errors.New("no queries provided")
	}

	parsedQueries, err := parseQueries(queries)
	if err != nil {
		return fmt.Errorf("failed to parse queries: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := grpc.Dial(cfg.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer conn.Close()

	client := proto.NewQueryServiceClient(conn)

	req := &proto.QueryRequest{
		Queries: parsedQueries,
	}

	resp, err := client.Query(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to query: %w", err)
	}

	for idx, result := range resp.Results {
		if idx > 0 {
			fmt.Println("")
		}

		fmt.Printf("Query %d:\n", result.QueryId)
		fmt.Printf("\tTotal count: %d\n", result.TotalCount)
		for _, group := range result.Groups {
			fmt.Printf("\tGroup %s: %d\n", formatGroupFields(group.Fields), group.Count)
		}
	}

	return nil
}

func formatGroupFields(fields []*proto.Result_Group_ResultField) string {
	var buf strings.Builder

	for idx, f := range fields {
		if idx > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(f.Column)
		buf.WriteString("=")
		buf.WriteString(strconv.Quote(f.Value))
	}

	return buf.String()
}

func parseQueries(queries []string) (parsedQueries []*proto.Query, err error) {
	for idx, q := range queries {
		pq, err := queryparser.ParseQuery(q)
		if err != nil {
			return nil, fmt.Errorf("failed to parse query %q: %w", q, err)
		}

		pq.Id = int32(idx + 1)

		parsedQueries = append(parsedQueries, pq)
	}

	return parsedQueries, nil
}
