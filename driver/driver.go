package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/akrennmair/updog"
	"github.com/akrennmair/updog/internal/convert"
	"github.com/akrennmair/updog/internal/queryparser"
	updogv1 "github.com/akrennmair/updog/proto/updog/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	sql.Register("updog", newUpdogDriver())
}

func newUpdogDriver() *updogDriver {
	return &updogDriver{
		fileConnCache: map[fileCacheKey]*fileConn{},
	}
}

type updogDriver struct {
	fileConnMtx   sync.RWMutex
	fileConnCache map[fileCacheKey]*fileConn
}

func (d *updogDriver) Open(name string) (driver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("couldn't parse connection string: %v", err)
	}

	switch u.Scheme {
	case "file":
		filepath := u.Opaque
		if filepath == "" {
			filepath = u.Path
		}
		return d.openFile(filepath, u.Query())
	case "grpc":
		return d.openConn(u.Hostname(), u.Port())
	default:
		return nil, fmt.Errorf("unsupported connection type %q", u.Scheme)
	}
}

type fileCacheKey struct {
	file string
	opts string
}

func (d *updogDriver) openFile(file string, optValues url.Values) (driver.Conn, error) {
	var opts []updog.IndexOption

	key := fileCacheKey{
		file: file,
	}

	if optValues.Get("preload") == "true" {
		opts = append(opts, updog.WithPreloadedData())
		key.opts += ";preload=true"
	}

	if optValues.Get("lrucache") == "true" {
		key.opts += ";lrucache=true"

		cacheSizeStr := optValues.Get("lrucachesize")
		cacheSize, err := strconv.ParseUint(cacheSizeStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid lrucachesize: %v", err)
		}

		key.opts += ";lrucachesize=" + cacheSizeStr
		lruCache := updog.NewLRUCache(cacheSize)

		opts = append(opts, updog.WithCache(lruCache))
	}

	d.fileConnMtx.RLock()
	conn, ok := d.fileConnCache[key]
	d.fileConnMtx.RUnlock()

	if ok {
		conn.refs.Add(1)
		return conn, nil
	}

	idx, err := updog.OpenIndex(file, opts...)
	if err != nil {
		return nil, fmt.Errorf("couldn't open index file %q: %v", file, err)
	}

	conn = &fileConn{
		idx: idx,
	}

	d.fileConnMtx.Lock()
	d.fileConnCache[key] = conn
	d.fileConnMtx.Unlock()

	conn.refs.Add(1)

	return conn, nil
}

func (d *updogDriver) openConn(host string, port string) (driver.Conn, error) {
	conn, err := grpc.Dial(host+":"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &grpcConn{conn: conn, client: updogv1.NewQueryServiceClient(conn)}, nil
}

type fileConn struct {
	idx *updog.Index

	refs atomic.Int32
}

func (c *fileConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

func (c *fileConn) prepare(query string) (*fileStmt, error) {
	q, err := queryparser.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("parsing query failed: %v", err)
	}

	return &fileStmt{
		c: c,
		q: q,
	}, nil
}

func (c *fileConn) Close() error {
	if c.refs.Add(-1) <= 0 {
		idx := c.idx
		c.idx = nil
		return idx.Close()
	}

	return nil
}

func (c *fileConn) Begin() (driver.Tx, error) {
	return c, nil
}

func (c *fileConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c, nil
}

func (c *fileConn) Commit() error {
	return nil
}

func (c *fileConn) Rollback() error {
	return nil
}

func (c *fileConn) Ping(ctx context.Context) error {
	return nil
}

func (c *fileConn) ResetSession(ctx context.Context) error {
	return nil
}

func (c *fileConn) IsValid() bool {
	return c.idx != nil && c.refs.Load() >= 1
}

func (c *fileConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	stmt, err := c.prepare(query)
	if err != nil {
		return nil, err
	}

	size := 0

	for _, a := range args {
		if a.Ordinal > size {
			size = a.Ordinal
		}
	}

	values := make([]string, size)

	for _, a := range args {
		values[a.Ordinal-1] = fmt.Sprint(a.Value)
	}

	return stmt.query(values)
}

func (stmt *fileStmt) query(values []string) (driver.Rows, error) {
	q := queryparser.ReplacePlaceholders(stmt.q, values)

	qq := convert.ToQuery(q)

	result, err := stmt.c.idx.Execute(qq)
	if err != nil {
		return nil, err
	}

	return newRows(result, q.GroupBy), nil
}

func (c *fileConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return c.Prepare(query)
}

type fileStmt struct {
	c *fileConn
	q *updogv1.Query
}

func (stmt *fileStmt) Close() error {
	return nil
}

func numInput(q *updogv1.Query) int {
	maxPlaceholder := int32(0)

	queryparser.Walk(q, func(e *updogv1.Query_Expression) bool {
		if v, ok := e.Value.(*updogv1.Query_Expression_Eq); ok {
			if v.Eq.Placeholder > maxPlaceholder {
				maxPlaceholder = v.Eq.Placeholder
			}
		}
		return true
	})

	return int(maxPlaceholder)
}

func (stmt *fileStmt) NumInput() int {
	return numInput(stmt.q)
}

func (stmt *fileStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("only queries are supported")
}

func (stmt *fileStmt) Query(args []driver.Value) (driver.Rows, error) {
	var values []string

	for _, a := range args {
		values = append(values, fmt.Sprint(a))
	}

	return stmt.query(values)
}

func newRows(result *updog.Result, groupBy []string) *rows {
	r := &rows{
		cols: append(groupBy, "count"),
	}

	if len(result.Groups) > 0 {
		for _, rr := range result.Groups {
			fields := []string{}
			for _, f := range rr.Fields {
				fields = append(fields, f.Value)
			}
			r.rows = append(r.rows, row{count: rr.Count, fields: fields})
		}
	} else {
		r.rows = []row{{count: result.Count}}
	}

	return r
}

type row struct {
	fields []string
	count  uint64
}

type rows struct {
	cols   []string
	rows   []row
	closed bool
	idx    int
}

func (r *rows) Columns() []string {
	return r.cols
}

func (r *rows) Close() error {
	r.closed = true
	return nil
}

func (r *rows) Next(values []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}

	for idx, f := range r.rows[r.idx].fields {
		values[idx] = f
	}

	values[len(r.rows[r.idx].fields)] = int64(r.rows[r.idx].count)

	r.idx++

	return nil
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	if index < len(r.cols)-1 {
		return reflect.TypeOf("")
	}

	return reflect.TypeOf(int64(0))
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < len(r.cols)-1 {
		return "TEXT"
	}

	return "BIGINT"
}

func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < len(r.cols)-1 {
		return math.MaxInt64, true
	}

	return 0, false
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return false, true
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	return 0, 0, false
}

type grpcConn struct {
	conn   *grpc.ClientConn
	client updogv1.QueryServiceClient
}

func (c *grpcConn) Prepare(query string) (driver.Stmt, error) {
	return c.prepare(query)
}

func (c *grpcConn) prepare(query string) (*grpcStmt, error) {
	q, err := queryparser.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("parsing query failed: %v", err)
	}

	return &grpcStmt{
		c: c,
		q: q,
	}, nil
}

func (c *grpcConn) Close() error {
	return c.conn.Close()
}

func (c *grpcConn) Begin() (driver.Tx, error) {
	return c, nil
}

func (c *grpcConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c, nil
}

func (c *grpcConn) Commit() error {
	return nil
}

func (c *grpcConn) Rollback() error {
	return nil
}

func (c *grpcConn) Ping(ctx context.Context) error {
	return nil
}

func (c *grpcConn) ResetSession(ctx context.Context) error {
	return nil
}

func (c *grpcConn) IsValid() bool {
	return c.conn != nil && c.client != nil
}

type grpcStmt struct {
	c *grpcConn
	q *updogv1.Query
}

func (stmt *grpcStmt) Close() error {
	return nil
}

func (stmt *grpcStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, errors.New("only queries are supported")
}

func (stmt *grpcStmt) Query(args []driver.Value) (driver.Rows, error) {
	var values []string

	for _, a := range args {
		values = append(values, fmt.Sprint(a))
	}

	return stmt.query(values)
}

func (stmt *grpcStmt) query(values []string) (driver.Rows, error) {
	q := queryparser.ReplacePlaceholders(stmt.q, values)

	result, err := stmt.c.client.Query(context.Background(), &updogv1.QueryRequest{
		Queries: []*updogv1.Query{q},
	})
	if err != nil {
		return nil, err
	}

	if len(result.Results) != 1 {
		return nil, fmt.Errorf("expected 1 result, got %d", len(result.Results))
	}

	return newRows(convert.ToResult(result.Results[0]), q.GroupBy), nil
}

func (stmt *grpcStmt) NumInput() int {
	return numInput(stmt.q)
}
