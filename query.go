package updog

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
)

// Query describes a count query to execute on an index. updog allows you to run
// the equivalent of SQL queries like `SELECT x, y, z, COUNT(*) WHERE ... GROUP BY x, y, z`.
type Query struct {
	// Expr is the expression you want to limit your query on. You can use the types ExprEqual, ExprNot,
	// ExprAnd and ExprOr to construct your expression.
	Expr Expression

	// GroupBy is a list of column names you want to group by. The result will then contain the
	// results for all available values of all the listed columns for which a non-zero count result
	// was determined.
	GroupBy []string

	groupByFields []groupBy
}

// Execute runs the provided query on the index and returns the query result.
func (idx *Index) Execute(q *Query) (*Result, error) {
	if idx.metrics.ExecuteDuration != nil {
		defer func(t0 time.Time) {
			idx.metrics.ExecuteDuration.Observe(time.Since(t0).Seconds())
		}(time.Now())
	}

	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	if err := q.populateGroupBy(q.GroupBy, idx.schema); err != nil {
		return nil, err
	}

	result, err := q.Expr.eval(idx)
	if err != nil {
		return nil, err
	}

	return &Result{
		Count:  result.GetCardinality(),
		Groups: q.groupBy(result, idx),
	}, nil
}

// Result contains the query result.
type Result struct {
	// Count is the total count of rows that matched the query expression.
	Count uint64

	// Groups contains a list of grouped results. If no GroupBy list was provided
	// in the query, this list will be empty.
	Groups []ResultGroup
}

// ResultGroup contains a single grouped result.
type ResultGroup struct {
	// Fields contains a list of result fields for that group, each field consisting of the
	// column name and value.
	Fields []ResultField

	// Count contains the determined count for the list of result fields.
	Count uint64
}

// ResultField contains a single column name and value. It is used in ResultGroup objects.
type ResultField struct {
	Column string
	Value  string
}

type Expression interface {
	eval(idx *Index) (*roaring.Bitmap, error)
	String() string
	cacheKey() uint64
}

func (q *Query) populateGroupBy(columns []string, sch *schema) error {
	for _, colName := range columns {
		col, ok := sch.Columns[colName]
		if !ok {
			return fmt.Errorf("column %q not found", colName)
		}

		gb := groupBy{Column: colName}

		for v, valueIdx := range col.Values {
			gb.Values = append(gb.Values, groupByValue{
				Value: v,
				Idx:   valueIdx,
			})
		}

		sort.Slice(gb.Values, func(i, j int) bool {
			return gb.Values[i].Value < gb.Values[j].Value
		})

		q.groupByFields = append(q.groupByFields, gb)
	}

	return nil
}

type resultGroup struct {
	fields []ResultField
	result *roaring.Bitmap
}

func (q *Query) groupBy(result *roaring.Bitmap, idx *Index) (finalResult []ResultGroup) {
	if len(q.groupByFields) == 0 {
		return nil
	}

	resultGroups := []resultGroup{
		{result: result},
	}

	for _, gbf := range q.groupByFields {
		var newResultGroups []resultGroup

		for _, rg := range resultGroups {
			for _, v := range gbf.Values {
				vbm, err := idx.values.GetCol(v.Idx)
				if err != nil {
					continue
				}

				result := roaring.And(rg.result, vbm)
				if result.GetCardinality() == 0 {
					continue
				}

				newResultGroups = append(newResultGroups, resultGroup{
					fields: append(rg.fields, ResultField{Column: gbf.Column, Value: v.Value}),
					result: result,
				})
			}
		}

		resultGroups = newResultGroups
	}

	for _, rg := range resultGroups {
		finalResult = append(finalResult, ResultGroup{
			Fields: rg.fields,
			Count:  rg.result.GetCardinality(),
		})
	}

	return finalResult
}

type groupBy struct {
	Column string
	Values []groupByValue
}

type groupByValue struct {
	Value string
	Idx   uint64
}

type ExprEqual struct {
	Column string
	Value  string
}

func (e *ExprEqual) eval(idx *Index) (*roaring.Bitmap, error) {
	_, ok := idx.schema.Columns[e.Column]
	if !ok {
		return nil, fmt.Errorf("column %q not found in schema", e.Column)
	}

	valueIdx := getValueIndex(e.Column, e.Value)

	cacheKey := e.cacheKey()

	bm, ok := idx.cache.Get(cacheKey)
	if ok {
		return bm, nil
	}

	bm, err := idx.values.GetCol(valueIdx)
	if err != nil || bm == nil {
		bm = roaring.New()
	}

	idx.cache.Put(cacheKey, bm)

	return bm, nil
}

func (e *ExprEqual) String() string {
	return fmt.Sprintf("(EQUAL %s %q)", e.Column, e.Value)
}

func (e *ExprEqual) cacheKey() uint64 {
	return getValueIndex(e.Column, e.Value)
}

type ExprNot struct {
	Expr Expression
}

func (e *ExprNot) eval(idx *Index) (*roaring.Bitmap, error) {
	cacheKey := e.cacheKey()

	bm, ok := idx.cache.Get(cacheKey)
	if ok {
		return bm, nil
	}

	bm, err := e.Expr.eval(idx)
	if err != nil {
		return nil, err
	}

	bm = roaring.Flip(bm, 0, uint64(idx.nextRowID))

	idx.cache.Put(cacheKey, bm)

	return bm, nil
}

func (e *ExprNot) String() string {
	return fmt.Sprintf("(NOT %s)", e.Expr.String())
}

const (
	maskNot = 0x87A9CD14CAEB50EB
	maskAnd = 0xF9F1F5ADCB67A077
	maskOr  = 0xBFB85A99B03E78E7
)

func (e *ExprNot) cacheKey() uint64 {
	return bits.RotateLeft64(e.Expr.cacheKey(), 1) ^ maskNot
}

type ExprAnd struct {
	Exprs []Expression
}

func (e *ExprAnd) eval(idx *Index) (*roaring.Bitmap, error) {
	var elems []*roaring.Bitmap

	cacheKey := e.cacheKey()

	bm, ok := idx.cache.Get(cacheKey)
	if ok {
		return bm, nil
	}

	for _, e := range e.Exprs {
		elem, err := e.eval(idx)
		if err != nil {
			return nil, err
		}

		elems = append(elems, elem)
	}

	bm = roaring.FastAnd(elems...)

	idx.cache.Put(cacheKey, bm)

	return bm, nil
}

func (e *ExprAnd) String() string {
	var buf strings.Builder

	buf.WriteString("(AND ")

	for idx, expr := range e.Exprs {
		if idx > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(expr.String())
	}

	buf.WriteString(")")

	return buf.String()
}

func (e *ExprAnd) cacheKey() uint64 {
	key := uint64(maskAnd)
	for _, e := range e.Exprs {
		key = key ^ bits.RotateLeft64(e.cacheKey(), 1)
	}

	return key
}

type ExprOr struct {
	Exprs []Expression
}

func (e *ExprOr) eval(idx *Index) (*roaring.Bitmap, error) {
	var elems []*roaring.Bitmap

	cacheKey := e.cacheKey()

	bm, ok := idx.cache.Get(cacheKey)
	if ok {
		return bm, nil
	}

	for _, e := range e.Exprs {
		elem, err := e.eval(idx)
		if err != nil {
			return nil, err
		}

		elems = append(elems, elem)
	}

	bm = roaring.FastOr(elems...)

	idx.cache.Put(cacheKey, bm)

	return bm, nil
}

func (e *ExprOr) String() string {
	var buf strings.Builder

	buf.WriteString("(OR ")

	for idx, expr := range e.Exprs {
		if idx > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(expr.String())
	}

	buf.WriteString(")")

	return buf.String()
}

func (e *ExprOr) cacheKey() uint64 {
	key := uint64(maskOr)
	for _, e := range e.Exprs {
		key = key ^ bits.RotateLeft64(e.cacheKey(), 1)
	}

	return key
}
