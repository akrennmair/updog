package updog

import (
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"
)

type Query struct {
	Expr    Expression
	GroupBy []string
}

func (q *Query) Execute(idx *Index) (*Result, error) {
	idx.mtx.RLock()
	defer idx.mtx.RUnlock()

	var qp queryPlan

	if err := qp.populateGroupBy(q.GroupBy, idx.schema); err != nil {
		return nil, err
	}

	result, err := q.Expr.eval(idx)
	if err != nil {
		return nil, err
	}

	return &Result{
		Count:  result.GetCardinality(),
		Groups: qp.groupBy(result, idx),
	}, nil
}

type Result struct {
	Count uint64

	Groups []ResultGroup
}

type ResultGroup struct {
	Fields []ResultField
	Count  uint64
}

type ResultField struct {
	Column string
	Value  string
}

type Expression interface {
	eval(idx *Index) (*roaring.Bitmap, error)
	String() string
	cacheKey() uint64
}

type queryPlan struct {
	groupByFields []groupBy
}

func (qp *queryPlan) populateGroupBy(columns []string, sch *schema) error {
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

		qp.groupByFields = append(qp.groupByFields, gb)
	}

	return nil
}

type resultGroup struct {
	fields []ResultField
	result *roaring.Bitmap
}

func (qp *queryPlan) groupBy(result *roaring.Bitmap, idx *Index) (finalResult []ResultGroup) {
	if len(qp.groupByFields) == 0 {
		return nil
	}

	resultGroups := []resultGroup{
		{result: result},
	}

	for _, gbf := range qp.groupByFields {
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
