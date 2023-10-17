package updog

import (
	"errors"
	"fmt"
	"sort"

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

	if err := q.Expr.gen(&qp, idx.schema); err != nil {
		return nil, err
	}

	if err := qp.populateGroupBy(q.GroupBy, idx.schema); err != nil {
		return nil, err
	}

	var stack []*roaring.Bitmap

	for _, cmd := range qp.cmds {
		switch cmd.op {
		case cmdLoad:
			v, err := idx.values.GetCol(cmd.u64)
			if err != nil {
				v = roaring.New()
			}
			stack = append(stack, v)
		case cmdNot:
			var elem *roaring.Bitmap
			elem, stack = pop(stack)
			elem = roaring.Flip(elem, 0, uint64(idx.nextRowID))
			stack = append(stack, elem)
		case cmdAnd:
			var elems []*roaring.Bitmap
			for i := uint64(0); i < cmd.u64; i++ {
				var a *roaring.Bitmap
				a, stack = pop(stack)
				elems = append(elems, a)
			}
			elem := roaring.FastAnd(elems...)
			stack = append(stack, elem)
		case cmdOr:
			var elems []*roaring.Bitmap
			for i := uint64(0); i < cmd.u64; i++ {
				var a *roaring.Bitmap
				a, stack = pop(stack)
				elems = append(elems, a)
			}
			elem := roaring.FastOr(elems...)
			stack = append(stack, elem)
		default:
			return nil, fmt.Errorf("invalid op code %d", cmd.op)
		}
	}

	if len(stack) != 1 {
		return nil, fmt.Errorf("expected single result after execution, got %d elements on stack instead", len(stack))
	}

	return &Result{
		Count:  stack[0].GetCardinality(),
		Groups: qp.groupBy(stack[0], idx),
	}, nil
}

func pop(stack []*roaring.Bitmap) (elem *roaring.Bitmap, newStack []*roaring.Bitmap) {
	n := len(stack)
	elem = stack[n-1]
	newStack = stack[:n-1]
	return
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
	gen(qp *queryPlan, sch *schema) error
}

type queryPlan struct {
	cmds          []cmd
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

type cmd struct {
	op  cmdOp
	u64 uint64
}

type cmdOp int

const (
	cmdLoad cmdOp = iota
	cmdNot
	cmdAnd
	cmdOr
)

type ExprEqual struct {
	Column string
	Value  string
}

func (e *ExprEqual) gen(qp *queryPlan, sch *schema) error {
	_, ok := sch.Columns[e.Column]
	if !ok {
		return fmt.Errorf("column %q not found in schema", e.Column)
	}

	valueIdx := getValueIndex(e.Column, e.Value)

	qp.cmds = append(qp.cmds, cmd{op: cmdLoad, u64: valueIdx})

	return nil
}

type ExprNot struct {
	Expr Expression
}

func (e *ExprNot) gen(qp *queryPlan, sch *schema) error {
	if err := e.Expr.gen(qp, sch); err != nil {
		return err
	}

	qp.cmds = append(qp.cmds, cmd{op: cmdNot})

	return nil
}

type ExprAnd struct {
	Exprs []Expression
}

func (e *ExprAnd) gen(qp *queryPlan, sch *schema) error {
	if len(e.Exprs) == 0 {
		return errors.New("no expression to AND")
	}

	for _, expr := range e.Exprs {
		if err := expr.gen(qp, sch); err != nil {
			return err
		}
	}

	qp.cmds = append(qp.cmds, cmd{op: cmdAnd, u64: uint64(len(e.Exprs))})

	return nil
}

type ExprOr struct {
	Exprs []Expression
}

func (e *ExprOr) gen(qp *queryPlan, sch *schema) error {
	if len(e.Exprs) == 0 {
		return errors.New("no expression to OR")
	}

	for _, expr := range e.Exprs {
		if err := expr.gen(qp, sch); err != nil {
			return err
		}
	}

	qp.cmds = append(qp.cmds, cmd{op: cmdOr, u64: uint64(len(e.Exprs))})

	return nil
}
