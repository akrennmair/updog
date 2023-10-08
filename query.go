package updog

import (
	"fmt"

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

	var stack []*roaring.Bitmap

	for _, cmd := range qp.cmds {
		switch cmd.op {
		case cmdLoad:
			v, ok := idx.values[cmd.u64]
			if !ok {
				v = roaring.New()
			}
			stack = append(stack, v)
		case cmdNot:
			var elem *roaring.Bitmap
			elem, stack = pop(stack)
			elem = roaring.Flip(elem, 0, uint64(idx.nextRowID))
			stack = append(stack, elem)
		case cmdAnd:
			var a, b *roaring.Bitmap
			a, stack = pop(stack)
			b, stack = pop(stack)
			elem := roaring.And(a, b)
			stack = append(stack, elem)
		case cmdOr:
			var a, b *roaring.Bitmap
			a, stack = pop(stack)
			b, stack = pop(stack)
			elem := roaring.Or(a, b)
			stack = append(stack, elem)
		default:
			return nil, fmt.Errorf("invalid op code %d", cmd.op)
		}
	}

	if len(stack) != 1 {
		return nil, fmt.Errorf("expected single result after execution, got %d elements on stack instead", len(stack))
	}

	return &Result{
		Count: stack[0].GetCardinality(),
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
}

type Expression interface {
	gen(qp *queryPlan, sch *schema) error
}

type queryPlan struct {
	cmds          []cmd
	groupByFields []groupBy
}

type groupBy struct {
	Column string
	Value  string
	Idx    uint64
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
	Expression Expression
}

func (e *ExprNot) gen(qp *queryPlan, sch *schema) error {
	if err := e.Expression.gen(qp, sch); err != nil {
		return err
	}

	qp.cmds = append(qp.cmds, cmd{op: cmdNot})

	return nil
}

type ExprAnd struct {
	Left  Expression
	Right Expression
}

func (e *ExprAnd) gen(qp *queryPlan, sch *schema) error {
	if err := e.Left.gen(qp, sch); err != nil {
		return err
	}

	if err := e.Right.gen(qp, sch); err != nil {
		return err
	}

	qp.cmds = append(qp.cmds, cmd{op: cmdAnd})

	return nil
}

type ExprOr struct {
	Left  Expression
	Right Expression
}

func (e *ExprOr) gen(qp *queryPlan, sch *schema) error {
	if err := e.Left.gen(qp, sch); err != nil {
		return err
	}

	if err := e.Right.gen(qp, sch); err != nil {
		return err
	}

	qp.cmds = append(qp.cmds, cmd{op: cmdOr})

	return nil
}
