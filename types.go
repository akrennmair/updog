package updog

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// IndexWriter is a helper type to create a columnar index by adding row
// data. It needs to be created using the NewIndexWriter constructor function.
type IndexWriter struct {
	mtx sync.RWMutex

	schema *schema

	values    map[uint64]*roaring.Bitmap
	nextRowID uint32
}

type schema struct {
	Columns map[string]*column
}

func (sch *schema) add(k, v string) uint64 {
	col, ok := sch.Columns[k]
	if !ok {
		col = &column{
			Values: make(map[string]uint64),
		}
		sch.Columns[k] = col
	}

	val, ok := col.Values[v]
	if !ok {
		val = getValueIndex(k, v)
		col.Values[v] = val
	}

	return val
}

type column struct {
	Values map[string]uint64
}
