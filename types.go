package updog

import (
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type Index struct {
	mtx sync.Mutex

	schema *schema

	values    map[uint64]*roaring.Bitmap
	nextRowID uint32
}

type schema struct {
	Columns map[string]*column
}

type column struct {
	Values map[string]uint64
}