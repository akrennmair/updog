package updog

import (
	"bufio"
	"encoding/gob"
	"io"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cespare/xxhash/v2"
)

func NewIndex() *Index {
	return &Index{
		schema: &schema{
			Columns: make(map[string]*column),
		},
		values: make(map[uint64]*roaring.Bitmap),
	}
}

func (idx *Index) AddRow(values map[string]string) uint32 {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	rowID := idx.nextRowID
	defer func() {
		idx.nextRowID++
	}()

	for k, v := range values {
		valueIdx := idx.addToSchema(k, v)

		bm := idx.getValueBitmap(valueIdx)

		bm.Add(rowID)
	}

	return rowID
}

func (idx *Index) addToSchema(k, v string) uint64 {
	col, ok := idx.schema.Columns[k]
	if !ok {
		col = &column{
			Values: make(map[string]uint64),
		}
		idx.schema.Columns[k] = col
	}

	val, ok := col.Values[v]
	if !ok {
		val = getValueIndex(k, v)
		col.Values[v] = val
	}

	return val
}

func getValueIndex(k, v string) uint64 {
	return xxhash.Sum64(append(append([]byte(k), 0), []byte(v)...))
}

func (idx *Index) getValueBitmap(valueIdx uint64) *roaring.Bitmap {
	bm, ok := idx.values[valueIdx]
	if !ok {
		bm = roaring.New()
		idx.values[valueIdx] = bm
	}

	return bm
}

func (idx *Index) Optimize() {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	idx.optimize()
}

func (idx *Index) optimize() {
	var wg sync.WaitGroup

	for _, bm := range idx.values {
		wg.Add(1)
		go func(b *roaring.Bitmap) {
			defer wg.Done()
			b.RunOptimize()
		}(bm)
	}

	wg.Wait()
}

func (idx *Index) WriteTo(w io.Writer) error {
	bw := bufio.NewWriter(w)

	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	idx.optimize()

	enc := gob.NewEncoder(bw)
	if err := enc.Encode(idx.schema); err != nil {
		return err
	}

	if err := enc.Encode(idx.nextRowID); err != nil {
		return err
	}

	numValues := len(idx.values)

	if err := enc.Encode(numValues); err != nil {
		return err
	}

	for k, v := range idx.values {
		if err := enc.Encode(k); err != nil {
			return err
		}

		if err := enc.Encode(v); err != nil {
			return err
		}
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	return nil
}

func NewIndexFromReader(r io.Reader) (*Index, error) {
	idx := NewIndex()

	dec := gob.NewDecoder(r)

	if err := dec.Decode(&idx.schema); err != nil {
		return nil, err
	}

	if err := dec.Decode(&idx.nextRowID); err != nil {
		return nil, err
	}

	var numValues int

	if err := dec.Decode(&numValues); err != nil {
		return nil, err
	}

	for i := 0; i < numValues; i++ {
		var (
			k uint64
			v roaring.Bitmap
		)

		if err := dec.Decode(&k); err != nil {
			return nil, err
		}

		if err := dec.Decode(&v); err != nil {
			return nil, err
		}

		idx.values[k] = &v
	}

	return idx, nil
}
