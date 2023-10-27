package updog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cespare/xxhash/v2"
	"github.com/dgraph-io/badger/v4"
)

// NewIndexWriter creates a new IndexWriter object. IndexWriter is used to add row data and to write
// the corresponding index data to a persistent store.
func NewIndexWriter() *IndexWriter {
	return &IndexWriter{
		schema: &schema{
			Columns: make(map[string]*column),
		},
		values: make(map[uint64]*roaring.Bitmap),
	}
}

// AddRow adds a row of data and returns its row ID. The row data must be provided as map,
// where the keys contain the column names, and the values the corresponding column values.
func (idx *IndexWriter) AddRow(values map[string]string) uint32 {
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

func (idx *IndexWriter) addToSchema(k, v string) uint64 {
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

func (idx *IndexWriter) getValueBitmap(valueIdx uint64) *roaring.Bitmap {
	bm, ok := idx.values[valueIdx]
	if !ok {
		bm = roaring.New()
		idx.values[valueIdx] = bm
	}

	return bm
}

func (idx *IndexWriter) optimize() {
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

var (
	keySchema      = []byte{'S'}
	keyNextRowID   = []byte{'I'}
	keyPrefixValue = []byte{'V'}
)

// WriteToDirectory writes the index data to the provided directory. In that directory,
// a badger database is created.
func (idx *IndexWriter) WriteToDirectory(d string) error {
	db, err := badger.Open(badger.DefaultOptions(d))
	if err != nil {
		return err
	}

	return idx.WriteToBadgerDatabase(db)
}

// WriteToBadgerDatabase writes the index data directly to a badger database.
func (idx *IndexWriter) WriteToBadgerDatabase(db *badger.DB) error {
	tx := db.NewTransaction(true)

	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	idx.optimize()

	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(idx.schema); err != nil {
		return err
	}

	if err := tx.Set(keySchema, buf.Bytes()); err != nil {
		tx.Discard()
		return err
	}

	var rowIDbuf [4]byte

	binary.BigEndian.PutUint32(rowIDbuf[:], idx.nextRowID)

	if err := tx.Set(keyNextRowID, rowIDbuf[:]); err != nil {
		tx.Discard()
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	for k, v := range idx.values {
		tx := db.NewTransaction(true)

		var keyBuf [8]byte

		binary.BigEndian.PutUint64(keyBuf[:], k)

		valueBuf, err := v.ToBytes()
		if err != nil {
			tx.Discard()
			return err
		}

		if err := tx.Set(append(keyPrefixValue, keyBuf[:]...), valueBuf); err != nil {
			tx.Discard()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
