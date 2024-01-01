package updog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/akrennmair/updog/internal/openfile"
	"github.com/cespare/xxhash/v2"
	"go.etcd.io/bbolt"
)

// IndexWriter is a helper type to create a columnar index by adding row
// data. It needs to be created using the NewIndexWriter constructor function.
type IndexWriter struct {
	mtx sync.RWMutex

	schema *schema

	values    map[uint64]*roaring.Bitmap
	nextRowID uint32

	filename string
}

// NewIndexWriter creates a new IndexWriter object. IndexWriter is used to add row data and to write
// the corresponding index data to a persistent store.
func NewIndexWriter(filename string) *IndexWriter {
	return &IndexWriter{
		schema: &schema{
			Columns: make(map[string]*column),
		},
		values:   make(map[uint64]*roaring.Bitmap),
		filename: filename,
	}
}

// AddRow adds a row of data and returns its row ID. The row data must be provided as map,
// where the keys contain the column names, and the values the corresponding column values.
func (idx *IndexWriter) AddRow(values map[string]string) (uint32, error) {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	rowID := idx.nextRowID
	defer func() {
		idx.nextRowID++
	}()

	for k, v := range values {
		valueIdx := idx.schema.add(k, v)

		bm := idx.getValueBitmap(valueIdx)

		bm.Add(rowID)
	}

	return rowID, nil
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

// WriteToFile writes the index data to the provided file.
func (idx *IndexWriter) Flush() error {
	db, err := bbolt.Open(idx.filename, 0644, &bbolt.Options{OpenFile: openfile.OpenFile(openfile.Options{FailIfFileExists: true})})
	if err != nil {
		return err
	}

	defer db.Close()

	return idx.WriteToBoltDatabase(db)
}

// WriteToBoltDatabase writes the index data directly to a badger database.
func (idx *IndexWriter) WriteToBoltDatabase(db *bbolt.DB) error {
	tx, err := db.Begin(true)
	if err != nil {
		return fmt.Errorf("failed to start new transaction: %w", err)
	}

	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	idx.optimize()

	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(idx.schema); err != nil {
		return err
	}

	bucket, err := tx.CreateBucketIfNotExists([]byte("data"))
	if err != nil {
		return err
	}

	if err := bucket.Put(keySchema, buf.Bytes()); err != nil {
		return err
	}

	var rowIDbuf [4]byte

	binary.BigEndian.PutUint32(rowIDbuf[:], idx.nextRowID)

	if err := bucket.Put(keyNextRowID, rowIDbuf[:]); err != nil {
		return err
	}

	i := 0

	for k, v := range idx.values {
		var keyBuf [8]byte

		binary.BigEndian.PutUint64(keyBuf[:], k)

		valueBuf, err := v.ToBytes()
		if err != nil {
			return err
		}

		if err := bucket.Put(append(keyPrefixValue, keyBuf[:]...), valueBuf); err != nil {
			return err
		}

		i++

		if i%1000 == 0 {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("failed to commit transaction: %w", err)
			}

			tx, err = db.Begin(true)
			if err != nil {
				return fmt.Errorf("failed to start new transaction: %w", err)
			}

			bucket = tx.Bucket([]byte("data"))
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
