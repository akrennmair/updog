package updog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/dgraph-io/badger/v4"
)

func OpenIndex(dir string, opts ...IndexOption) (*Index, error) {
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		return nil, err
	}

	return OpenIndexFromBadgerDatabase(db, opts...)
}

func OpenIndexFromBadgerDatabase(db *badger.DB, opts ...IndexOption) (*Index, error) {
	idx := &Index{}

	idx.db = db

	err := db.View(func(tx *badger.Txn) error {
		schemaItem, err := tx.Get(keySchema)
		if err != nil {
			return err
		}

		schemaData, err := schemaItem.ValueCopy(nil)
		if err != nil {
			return err
		}

		var sch schema

		if err := gob.NewDecoder(bytes.NewReader(schemaData)).Decode(&sch); err != nil {
			return err
		}

		idx.schema = &sch

		rowsItem, err := tx.Get(keyNextRowID)
		if err != nil {
			return err
		}

		rowsData, err := rowsItem.ValueCopy(nil)
		if err != nil {
			return err
		}

		idx.nextRowID = binary.BigEndian.Uint32(rowsData)

		return nil
	})

	if err != nil {
		db.Close()
		return nil, err
	}

	idx.cache = &nullCache{}

	for _, opt := range opts {
		if err := opt(idx); err != nil {
			return nil, err
		}
	}

	if idx.values == nil {
		idx.values = newOnDemandColGetter(idx.db)
	}

	return idx, nil
}

type Index struct {
	mtx sync.RWMutex

	schema    *schema
	nextRowID uint32

	db *badger.DB

	values colGetter

	cache cache
}

type cache interface {
	Get(key uint64) (*roaring.Bitmap, bool)
	Put(key uint64, bm *roaring.Bitmap)
}

type nullCache struct{}

func (c *nullCache) Get(key uint64) (*roaring.Bitmap, bool) {
	return nil, false
}

func (c *nullCache) Put(key uint64, bm *roaring.Bitmap) {
}

type colGetter interface {
	GetCol(key uint64) (*roaring.Bitmap, error)
}

type IndexOption func(idx *Index) error

func (idx *Index) Close() error {
	if idx.db == nil {
		return nil
	}

	err := idx.db.Close()
	idx.db = nil
	return err
}

func newOnDemandColGetter(db *badger.DB) colGetter {
	return &onDemandColGetter{db: db}
}

type onDemandColGetter struct {
	db *badger.DB
}

func (g *onDemandColGetter) GetCol(key uint64) (*roaring.Bitmap, error) {
	var bm *roaring.Bitmap

	err := g.db.View(func(tx *badger.Txn) error {
		var keyBuf [8]byte

		binary.BigEndian.PutUint64(keyBuf[:], key)

		item, err := tx.Get(append(keyPrefixValue, keyBuf[:]...))
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		bm = roaring.New()

		if _, err := bm.FromBuffer(data); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return bm, nil
}

func WithPreloadedData() IndexOption {
	return func(idx *Index) error {
		cg, err := newPreloadedColGetter(idx.db)
		if err != nil {
			return err
		}

		idx.values = cg

		return nil
	}
}

func newPreloadedColGetter(db *badger.DB) (colGetter, error) {
	cg := &preloadedColGetter{
		values: map[uint64]*roaring.Bitmap{},
	}

	err := db.View(func(tx *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = keyPrefixValue

		iter := tx.NewIterator(opts)

		defer iter.Close()

		for iter.Rewind(); iter.Valid(); iter.Next() {
			keyData := iter.Item().Key()

			key := binary.BigEndian.Uint64(keyData[1:])

			value, err := iter.Item().ValueCopy(nil)
			if err != nil {
				return err
			}

			bm := roaring.New()
			if _, err := bm.FromBuffer(value); err != nil {
				return err
			}

			cg.values[key] = bm
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return cg, nil
}

type preloadedColGetter struct {
	values map[uint64]*roaring.Bitmap
}

func (cg *preloadedColGetter) GetCol(key uint64) (*roaring.Bitmap, error) {
	return cg.values[key], nil
}
