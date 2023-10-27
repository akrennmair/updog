package updog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"go.etcd.io/bbolt"
)

func OpenIndex(file string, opts ...IndexOption) (*Index, error) {
	db, err := bbolt.Open(file, 0644, &bbolt.Options{})
	if err != nil {
		return nil, err
	}

	return OpenIndexFromBoltDatabase(db, opts...)
}

func OpenIndexFromBoltDatabase(db *bbolt.DB, opts ...IndexOption) (*Index, error) {
	idx := &Index{}

	idx.db = db

	err := db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("data"))
		schemaItem := bucket.Get(keySchema)

		var sch schema

		if err := gob.NewDecoder(bytes.NewReader(schemaItem)).Decode(&sch); err != nil {
			return err
		}

		idx.schema = &sch

		rowsItem := bucket.Get(keyNextRowID)

		idx.nextRowID = binary.BigEndian.Uint32(rowsItem)
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

	db *bbolt.DB

	values colGetter

	cache cache
}

type colGetter interface {
	GetCol(key uint64) (*roaring.Bitmap, error)
}

type IndexOption func(idx *Index) error

func WithLRUCache(maxSizeBytes uint64) IndexOption {
	return func(idx *Index) error {
		idx.cache = newLRUCache(maxSizeBytes)
		return nil
	}
}

func (idx *Index) Close() error {
	if idx.db == nil {
		return nil
	}

	err := idx.db.Close()
	idx.db = nil
	return err
}

func newOnDemandColGetter(db *bbolt.DB) colGetter {
	return &onDemandColGetter{db: db}
}

type onDemandColGetter struct {
	db *bbolt.DB
}

func (g *onDemandColGetter) GetCol(key uint64) (*roaring.Bitmap, error) {
	var bm *roaring.Bitmap

	err := g.db.View(func(tx *bbolt.Tx) error {
		var keyBuf [8]byte

		bucket := tx.Bucket([]byte("data"))

		binary.BigEndian.PutUint64(keyBuf[:], key)

		item := bucket.Get(append(keyPrefixValue, keyBuf[:]...))

		bm = roaring.New()

		if _, err := bm.FromBuffer(item); err != nil {
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

func newPreloadedColGetter(db *bbolt.DB) (colGetter, error) {
	cg := &preloadedColGetter{
		values: map[uint64]*roaring.Bitmap{},
	}

	err := db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte("data")).Cursor()

		for k, v := c.Seek(keyPrefixValue); k != nil && bytes.HasPrefix(k, keyPrefixValue); k, v = c.Next() {
			key := binary.BigEndian.Uint64(k[1:])

			bm := roaring.New()
			if _, err := bm.FromBuffer(v); err != nil {
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
