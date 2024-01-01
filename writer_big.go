package updog

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"go.etcd.io/bbolt"
)

func NewBigIndexWriter(db *bbolt.DB, tempDB *bbolt.DB) (*BigIndexWriter, error) {
	idx := &BigIndexWriter{
		schema: &schema{
			Columns: make(map[string]*column),
		},
		db:     db,
		tempDB: tempDB,
	}

	if err := idx.tempDB.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("temp"))
		return err
	}); err != nil {
		return nil, err
	}

	return idx, nil
}

type BigIndexWriter struct {
	mtx sync.Mutex

	schema    *schema
	db        *bbolt.DB
	tempDB    *bbolt.DB
	nextRowID uint32
}

func (idx *BigIndexWriter) AddRow(tx *bbolt.Tx, values map[string]string) (uint32, error) {
	idx.mtx.Lock()
	defer idx.mtx.Unlock()

	rowID := idx.nextRowID
	defer func() {
		idx.nextRowID++
	}()

	for k, v := range values {
		valueIdx := idx.schema.add(k, v)

		var key [12]byte

		binary.BigEndian.PutUint64(key[:8], valueIdx)
		binary.BigEndian.PutUint32(key[8:], rowID)

		bucket := tx.Bucket([]byte("temp"))

		if err := bucket.Put(key[:], []byte{}); err != nil {
			return 0, err
		}
	}

	return rowID, nil
}

func (idx *BigIndexWriter) Flush() error {
	return idx.tempDB.View(func(tempTx *bbolt.Tx) error {
		return idx.db.Update(func(tx *bbolt.Tx) error {
			tempBucket := tempTx.Bucket([]byte("temp"))

			dataBucket, err := tx.CreateBucketIfNotExists([]byte("data"))
			if err != nil {
				return err
			}

			var (
				currentValueIdx uint64
				bm              *roaring.Bitmap
			)

			// iterate over all keys in the temp bucket and decode the keys,
			// create bitmaps from them, set values, and write bitmaps to data bucket.
			cursor := tempBucket.Cursor()

			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				if len(k) != 12 {
					return fmt.Errorf("invalid temp key found with length %d", len(k))
				}

				valueIdx := binary.BigEndian.Uint64(k[:8])
				rowID := binary.BigEndian.Uint32(k[8:])

				if currentValueIdx != valueIdx {
					// bm == nil indicates that this is for the first valueIdx, so we don't need to do
					// a full rotate yet.
					if bm != nil {
						var keyBuf [8]byte

						binary.BigEndian.PutUint64(keyBuf[:], currentValueIdx)

						bm.RunOptimize()
						valueBuf, err := bm.ToBytes()
						if err != nil {
							return err
						}

						if err := dataBucket.Put(append(keyPrefixValue, keyBuf[:]...), valueBuf); err != nil {
							return err
						}
					}

					currentValueIdx = valueIdx
					bm = roaring.New()
				}

				bm.Add(rowID)
			}

			// write last bitmap to data bucket:
			if bm != nil {
				var keyBuf [8]byte

				binary.BigEndian.PutUint64(keyBuf[:], currentValueIdx)

				bm.RunOptimize()
				valueBuf, err := bm.ToBytes()
				if err != nil {
					return err
				}

				if err := dataBucket.Put(append(keyPrefixValue, keyBuf[:]...), valueBuf); err != nil {
					return err
				}
			}

			// write nextRowID to data bucket:
			var rowIDbuf [4]byte

			binary.BigEndian.PutUint32(rowIDbuf[:], idx.nextRowID)

			if err := dataBucket.Put(keyNextRowID, rowIDbuf[:]); err != nil {
				return err
			}

			// write schema to data bucket:
			var buf bytes.Buffer

			if err := gob.NewEncoder(&buf).Encode(idx.schema); err != nil {
				return err
			}

			if err := dataBucket.Put(keySchema, buf.Bytes()); err != nil {
				return err
			}

			return nil
		})
	})
}
