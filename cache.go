package updog

import (
	"container/list"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
)

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

func newLRUCache(maxSize uint64) *lruCache {
	return &lruCache{
		entries: make(map[uint64]*list.Element),
		lruList: list.New(),
		maxSize: maxSize,
	}
}

type lruCache struct {
	entries map[uint64]*list.Element
	lruList *list.List

	curSize uint64
	maxSize uint64
}

func (c *lruCache) Get(key uint64) (*roaring.Bitmap, bool) {
	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}

	c.lruList.MoveToFront(elem)

	item := elem.Value.(*lruCacheItem)

	return item.bm, true
}

func (c *lruCache) Put(key uint64, bm *roaring.Bitmap) {
	if elem, ok := c.entries[key]; ok {
		c.lruList.MoveToFront(elem)
		item := elem.Value.(*lruCacheItem)
		item.bm = bm
		return
	}

	item := &lruCacheItem{
		key:  key,
		size: bm.GetSizeInBytes(),
		bm:   bm,
	}

	c.entries[key] = c.lruList.PushFront(item)

	c.curSize += item.size + uint64(lruCacheItemSize) + uint64(listElementSize)

	for c.curSize > c.maxSize && c.lruList.Len() > 0 {
		item := c.lruList.Remove(c.lruList.Back()).(*lruCacheItem)
		c.curSize -= item.size + uint64(lruCacheItemSize) + uint64(listElementSize)
		delete(c.entries, item.key)
	}
}

var (
	lruCacheItemSize = unsafe.Sizeof(lruCacheItem{})
	listElementSize  = unsafe.Sizeof(list.Element{})
)

type lruCacheItem struct {
	key  uint64
	size uint64
	bm   *roaring.Bitmap
}
