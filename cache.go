package updog

import (
	"container/list"
	"unsafe"

	"github.com/RoaringBitmap/roaring"
)

// Cache is the interface that needs to be fulfilled for a cache implementation.
type Cache interface {
	Get(key uint64) (bm *roaring.Bitmap, found bool)
	Put(key uint64, bm *roaring.Bitmap)
}

type nullCache struct{}

func (c *nullCache) Get(key uint64) (*roaring.Bitmap, bool) {
	return nil, false
}

func (c *nullCache) Put(key uint64, bm *roaring.Bitmap) {
}

type CounterMetric interface {
	Inc()
}

// CacheMetrics contains the counter metrics relating to a cache. It is currently mainly
// used in the LRUCache implementation.
type CacheMetrics struct {
	CacheHit  CounterMetric
	CacheMiss CounterMetric
	GetCall   CounterMetric
	PutCall   CounterMetric
}

// NewLRUCache returns a new LRUCache object.
func NewLRUCache(maxSizeBytes uint64, opts ...LRUCacheOption) *LRUCache {
	cache := &LRUCache{
		entries: make(map[uint64]*list.Element),
		lruList: list.New(),
		maxSize: maxSizeBytes,
		metrics: &CacheMetrics{},
	}

	for _, o := range opts {
		o(cache)
	}

	return cache
}

type LRUCacheOption func(c *LRUCache)

// LRUCache is a size-bounded cache with a LRU cache replacement policy. You
// have to use the NewLRUCache constructor function to create an instance of it.
type LRUCache struct {
	entries map[uint64]*list.Element
	lruList *list.List

	curSize uint64
	maxSize uint64

	metrics *CacheMetrics
}

// WithCacheMetrics is an option for LRUCache to set a CacheMetrics object.
func WithCacheMetrics(metrics *CacheMetrics) LRUCacheOption {
	return func(c *LRUCache) {
		c.metrics = metrics
	}
}

// Get returns the bitmap associated with the provided key, if available.
func (c *LRUCache) Get(key uint64) (*roaring.Bitmap, bool) {
	if c.metrics.GetCall != nil {
		c.metrics.GetCall.Inc()
	}

	elem, ok := c.entries[key]
	if !ok {
		if c.metrics.CacheMiss != nil {
			c.metrics.CacheMiss.Inc()
		}
		return nil, false
	}

	if c.metrics.CacheHit != nil {
		c.metrics.CacheHit.Inc()
	}

	c.lruList.MoveToFront(elem)

	item := elem.Value.(*lruCacheItem)

	return item.bm, true
}

// Put stores the provided bitmap under the provided key in the cache. It ensures
// that the maximum size of the LRU cache is kept, by evicting other cached elements
// if necessary.
func (c *LRUCache) Put(key uint64, bm *roaring.Bitmap) {
	if c.metrics.PutCall != nil {
		c.metrics.PutCall.Inc()
	}

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
