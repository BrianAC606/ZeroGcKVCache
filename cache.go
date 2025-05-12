package kvcache

import (
	"fmt"
	"sync"
)

const (
	segmentCount    = 256
	segmentAndOpVal = 255
	minBufSize      = 512 * 1024
)

type Cache struct {
	segments [segmentCount]segment
	locks    [segmentCount]sync.Mutex
}

func NewCache(size int) (cache *Cache) {
	if size < minBufSize {
		size = minBufSize
	}
	cache = &Cache{}
	for i := 0; i < segmentCount; i++ {
		//cache.segments[i] = NewSegment(size / segmentCount, i, )
	}

	return
}

func (cache *Cache) Set(key, value []byte, expireSeconds int) (err error) {
	return
}


func (cache *Cache) Get(key []byte) (val []byte) {
	return
}

func (cache *Cache) Touch(key []byte, expireSeconds int) (err error) {
	return
}

func (cache *Cache) GetOrSet(key, value []byte, expireSeconds int) (retVal []byte, err error) {
	return
}

func (cache *Cache) SetAndGet(key, value []byte, expireSeconds int) (retVal []byte, found bool, err error) {
	return
}

func (cache *Cache) Update(key []byte) {}

func (cache *Cache) Peek(key []byte) (value []byte, err error) {

	return
}
