package kvcache

import (
	"unsafe"
)

type Iterator struct {
	cache      *Cache
	segmentIdx int
	slotIdx    int
	entryIdx   int
}

type Entry struct {
	Key      []byte
	Value    []byte
	ExpireAt uint32
}

func (it *Iterator) Next() *Entry {
	for it.segmentIdx < 256 {
		entry := it.nextForSegment(it.segmentIdx)
		if entry != nil {
			return entry
		}
		it.segmentIdx++
		it.slotIdx = 0
		it.entryIdx = 0
	}
	return nil
}

func (it *Iterator) nextForSegment(segIdx int) *Entry {
	it.cache.locks[segIdx].Lock()
	defer it.cache.locks[segIdx].Unlock()
	seg := &it.cache.segments[segIdx]
	for it.slotIdx < 256 {
		entry := it.nextForSlot(seg, it.slotIdx)
		if entry != nil {
			return entry
		}
		it.slotIdx++
		it.entryIdx = 0
	}
	return nil
}

func (it *Iterator) nextForSlot(seg *segment, slotId int) *Entry {
	slotOff := int32(it.slotIdx) * seg.slotCap
	slot := seg.slotsData[slotOff : slotOff+seg.slotsLen[it.slotIdx] : slotOff+seg.slotCap]
	for it.entryIdx < len(slot) {
		ptr := slot[it.entryIdx]
		it.entryIdx++
		now := seg.timer.Now()
		var hdrBuf [ENTRY_HDR_SIZE]byte
		seg.rb.ReadAt(hdrBuf[:], ptr.offset)
		hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
		if hdr.expireAt == 0 || hdr.expireAt > now {
			entry := new(Entry)
			entry.Key = make([]byte, hdr.keyLen)
			entry.Value = make([]byte, hdr.valLen)
			entry.ExpireAt = hdr.expireAt
			seg.rb.ReadAt(entry.Key, ptr.offset+ENTRY_HDR_SIZE)
			seg.rb.ReadAt(entry.Value, ptr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
			return entry
		}
	}
	return nil
}

func (cache *Cache) NewIterator() *Iterator {
	return &Iterator{
		cache: cache,
	}
}
