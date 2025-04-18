package kvcache

import (
	"errors"
	"sync/atomic"
	"unsafe"
)
const ENTRY_HDR_SIZE = 24

var (
	ErrLargeKey   = errors.New("The key is larger than 65535")
	ErrLargeEntry = errors.New("the entry is larger than 1/1024 of cache size")
	ErrNotFound = errors.New("Entry not found")
)

type entryPtr struct {
	offset   int64
	hash16   int16
	keylen   int16
	reserved int32
}

type entryHdr struct {
	accessTime uint32
	expireAt   uint32
	keyLen     uint16
	hash16     uint16
	valCap     uint32
	valLen     uint32
	deleted    bool
	slotId     uint8
	reserved   uint16
}

type segement struct {
	//主要字段
	rb        RingBuffer //环形缓冲
	slotsData []entryPtr //储存全部256个槽的entryPtr指针
	slotsLen  [256]int32 //256个槽各自的实际大小
	slotCap   int32      //一个槽的最大容量
	segId     int        //当前segment的id
	vacuumLen int64      //当前segement的ringbuffer中还有的非分配的字节长度

	//辅助计数字段
	missCount  int64
	hitCount   int64
	entryCount int64 //ringbuffer中entry实际数量
	totalCount int64 //ringbuffer中entry实际数量和逻辑删除数量的总和
	totalTime  int64 //所有查找时间的总和
	timer      Timer //获取系统时间的计数器

}

func NewSegement(bufsize int, segId int, timer Timer) (seg segement) {
	seg.segId = segId
	seg.rb = NewRingBuffer(bufsize, 0)
	seg.vacuumLen = int64(bufsize)
	seg.slotCap = 1
	seg.timer = timer
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	return
}

func (seg *segement) set(key, value []byte, hashVal uint64, expireSeconds int) (err error) {
	if len(key) > 65535 {
		return ErrLargeKey	
	}
	maxKeyValueLen := seg.rb.Size() / 4 - ENTRY_HDR_SIZE
	if len(key) + len(value) > maxKeyValueLen {
		return ErrLargeEntry
	}

	now := seg.timer.Now()
	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}

	hash16 := uint16(hashVal >> 16)
	slotId := uint8(hashVal >> 8)

	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)

	var entryHdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&entryHdrBuf[0]))

	if match {
		ptr := &slot[idx]
		seg.rb.ReadAt(entryHdrBuf[:], ptr.offset)

		hdr.valLen = uint32(len(value))
		hdr.expireAt = expireAt
		oldAccessTime := hdr.accessTime
		hdr.accessTime = now
		if uint32(len(value)) <= hdr.valCap {
			//如果新的value可以直接写入旧的entry中
			seg.rb.WriteAt(entryHdrBuf[:], ptr.offset)
			seg.rb.WriteAt(value, ptr.offset + ENTRY_HDR_SIZE + int64(hdr.keyLen))
			atomic.AddInt64(&seg.totalTime, int64(now-oldAccessTime))
			return
		}
		//如果新的value无法直接写入旧的entry中，需要重新分配一个entry
		seg.delEntryPtr(slotId, slot, idx)
		for hdr.valCap < uint32(len(value)) {
			hdr.valCap *= 2
		}
		if hdr.valCap > uint32(maxKeyValueLen - len(key)) {
			hdr.valCap = uint32(maxKeyValueLen - len(key))
		}
	} else {
		hdr.hash16 = hash16
		hdr.expireAt = expireAt
		hdr.accessTime = now
		hdr.slotId = slotId
		hdr.keyLen = uint16(len(key))
		hdr.valLen = uint32(len(value))
		hdr.valCap = uint32(len(value))
		if hdr.valCap == 0 {
			hdr.valCap = 1
		}
	}

	entryLen := ENTRY_HDR_SIZE + int(hdr.valCap) + int(hdr.keyLen)
	slotsModified := seg.evacuate(int64(entryLen), slotId, now)
	if slotsModified {
		//如果有槽内的entryPtr被删除了，需要重新查找
		slot = seg.getSlot(slotId)
		idx, match = seg.lookup(slot, hash16, key)
	}
	seg.insertEntryPtr(slotId, idx, int16(hdr.hash16), seg.rb.End(), hdr.keyLen)
	seg.rb.Append(entryHdrBuf[:])
	seg.rb.Append(key)
	seg.rb.Append(value)
	seg.rb.Skip(int64(hdr.valCap - hdr.valLen))
	//set函数里非原地修改的变量都是新加的方式加到环形队列中去的
	atomic.AddInt64(&seg.totalCount, 1)
	atomic.AddInt64(&seg.totalTime, int64(now))
	seg.vacuumLen -= int64(entryLen)
	return
}

func (seg *segement) get(key, buf []byte, hashVal uint64, peek bool) (value []byte, expireAt uint32, err error) {
	return nil, 0, nil
}

func (seg *segement) del(key []byte, hashVal uint64) (affected bool) {

	hash16 := uint16(hashVal >> 16)
	slotId := uint8(hashVal >> 8)

	slots := seg.getSlot(slotId)
	idx , match := seg.lookup(slots, hash16, key)
	if !match {
		return false
	}
	seg.delEntryPtr(slotId, slots, idx)
	return true
}

// 修改过期时间
func (seg *segement) touch(key []byte, hashVal uint64, expireSeconds int) (err error) {
	return nil
}

func (seg *segement) ttl(key []byte, hashVal uint64) (timeLeft uint32, err error) {
	return 0, nil
}

func (seg *segement) locate(key []byte, hashVal uint64, peek bool) (hdrEntry entryHdr, ptrOffset int64, err error) {
	return
}

func (seg *segement) evacuate(entryLen int64, slotId uint8, now uint32) (slotModified bool) {
	//联系数据迁移的次数
	evacuateCount := 0
	var entryHdrbuf [ENTRY_HDR_SIZE]byte

	for seg.vacuumLen < entryLen {
		//得到最老的entry得偏移量
		oldOff := seg.rb.End() + seg.vacuumLen - int64(seg.rb.Size())
		hdr := (*entryHdr)(unsafe.Pointer(&entryHdrbuf[0]))
		seg.rb.ReadAt(entryHdrbuf[:], oldOff)
		oldEntryLen := ENTRY_HDR_SIZE + int(hdr.valCap) + int(hdr.keyLen)
		//如果是逻辑删除的entry，直接跳过
		if hdr.deleted {
			evacuateCount = 0
			atomic.AddInt64(&seg.vacuumLen, int64(oldEntryLen))
			atomic.AddInt64(&seg.totalCount, -1)
			atomic.AddInt64(&seg.totalTime, -int64(hdr.accessTime))
			continue
		}

		now := seg.timer.Now()
		expired := isExpired(hdr.expireAt, now)
		fewAccessed := int64(hdr.accessTime) * int64(seg.totalCount) < seg.totalTime
		if expired || fewAccessed || evacuateCount > 5 {
			//直接当场删除
			seg.delEntryPtrByOffset(slotId, hdr.hash16, oldOff)
			if hdr.slotId == slotId {
				//表示给定的槽内有entryPtr被删除了，该entryPtr后面的entryPtr的下表都会发生变动
				slotModified = true
			}
			evacuateCount = 0
			atomic.AddInt64(&seg.vacuumLen, int64(oldEntryLen))
			atomic.AddInt64(&seg.totalCount, -1)
			atomic.AddInt64(&seg.totalTime, -int64(hdr.accessTime))
		} else {
			//当前entry不能删除，将该数据重新写入
			newoff := seg.rb.Evacuate(oldOff, oldEntryLen)
			seg.updateEntryPtr(hdr.slotId, hdr.hash16, oldOff, newoff)
			evacuateCount++
		}
	}
	return
}

func (seg *segement) expand() {
	newslotsData := make([]entryPtr, seg.slotCap * 2 * 256)
	for i := 0; i < 256; i++ {
		off := int32(i) * seg.slotCap
		copy(newslotsData[off * 2:], seg.slotsData[off : off + seg.slotsLen[i]])
	}
	seg.slotsData = newslotsData
	seg.slotCap *= 2
}

func (seg *segement) updateEntryPtr(slotId uint8, hash16 uint16, oldOff, newOff int64) {

}

func (seg *segement) insertEntryPtr(slotId uint8, idx int, hash16 int16, offset int64, keyLen uint16) {
	


}

func (seg *segement) delEntryPtr(slotId uint8, slots []entryPtr, idx int) {
	var entryPtrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&entryPtrBuf[0]))
	seg.rb.ReadAt(entryPtrBuf[:], slots[idx].offset)
	hdr.deleted = true
	seg.rb.WriteAt(entryPtrBuf[:], slots[idx].offset)
	copy(slots[idx:], slots[idx + 1:])	
	//对segment内计数进行调整
	atomic.AddInt64(&seg.entryCount, -1)
	seg.slotsLen[slotId]--
}

func (seg *segement) delEntryPtrByOffset(slotId uint8, hash16 uint16, offset int64) {

}

func entryPtrIdx(slot []entryPtr, hash16 uint16) (idx int) {
	return
}

func (seg *segement) lookup(slot []entryPtr, hash16 uint16, key []byte) (idx int, match bool) {
	return
}

func (seg *segement) lookupByOff(slot []entryPtr, hash16 uint16, offset int64) (idx int, match bool) {
	return
}

func (seg *segement) getSlot(slotId uint8) []entryPtr {
	off := int32(slotId) * seg.slotCap
	return seg.slotsData[off : off+seg.slotsLen[slotId] : off+seg.slotCap]
}

func isExpired(keyExpireAt, now uint32) bool {
	return keyExpireAt != 0 && keyExpireAt <= now
}