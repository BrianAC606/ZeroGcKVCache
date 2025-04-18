package kvcache

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

type RingBuffer interface {
	ReadAt([]byte, int64) (int, error)  //读取某位置上的值
	WriteAt([]byte, int64) (int, error) //写入某位置上的值
	Append([]byte) (int, error)         //将将值追加到缓存末尾

	EqualAt([]byte, int64) bool //比较某位置上的值是否相等
	Evacuate(int64, int) int64  //将某位置上的值重新写入缓存

	Size() int      //获取缓存大小
	Reset(int64)    //重置缓存
	ReSize(int)     //重新设置缓存大小
	Skip(int64)     //跳过(预留)一段内存
	Begin() int64   //获取缓存起始位置
	End() int64     //获取缓存结束位置
	String() string //获取缓存信息

	Dump() []byte                       //备份缓存数据
	Slice(int64, int64) ([]byte, error) //获取缓存数据的某一部分
}

var ErrorOutOfRange = errors.New("out of range")

// 将逻辑地址和物理地址分开，能够支持上层应用对更大地址进行操作，同时下层缓存能够对上层透明的调整大小
type ringBuffer struct {
	data  []byte
	index int
	begin int64
	end   int64
}

func NewRingBuffer(size int, begin int64) RingBuffer {
	rb := &ringBuffer{
		data:  make([]byte, size),
	}
	rb.Reset(begin)
	return rb	
}

func (rb *ringBuffer) ReadAt(b []byte, off int64) (n int, err error) {
	if off < rb.begin || off >= rb.end {
		return 0, ErrorOutOfRange
	}

	readBegin := rb.getDataOff(off)
	readEnd := readBegin + int(rb.end-off)
	if readEnd <= len(rb.data) {
		n = copy(b, rb.data[readBegin:readEnd])
	} else {
		n = copy(b, rb.data[readBegin:])
		if n < len(b) {
			n += copy(b[n:], rb.data[:(len(b)-n)])
		}
	}
	if n < len(b) {
		err = io.EOF
	}
	return
}

func (rb *ringBuffer) WriteAt(b []byte, off int64) (n int, err error) {
	if off < rb.begin || off > rb.end {
		return 0, ErrorOutOfRange
	}
	writeBegin := rb.getDataOff(off)
	writeEnd := writeBegin + int(rb.end-off)
	if writeEnd <= len(rb.data) {
		n = copy(rb.data[writeBegin:writeEnd], b)
	} else {
		n = copy(rb.data[writeBegin:], b)
		if n < len(b) {
			n += copy(rb.data[:(len(b)-n)], b[n:])
		}
	}
	if n < len(b) {
		err = io.ErrShortWrite
	}
	return 0, nil
}

func (rb *ringBuffer) Append(b []byte) (n int, err error) {
	if len(b) > len(rb.data) {
		err = ErrorOutOfRange
		return
	}
	n = copy(rb.data[rb.index:], b)
	rb.index += n
	if n < len(b) {
		rb.index = copy(rb.data, b[n:])
		n += rb.index
		if n < len(b) {
			err = io.ErrShortWrite
			return
		}
	}
	rb.end += int64(n)
	if int(rb.end-rb.begin) > len(rb.data) {
		rb.begin = rb.end - int64(len(rb.data))
	}
	return
}

func (rb *ringBuffer) EqualAt(b []byte, off int64)(equal bool) {
	 if off < rb.begin || off + int64(len(b)) > rb.end {
		return
	 }
	 dataBegin := rb.getDataOff(off)
	 dataEnd := dataBegin + len(b)
	 if dataEnd <= len(rb.data) {
		equal = bytes.Equal(rb.data[dataBegin:dataEnd], b)
	 } else {
		firstPart := len(rb.data) - dataBegin
		equal = bytes.Equal(rb.data[dataBegin:], b[:firstPart])
		equal = bytes.Equal(rb.data[:len(b) - firstPart], b[firstPart:])
	 }
	 return
}

func (rb *ringBuffer) Evacuate(off int64, length int) (newoff int64) {
	 if off < rb.begin || off + int64(length)  > rb.end {
		return -1
	 }
	 /* 这里需要两次拷贝，效率较低
	 data := make([]byte, length)
	 n,_ := rb.ReadAt(data, off)
	 if n != length {
		return -1
	 }
	 rb.Append(data)
	 return rb.end - int64(length)
	 */

	 //该方法只需要一次拷贝，大大提升数据迁移的效率
	 readbegin := rb.getDataOff(off)
	 if readbegin == rb.index {
		//index之后是空白数据，如果index之后有数据可读，即开始覆盖写了
		rb.index = (rb.index + length) % len(rb.data)
	 } else {
		readend := readbegin + length
		var n int
		if readend < len(rb.data) {
			n = copy(rb.data[rb.index:], rb.data[readbegin:readend])
			if n < length {
				rb.index = copy(rb.data, rb.data[readbegin + n : readend])
			} 
		} else { // 如果index要发生回环，则前半段必然大于readbegin到数组末尾的长度
			n = copy(rb.data[rb.index:], rb.data[readbegin:])
			rb.index += n
			n2 := copy(rb.data[rb.index:], rb.data[:length - n])
			if n + n2 < length {
				rb.index = copy(rb.data, rb.data[n2 : length - n])
			}
		}
		newoff = rb.end
		rb.end += int64(length)
		if int(rb.end - rb.begin) > len(rb.data) {
			rb.begin = rb.end - int64(len(rb.data))
		}
	 }
	 return
}

func (rb *ringBuffer) Size() int {
	return len(rb.data)
}

func (rb *ringBuffer) Reset(off int64) {
	rb.index = 0
	rb.begin = off
	rb.end = off
}

func (rb *ringBuffer) ReSize(size int) {
	if size == len(rb.data) {
		return
	}
	newbuf := make([]byte, size)
	if size < len(rb.data) {
		n := copy(newbuf, rb.data[rb.index:])
		if n < size {
			copy(newbuf[n:], rb.data)
		}
	} else {
		n := copy(newbuf, rb.data[rb.index:])
		if n < len(rb.data) {
			copy(newbuf[n:], rb.data[:rb.index])
		}
	}
	rb.index = 0
}

func (rb *ringBuffer) Skip(length int64) {
	rb.end += length
	rb.index = (rb.index + int(length)) % len(rb.data)
	if int(rb.end-rb.begin) > len(rb.data) {
		rb.begin = rb.end - int64(len(rb.data))
	}
}

func (rb *ringBuffer) Begin() int64 {
	return rb.begin
}

func (rb *ringBuffer) End() int64 {
	return rb.end
}

func (rb *ringBuffer) String() string {
	return fmt.Sprintf("ringBuffer size:%d, begin:%d, end:%d, index:%d", len(rb.data), rb.begin, rb.end, rb.index)
}

func (rb *ringBuffer) Dump() []byte {
	dumpData := make([]byte, 0, len(rb.data))
	copy(dumpData, rb.data)
	return dumpData
}

func (rb *ringBuffer) Slice(start, end int64) (slice []byte,err error) {
	if start < rb.begin || end > rb.end {
		return nil, ErrorOutOfRange
	}
	dataBegin := rb.getDataOff(start)
	dataEnd := dataBegin + int(end - start)
	if dataEnd < len(rb.data) {
		slice = rb.data[dataBegin:dataEnd]
	} else {
		slice = make([]byte, dataEnd - dataBegin)
		n := copy(slice, rb.data[dataBegin:])
		copy(slice[n:], rb.data)
	}
	return 
}

func (rb *ringBuffer) getDataOff(off int64) (dataoff int) {
	if int(rb.end-rb.begin) < len(rb.data) {
		dataoff = int(off-rb.begin) % len(rb.data)
	} else {
		dataoff = (rb.index + int(off-rb.begin)) % len(rb.data)
	}
	return
}
