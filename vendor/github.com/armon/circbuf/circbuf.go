package circbuf

import (
	"fmt"
)

type Buffer struct {
	data        []byte
	size        int64
	writeCursor int64
	written     int64
}

func NewBuffer(size int64) (*Buffer, error) {
	if size <= 0 {
		return nil, fmt.Errorf("Size must be positive")
	}

	b := &Buffer{
		size: size,
		data: make([]byte, size),
	}
	return b, nil
}

func (b *Buffer) Write(buf []byte) (int, error) {

	n := len(buf)
	b.written += int64(n)

	if int64(n) > b.size {
		buf = buf[int64(n)-b.size:]
	}

	remain := b.size - b.writeCursor
	copy(b.data[b.writeCursor:], buf)
	if int64(len(buf)) > remain {
		copy(b.data, buf[remain:])
	}

	b.writeCursor = ((b.writeCursor + int64(len(buf))) % b.size)
	return n, nil
}

func (b *Buffer) Size() int64 {
	return b.size
}

func (b *Buffer) TotalWritten() int64 {
	return b.written
}

func (b *Buffer) Bytes() []byte {
	switch {
	case b.written >= b.size && b.writeCursor == 0:
		return b.data
	case b.written > b.size:
		out := make([]byte, b.size)
		copy(out, b.data[b.writeCursor:])
		copy(out[b.size-b.writeCursor:], b.data[:b.writeCursor])
		return out
	default:
		return b.data[:b.writeCursor]
	}
}

func (b *Buffer) Reset() {
	b.writeCursor = 0
	b.written = 0
}

func (b *Buffer) String() string {
	return string(b.Bytes())
}
