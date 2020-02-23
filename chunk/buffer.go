package chunk

import (
	"io"
	"sync/atomic"

	. "github.com/claudetech/loggo/default"
)

// BufferPool manages a pool of buffers
type BufferPool struct {
	pool chan *Buffer
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(size int, bufferSize int64) *BufferPool {
	if size <= 1 {
		panic("Invalid buffer pool size")
	}
	bp := &BufferPool{
		pool: make(chan *Buffer, size),
	}
	for i := 0; i < size; i++ {
		bp.pool <- bp.newBuffer(bufferSize)
	}
	Log.Debugf("Initialized buffer pool with %v %v B slots", size, bufferSize)
	return bp
}

// Get a buffer from the pool
func (bp *BufferPool) Get() *Buffer {
	if bp.used() == bp.size() {
		Log.Debugf("Buffer pool usage %v / %v (wait)", bp.used(), bp.size())
	}
	buffer := <-bp.pool
	Log.Debugf("Buffer pool usage %v / %v (get)", bp.used(), bp.size())
	return buffer
}

// Put a buffer into the pool
func (bp *BufferPool) Put(buffer *Buffer) {
	bp.pool <- buffer
	Log.Debugf("Buffer pool usage %v / %v (put)", bp.used(), bp.size())
}

func (bp *BufferPool) newBuffer(size int64) *Buffer {
	id := bp.free()
	bytes := make([]byte, size)
	return &Buffer{bytes, id, 0, bp}
}

func (bp *BufferPool) size() int {
	return cap(bp.pool)
}

func (bp *BufferPool) free() int {
	return len(bp.pool)
}

func (bp *BufferPool) used() int {
	return bp.size() - bp.free()
}

// Buffer is a managed memory buffer with a reference counter
type Buffer struct {
	bytes []byte
	id    int
	refs  int64

	pool *BufferPool
}

// Bytes from the buffer
func (b *Buffer) Bytes() []byte {
	return b.bytes
}

// ReadFrom reader into the buffer
func (b *Buffer) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.ReadFull(r, b.bytes)
	if err == io.ErrUnexpectedEOF {
		err = nil // Ignore short reads
	}
	return int64(n), err
}

// Ref increases the reference count of the buffer
func (b *Buffer) Ref() {
	refs := atomic.AddInt64(&b.refs, 1)
	Log.Tracef("Buffer %v references %v", b.id, refs)
}

// Unref decreases the reference count of the buffer
func (b *Buffer) Unref() {
	refs := atomic.AddInt64(&b.refs, -1)
	Log.Tracef("Buffer %v references %v", b.id, refs)
	if refs < 0 {
		panic("Buffer has negative reference count")
	}
	if refs == 0 {
		Log.Tracef("Return buffer %v", b.id)
		b.pool.Put(b)
	}
}
