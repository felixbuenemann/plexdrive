package chunk

import (
	"bytes"
	"sync"
	"sync/atomic"

	. "github.com/claudetech/loggo/default"
)

// BufferPool manages a pool of buffers
type BufferPool struct {
	size int64
	used int64
	pool sync.Pool
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(bufferSize int64) *BufferPool {
	bp := new(BufferPool)
	bp.pool = sync.Pool{
		New: func() interface{} {
			id := atomic.AddInt64(&bp.size, 1)
			Log.Debugf("Allocate buffer %v", id)
			buffer := bytes.NewBuffer(make([]byte, bufferSize))
			return &Buffer{*buffer, id, 0, bp}
		},
	}
	return bp
}

// Get a buffer from the pool
func (bp *BufferPool) Get() *Buffer {
	used := atomic.AddInt64(&bp.used, 1)
	Log.Debugf("Buffer pool usage %v / %v (get)", used, bp.size)
	return bp.pool.Get().(*Buffer)
}

// Put a buffer into the pool
func (bp *BufferPool) Put(buffer *Buffer) {
	used := atomic.AddInt64(&bp.used, -1)
	Log.Debugf("Buffer pool usage %v / %v (put)", used, bp.size)
	bp.pool.Put(buffer)
}

// Buffer is a managed memory buffer with a reference counter
type Buffer struct {
	bytes.Buffer
	id   int64
	refs int64
	pool *BufferPool
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
		Log.Debugf("Release buffer %v", b.id)
		b.Reset()
		b.pool.Put(b)
	}
}
