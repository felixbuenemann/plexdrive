package chunk

import (
	"io"
	"sync"
	"sync/atomic"

	. "github.com/claudetech/loggo/default"
)

// BufferPool manages a pool of buffers
type BufferPool struct {
	limit int64
	size  int64
	used  int64
	pool  sync.Pool
	full  *sync.Cond
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(size int, bufferSize int64) *BufferPool {
	if size <= 1 {
		panic("Invalid buffer pool size")
	}
	bp := new(BufferPool)
	bp.limit = int64(size)
	bp.pool = sync.Pool{
		New: func() interface{} {
			id := atomic.AddInt64(&bp.size, 1)
			Log.Debugf("Allocate buffer %v", id)
			bytes := make([]byte, bufferSize)
			return &Buffer{bytes, id, 0, bp}
		},
	}
	bp.full = sync.NewCond(new(sync.Mutex))
	Log.Debugf("Initialized buffer pool with %v %v B slots", size, bufferSize)
	return bp
}

// Get a buffer from the pool
func (bp *BufferPool) Get() *Buffer {
	bp.full.L.Lock()
	if bp.used >= bp.limit {
		Log.Debugf("Buffer pool usage %v / %v (%v) (wait)", bp.used, bp.limit, bp.size)
	}
	for bp.used >= bp.limit {
		bp.full.Wait()
	}
	buffer := bp.pool.Get().(*Buffer)
	used := atomic.AddInt64(&bp.used, 1)
	Log.Debugf("Buffer pool usage %v / %v (%v) (get)", used, bp.limit, bp.size)
	bp.full.Broadcast()
	bp.full.L.Unlock()
	return buffer
}

// Put a buffer into the pool
func (bp *BufferPool) Put(buffer *Buffer) {
	bp.pool.Put(buffer)
	bp.full.L.Lock()
	used := atomic.AddInt64(&bp.used, -1)
	Log.Debugf("Buffer pool usage %v / %v (%v) (put)", used, bp.limit, bp.size)
	bp.full.Broadcast()
	bp.full.L.Unlock()
}

// Buffer is a managed memory buffer with a reference counter
type Buffer struct {
	bytes []byte
	id    int64
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
		Log.Debugf("Return buffer %v", b.id)
		b.pool.Put(b)
	}
}
