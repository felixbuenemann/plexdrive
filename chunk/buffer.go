package chunk

import (
	"bytes"
	"sync"
	"sync/atomic"

	. "github.com/claudetech/loggo/default"
)

var bufPoolSize uint64

var bufPool = sync.Pool{
	New: func() interface{} {
		id := atomic.AddUint64(&bufPoolSize, 1)
		Log.Debugf("Allocate buffer %v", id)
		return &Buffer{id: id}
	},
}

// Buffer is a managed memory buffer with a reference counter
type Buffer struct {
	id uint64
	bytes.Buffer
	refs int64
}

// NewBuffer gets a buffer from the pool
func NewBuffer() *Buffer {
	return bufPool.Get().(*Buffer)
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
		bufPool.Put(b)
	}
}
