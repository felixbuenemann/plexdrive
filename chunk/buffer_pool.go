package chunk

import (
	. "github.com/claudetech/loggo/default"
)

// BufferPool manages a pool of buffers
type BufferPool struct {
	pool chan []byte
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(size int, bufferSize int64) *BufferPool {
	if size <= 1 {
		panic("Invalid buffer pool size")
	}
	bp := &BufferPool{
		pool: make(chan []byte, size),
	}
	for i := 0; i < size; i++ {
		bp.pool <- make([]byte, bufferSize, bufferSize)
	}
	Log.Debugf("Initialized buffer pool with %v %v B slots", size, bufferSize)
	return bp
}

// Get a buffer from the pool
func (bp *BufferPool) Get() []byte {
	if bp.used() == bp.size() {
		Log.Debugf("Buffer pool usage %v / %v (wait)", bp.used(), bp.size())
	}
	buffer := <-bp.pool
	Log.Debugf("Buffer pool usage %v / %v (get)", bp.used(), bp.size())
	return buffer
}

// Put a buffer into the pool
func (bp *BufferPool) Put(buffer []byte) {
	buffer = buffer[:cap(buffer)]
	bp.pool <- buffer
	Log.Debugf("Buffer pool usage %v / %v (put)", bp.used(), bp.size())
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
