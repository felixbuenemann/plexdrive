package chunk

import (
	"fmt"
	"os"
	"syscall"

	. "github.com/claudetech/loggo/default"
)

// BufferPool manages a pool of buffers
type BufferPool struct {
	pool chan []byte
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(size int, bufferSize int64, chunkFilePath string) (*BufferPool, error) {
	if size <= 1 {
		panic("Invalid buffer pool size")
	}
	bp := &BufferPool{
		pool: make(chan []byte, size),
	}
	// Non-empty string in chunkFilePath enables MMAP disk storage for chunks
	if chunkFilePath != "" {
		chunkFile, err := os.OpenFile(chunkFilePath, os.O_RDWR|os.O_CREATE, 0600)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not open chunk cache file")
		}
		err = chunkFile.Truncate(int64(size) * bufferSize)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not resize chunk cache file")
		}
		Log.Infof("Created buffer pool cache file %v", chunkFile.Name())
		for i := 0; i < size; i++ {
			buffer, err := syscall.Mmap(int(chunkFile.Fd()), int64(i)*bufferSize, int(bufferSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
			if err != nil {
				return nil, err
			}
			bp.pool <- buffer
		}
	} else {
		for i := 0; i < size; i++ {
			bp.pool <- make([]byte, bufferSize, bufferSize)
		}
	}
	Log.Debugf("Initialized buffer pool with %v * %v Byte slots", size, bufferSize)
	return bp, nil
}

// Get a buffer from the pool
func (bp *BufferPool) Get() []byte {
	select {
	case buffer := <-bp.pool:
		Log.Tracef("Buffer pool usage %v / %v (get)", bp.used(), bp.size())
		return buffer
	default:
		Log.Fatalf("Buffer pool usage %v / %v (blocking get)", bp.used(), bp.size())
		panic("Buffer pool blocking during Get")
	}
}

// Put a buffer into the pool
func (bp *BufferPool) Put(buffer []byte) {
	buffer = buffer[:cap(buffer)]
	select {
	case bp.pool <- buffer:
		Log.Tracef("Buffer pool usage %v / %v (put)", bp.used(), bp.size())
	default:
		Log.Fatalf("Buffer pool usage %v / %v (blocking put)", bp.used(), bp.size())
		panic("Buffer pool blocking during Put")
	}
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
