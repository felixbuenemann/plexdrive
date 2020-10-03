package chunk

import (
	"sync"
	"syscall"

	. "github.com/claudetech/loggo/default"
)

// MMapStorage caches chunks using BoltDB
type MMapStorage struct {
	chunkSize int
	chunks    map[string][]byte
	stack     *Stack
	lock      sync.RWMutex
}

func NewMMapStorage(chunkSize int64, maxChunks int) (Storage, error) {
	storage := MMapStorage{
		chunkSize: int(chunkSize),
		chunks:    make(map[string][]byte, maxChunks),
		stack:     NewStack(maxChunks),
	}
	return &storage, nil
}

// Check if a chunk exists in the database
func (s *MMapStorage) Check(id string) (exists bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if _, exists := s.chunks[id]; exists {
		s.stack.Touch(id)
		return true
	}
	return false
}

// Load a chunk from RAM
func (s *MMapStorage) Load(id string) ([]byte, func()) {
	s.lock.RLock()
	if chunk, exists := s.chunks[id]; exists {
		// Log.Debugf("Loaded chunk %v (found)", id)
		s.stack.Touch(id)
		return chunk, s.lock.RUnlock
	}
	s.lock.RUnlock()
	Log.Warningf("Loaded chunk %v (missing)", id)
	return nil, nil
}

// Store stores a chunk
func (s *MMapStorage) Store(id string, bytes []byte) (err error) {
	// Avoid storing same chunk multiple times
	if s.Check(id) {
		// Log.Debugf("Create chunk %v %v / %v (exists)", id, s.stack.Len(), s.stack.MaxSize)
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	usedChunks := s.stack.Len()
	maxChunks := s.stack.MaxSize

	var chunk []byte
	deleteID := s.stack.Pop()
	if "" != deleteID {
		chunk = s.chunks[deleteID]
		delete(s.chunks, deleteID)
		Log.Debugf("Create chunk %v / Delete chunk %v %v/%v (reused)", id, deleteID, usedChunks, maxChunks)
	} else {
		chunk, err = syscall.Mmap(-1, 0, int(s.chunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			Log.Errorf("Create chunk %v %v/%v (failed)", id, usedChunks, maxChunks)
			return
		}
		Log.Debugf("Create chunk %v %v/%v (stored)", id, usedChunks, maxChunks)
	}

	copy(chunk, bytes)
	s.chunks[id] = chunk
	s.stack.Push(id)

	return nil
}

// Clear removes all old chunks on disk
func (s *MMapStorage) Clear() error {
	return nil
}

// Close deallocates all chunks
func (s *MMapStorage) Close() error {
	Log.Debugf("Deallocating %v mmap regions...", len(s.chunks))
	for id, chunk := range s.chunks {
		if err := syscall.Munmap(chunk); nil != err {
			Log.Errorf("Failed to unmap chunk %v", id)
		}
	}
	return nil
}
