package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	. "github.com/claudetech/loggo/default"
)

// ErrTimeout is a timeout error
var ErrTimeout = errors.New("timeout")

var (
	pageSize   = int64(os.Getpagesize())
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
	crc64Table = crc64.MakeTable(crc64.ECMA)
)

// Storage is a chunk storage
type Storage struct {
	ChunkFile  *os.File
	ChunkSize  int64
	HeaderSize int64
	MaxChunks  int
	magic      uint64
	chunks     map[uint64]Chunk
	stack      *Stack
	lock       sync.RWMutex
	buffers    chan []byte
	loadChunks int
	signals    chan os.Signal
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int, chunkFilePath string) (*Storage, error) {
	s := Storage{
		ChunkSize: chunkSize,
		MaxChunks: maxChunks,
		chunks:    make(map[uint64]Chunk, maxChunks),
		stack:     NewStack(maxChunks),
		buffers:   make(chan []byte, maxChunks),
		signals:   make(chan os.Signal, 1),
	}

	// Non-empty string in chunkFilePath enables MMAP disk storage for chunks
	if chunkFilePath != "" {
		chunkFile, err := os.OpenFile(chunkFilePath, os.O_RDWR|os.O_CREATE, 0600)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not open chunk cache file")
		}
		stat, err := chunkFile.Stat()
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not stat chunk cache file")
		}
		currentSize := stat.Size()
		wantedSize := (pageSize + chunkSize) * int64(maxChunks)
		if currentSize != wantedSize {
			err = chunkFile.Truncate(wantedSize)
			if nil != err {
				Log.Debugf("%v", err)
				return nil, fmt.Errorf("Could not resize chunk cache file")
			}
		}
		s.magic = uint64(stat.ModTime().UnixNano() ^ chunkSize)
		Log.Infof("Created chunk cache file %v (magic:%v)", chunkFile.Name(), s.magic)
		s.ChunkFile = chunkFile
		s.HeaderSize = pageSize
		s.loadChunks = int(min(currentSize/(pageSize+chunkSize), int64(maxChunks)))
	}

	// Setup sighandler
	signal.Notify(s.signals, syscall.SIGINT, syscall.SIGTERM)

	// Initialize chunks
	if err := s.chunkLoader(); nil != err {
		return nil, err
	}

	return &s, nil
}

// chunkLoader mmaps buffers and enqueues them for verification
func (s *Storage) chunkLoader() error {
	start := time.Now()
	loadedChunks := 0
	for i := 0; i < s.MaxChunks; i++ {
		select {
		case sig := <-s.signals:
			Log.Warningf("Received signal %v, aborting chunk loader", sig)
			return fmt.Errorf("Aborted by signal")
		default:
			if loaded, err := s.initChunk(i); nil != err {
				Log.Errorf("Failed to allocate chunk %v: %v", i, err)
				return fmt.Errorf("Failed to initialize chunks")
			} else if loaded {
				loadedChunks++
			}
		}
	}
	elapsed := time.Since(start)
	throughput := float64(s.MaxChunks) * float64(s.ChunkSize>>20) / elapsed.Seconds()
	Log.Infof("Loaded %v/%v cache chunks in %v (%.2f MiB/s)", loadedChunks, s.MaxChunks, elapsed, throughput)
	return nil
}

// initChunk tries to restore a chunk from disk
func (s *Storage) initChunk(index int) (bool, error) {
	bytes, err := s.allocateChunk(index)
	if err != nil {
		Log.Debugf("%v", err)
		return false, err
	}

	var id uint64
	if index < s.loadChunks {
		id = binary.LittleEndian.Uint64(bytes)
	}

	if id == 0 {
		Log.Tracef("Allocate chunk %v/%v", index+1, s.MaxChunks)
		s.buffers <- bytes
		return false, nil
	}

	// Check if chunk exists first, could've been written while we verify
	s.lock.RLock()
	_, exists := s.chunks[id]
	s.lock.RUnlock()

	if exists {
		Log.Tracef("Load chunk %v/%v (exists)", index+1, s.MaxChunks)
		s.buffers <- bytes
		return false, nil
	}

	Log.Tracef("Load chunk %v/%v (restored)", index+1, s.MaxChunks)
	s.lock.Lock()
	bytes[24] = 0xFF
	s.chunks[id] = bytes
	s.stack.Push(id)
	s.lock.Unlock()

	return true, nil
}

// allocateChunk creates a new mmap-backed chunk
func (s *Storage) allocateChunk(index int) ([]byte, error) {
	// Log.Debugf("Mmap chunk %v / %v", index+1, s.MaxChunks)
	if s.ChunkFile != nil {
		offset := int64(index) * (s.HeaderSize + s.ChunkSize)
		bytes, err := syscall.Mmap(int(s.ChunkFile.Fd()), offset, int(s.HeaderSize+s.ChunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return nil, err
		}

		return bytes, nil
	} else {
		bytes, err := syscall.Mmap(-1, 0, int(s.ChunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
		if err != nil {
			return nil, err
		}

		return bytes, nil
	}
}

// Clear removes all old chunks on disk (will be called on each program start)
func (s *Storage) Clear() error {
	return nil
}

// Purge a chunk from storage
func (s *Storage) Purge(key string) bool {
	id := keyToId(key)
	s.lock.Lock()
	if chunk, exists := s.chunks[id]; exists {
		s.stack.Remove(id)
		delete(s.chunks, id)
		s.lock.Unlock()
		s.buffers <- chunk
		return true
	}
	s.lock.Unlock()
	return false
}

// Load a chunk from ram or creates it
func (s *Storage) Load(key string) []byte {
	id := keyToId(key)
	s.lock.RLock()
	if chunk, exists := s.chunks[id]; exists {
		clean := chunk.Clean()
		if clean || chunk.Valid(s.magic) {
			if clean {
				Log.Tracef("Load chunk %v (clean)", key)
			} else {
				Log.Debugf("Load chunk %v (verified)", key)
			}
			s.stack.Touch(id)
			defer s.lock.RUnlock()
			return chunk.Bytes()
		} else {
			Log.Warningf("Load chunk %v (purge: invalid)", key)
			s.lock.RUnlock()
			s.Purge(key)
			return nil
		}
	}
	s.lock.RUnlock()
	return nil
}

// Store stores a chunk in the RAM and adds it to the disk storage queue
func (s *Storage) Store(key string, bytes []byte) (err error) {
	id := keyToId(key)
	s.lock.RLock()

	// Avoid storing same chunk multiple times
	chunk, exists := s.chunks[id]
	if exists {
		if chunk.Clean() {
			Log.Tracef("Create chunk %v %v/%v (exists: clean)", key, s.stack.Len(), s.MaxChunks)
			s.lock.RUnlock()
			return nil
		}
		if chunk.Valid(s.magic) {
			Log.Debugf("Create chunk %v %v/%v (exists: valid)", key, s.stack.Len(), s.MaxChunks)
			s.lock.RUnlock()
			return nil
		}
		Log.Warningf("Create chunk %v %v/%v (exists: overwrite)", key, s.stack.Len(), s.MaxChunks)
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	if !exists {
		usedChunks := s.stack.Len()
		deleteID := s.stack.Pop()
		if 0 != deleteID {
			chunk = s.chunks[deleteID]
			delete(s.chunks, deleteID)
			Log.Debugf("Create chunk %v %v/%v (reused)", key, usedChunks, s.MaxChunks)
		} else {
			select {
			case chunk = <-s.buffers:
				Log.Debugf("Create chunk %v %v/%v (stored)", key, usedChunks, s.MaxChunks)
			default:
				Log.Debugf("Create chunk %v %v/%v (failed)", key, usedChunks, s.MaxChunks)
				return fmt.Errorf("No buffers available")
			}
		}
	}

	copy(chunk[s.HeaderSize:], bytes)
	if 0 != s.HeaderSize {
		size := uint32(len(bytes))
		binary.LittleEndian.PutUint64(chunk, id)
		checksum := crc32.Checksum(chunk[s.HeaderSize:], crc32Table)
		binary.LittleEndian.PutUint32(chunk[8:], checksum)
		binary.LittleEndian.PutUint32(chunk[12:], size)
		binary.LittleEndian.PutUint64(chunk[16:], s.magic^id^(uint64(checksum)<<32|uint64(size)))
		chunk[24] = 0 // Clear dirty bit
	}
	s.chunks[id] = chunk
	s.stack.Push(id)

	return nil
}

// keyToId converts string key to internal uint64 representation
func keyToId(key string) uint64 {
	return crc64.Checksum([]byte(key), crc64Table)
}
