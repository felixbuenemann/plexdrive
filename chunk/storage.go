package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
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
	chunks     map[uint64][]byte
	stack      *Stack
	lock       sync.RWMutex
	buffers    chan []byte
	loadQueue  chan []byte
	loadChunks int
	signals    chan os.Signal
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int, chunkFilePath string) (*Storage, error) {
	storage := Storage{
		ChunkSize: chunkSize,
		MaxChunks: maxChunks,
		chunks:    make(map[uint64][]byte, maxChunks),
		stack:     NewStack(maxChunks),
		buffers:   make(chan []byte, maxChunks),
		loadQueue: make(chan []byte, maxChunks),
		signals:   make(chan os.Signal, 1),
	}

	// Non-empty string in chunkFilePath enables MMAP disk storage for chunks
	if chunkFilePath != "" {
		chunkFile, err := os.OpenFile(chunkFilePath, os.O_RDWR|os.O_CREATE, 0600)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not open chunk cache file")
		}
		currentSize, err := chunkFile.Seek(0, io.SeekEnd)
		wantedSize := (pageSize + chunkSize) * int64(maxChunks)
		err = chunkFile.Truncate(wantedSize)
		if nil != err {
			Log.Debugf("%v", err)
			return nil, fmt.Errorf("Could not resize chunk cache file")
		}
		Log.Infof("Created chunk cache file %v", chunkFile.Name())
		storage.ChunkFile = chunkFile
		storage.HeaderSize = pageSize
		storage.loadChunks = int(min(currentSize/(pageSize+chunkSize), int64(maxChunks)))
	}

	// Setup sighandler
	signal.Notify(storage.signals, syscall.SIGINT, syscall.SIGTERM)

	// Initialize chunks
	if err := storage.chunkLoader(); nil != err {
		return nil, err
	}
	// Verify chunks in the background
	go storage.chunkVerifier()

	return &storage, nil
}

// chunkLoader mmaps buffers and enqueues them for verification
func (s *Storage) chunkLoader() error {
	start := time.Now()
	queuedChunks := 0
	for i := 0; i < s.MaxChunks; i++ {
		select {
		case sig := <-s.signals:
			Log.Warningf("Received signal %v, aborting chunk loader", sig)
			return fmt.Errorf("Aborted by signal")
		default:
			queued, err := s.initChunk(i)
			if err != nil {
				Log.Errorf("Failed to allocate chunk %v: %v", i, err)
				return fmt.Errorf("Failed to initialize chunks")
			} else if queued {
				queuedChunks++
			}
		}
	}
	close(s.loadQueue)
	elapsed := time.Since(start)
	throughput := float64(s.MaxChunks) * float64(s.ChunkSize>>20) / elapsed.Seconds()
	Log.Infof("Allocated %v cache chunks in %v (%.2f MiB/s), background verify for %v chunks pending...", s.MaxChunks, elapsed, throughput, queuedChunks)
	return nil
}

// chunkVerifier checksums queued buffers and restores their cache state
func (s *Storage) chunkVerifier() {
	start := time.Now()
	last := time.Now()
	loadedChunks := 0
	invalidChunks := 0
	for {
		select {
		case sig := <-s.signals:
			Log.Warningf("Received signal %v, aborting chunk loader", sig)
			return
		default:
			bytes, more := <-s.loadQueue
			if !more {
				elapsed := time.Since(start)
				totalChunks := loadedChunks + invalidChunks
				throughput := float64(totalChunks) * float64(s.ChunkSize>>20) / elapsed.Seconds()
				Log.Infof("Finished validating %v cache chunks in %v (%.2f MiB/s) (loaded:%v, invalid:%v)", totalChunks, elapsed, throughput, loadedChunks, invalidChunks)
				return
			}
			if s.verifyChunk(bytes) {
				loadedChunks++
			} else {
				invalidChunks++
			}
			if time.Since(last) > time.Second {
				Log.Debugf("Verifying chunks... (loaded:%v, invalid:%v)", loadedChunks, invalidChunks)
				last = time.Now()
			}
		}
	}
}

// initChunk tries to restore a chunk from disk
func (s *Storage) initChunk(index int) (bool, error) {
	bytes, err := s.allocateChunk(index)
	if err != nil {
		Log.Debugf("%v", err)
		return false, err
	}

	var id uint64
	var checksum uint32

	if index < s.loadChunks {
		id = binary.LittleEndian.Uint64(bytes)
		checksum = binary.LittleEndian.Uint32(bytes[8:])
	}

	if id == 0 || checksum == 0 {
		Log.Tracef("Allocate chunk %v / %v", index+1, s.MaxChunks)
		s.buffers <- bytes
		return false, nil
	}

	Log.Tracef("Load chunk %v / %v id:%016x checksum:%08x (queued)", index+1, s.MaxChunks, id, checksum)
	s.loadQueue <- bytes

	return true, nil
}

// verifyChecksum checks the CRC32C of the buffer
func (s *Storage) verifyChunk(bytes []byte) bool {
	id := binary.LittleEndian.Uint64(bytes)
	checksum := binary.LittleEndian.Uint32(bytes[8:])

	if id == 0 || checksum == 0 || checksum != crc32.Checksum(bytes[s.HeaderSize:], crc32Table) {
		s.buffers <- bytes
		return false
	}

	// Check if chunk exists first, could've been written while we verify
	s.lock.Lock()
	if _, exists := s.chunks[id]; exists {
		Log.Tracef("Load chunk id:%016x checksum:%08x (exists)", id, checksum)
		s.buffers <- bytes
	} else {
		Log.Tracef("Load chunk id:%016x checksum:%08x (restored)", id, checksum)
		s.chunks[id] = bytes
		s.stack.Push(id)
	}
	s.lock.Unlock()

	return true
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

// Load a chunk from ram or creates it
func (s *Storage) Load(key string) []byte {
	id := keyToId(key)
	s.lock.RLock()
	if chunk, exists := s.chunks[id]; exists {
		s.stack.Touch(id)
		defer s.lock.RUnlock()
		return chunk[s.HeaderSize:]
	}
	s.lock.RUnlock()
	return nil
}

// Store stores a chunk in the RAM and adds it to the disk storage queue
func (s *Storage) Store(key string, bytes []byte) (err error) {
	id := keyToId(key)
	s.lock.RLock()

	usedChunks := s.stack.Len()

	// Avoid storing same chunk multiple times
	if _, exists := s.chunks[id]; exists {
		Log.Debugf("Create chunk %016x %v / %v (exists)", id, usedChunks, s.MaxChunks)
		s.stack.Touch(id)
		s.lock.RUnlock()
		return nil
	}

	s.lock.RUnlock()
	s.lock.Lock()
	defer s.lock.Unlock()

	var chunk []byte
	deleteID := s.stack.Pop()
	if 0 != deleteID {
		chunk = s.chunks[deleteID]
		delete(s.chunks, deleteID)
		Log.Debugf("Deleted chunk %016x", deleteID)

		Log.Debugf("Create chunk %016x %v / %v (reused)", id, usedChunks, s.MaxChunks)
	} else {
		select {
		case chunk = <-s.buffers:
			Log.Debugf("Create chunk %016x %v / %v (stored)", id, usedChunks, s.MaxChunks)
		default:
			Log.Debugf("Create chunk %016x %v / %v (failed)", id, usedChunks, s.MaxChunks)
			return fmt.Errorf("No buffers available")
		}
	}

	copy(chunk[s.HeaderSize:], bytes)
	if 0 != s.HeaderSize {
		binary.LittleEndian.PutUint64(chunk, id)
		checksum := crc32.Checksum(chunk[s.HeaderSize:], crc32Table)
		binary.LittleEndian.PutUint32(chunk[8:], checksum)
	}
	s.chunks[id] = chunk
	s.stack.Push(id)

	return nil
}

// keyToId converts string key to internal uint64 representation
func keyToId(key string) uint64 {
	return crc64.Checksum([]byte(key), crc64Table)
}
