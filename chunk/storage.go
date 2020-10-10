package chunk

import (
	"bytes"
	"encoding/gob"
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
	"github.com/plexdrive/plexdrive/drive"
)

// ErrTimeout is a timeout error
var ErrTimeout = errors.New("timeout")

var (
	crc32Table = crc32.MakeTable(crc32.Castagnoli)
	crc64Table = crc64.MakeTable(crc64.ECMA)
)

// Storage is a chunk storage
type Storage struct {
	ChunkFile  *os.File
	ChunkSize  int64
	MaxChunks  int64
	cache      *drive.Cache
	chunks     map[uint64]*Chunk
	stack      *Stack
	lock       sync.RWMutex
	buffers    chan *Chunk
	loadChunks int64
	signals    chan os.Signal
	verify     bool
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int64, chunkFilePath string, cache *drive.Cache) (*Storage, error) {
	s := Storage{
		ChunkSize: chunkSize,
		MaxChunks: maxChunks,
		cache:     cache,
		chunks:    make(map[uint64]*Chunk, maxChunks),
		stack:     NewStack(maxChunks),
		buffers:   make(chan *Chunk, maxChunks),
		signals:   make(chan os.Signal, 1),
	}

	// Non-empty string in chunkFilePath enables MMAP disk s for chunks
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
		wantedSize := chunkSize * maxChunks
		if currentSize != wantedSize {
			err = chunkFile.Truncate(wantedSize)
			if nil != err {
				Log.Debugf("%v", err)
				return nil, fmt.Errorf("Could not resize chunk cache file")
			}
		}
		Log.Infof("Created chunk cache file %v", chunkFile.Name())
		s.ChunkFile = chunkFile
		s.verify = true
		s.loadChunks = min(currentSize/chunkSize, maxChunks)
	} else {
		s.verify = false
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
	for i := int64(0); i < s.MaxChunks; i++ {
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
	Log.Infof("Loaded %v/%v cache chunks in %v", loadedChunks, s.MaxChunks, elapsed)
	return nil
}

// initChunk tries to restore a chunk from disk
func (s *Storage) initChunk(index int64) (bool, error) {
	chunk, err := s.allocateChunk(index)
	if err != nil {
		Log.Debugf("%v", err)
		return false, err
	}

	if index < s.loadChunks {
		if err := s.loadJournal(chunk); nil != err {
			Log.Errorf("Failed to load metadata for chunk %v/%v", index+1, s.MaxChunks)
		}
	}

	id := chunk.ID

	if 0 == id {
		Log.Tracef("Allocate chunk %v/%v", index+1, s.MaxChunks)
		s.buffers <- chunk
		return false, nil
	}

	// Check if chunk exists first, could've been written while we verify
	s.lock.RLock()
	_, exists := s.chunks[id]
	s.lock.RUnlock()

	if exists {
		Log.Tracef("Load chunk %v/%v (exists)", index+1, s.MaxChunks)
		s.buffers <- chunk
		return false, nil
	}

	Log.Tracef("Load chunk %v/%v (restored)", index+1, s.MaxChunks)
	s.lock.Lock()
	s.chunks[id] = chunk
	s.stack.Push(id)
	s.lock.Unlock()

	return true, nil
}

// allocateChunk creates a new mmap-backed chunk
func (s *Storage) allocateChunk(index int64) (chunk *Chunk, err error) {
	// Log.Debugf("Mmap chunk %v / %v", index+1, s.MaxChunks)
	chunk = new(Chunk)
	chunk.verify = s.verify
	if s.ChunkFile != nil {
		offset := index * s.ChunkSize
		chunk.bytes, err = syscall.Mmap(int(s.ChunkFile.Fd()), offset, int(s.ChunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		chunk.Offset = uint64(offset)
	} else {
		chunk.bytes, err = syscall.Mmap(-1, 0, int(s.ChunkSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	}
	if nil != err {
		return nil, err
	}
	return
}

// Clear removes all old chunks on disk (will be called on each program start)
func (s *Storage) Clear() error {
	if nil == s.ChunkFile {
		return nil
	}
	prunedChunks, err := s.cache.PruneChunks(uint64(s.ChunkSize), uint64(s.MaxChunks))
	if nil == err {
		Log.Infof("Pruned %v stale chunks from the database", prunedChunks)
	}
	return err
}

// Load a chunk from ram or creates it
func (s *Storage) Load(key string) []byte {
	id := keyToID(key)
	s.lock.RLock()
	defer s.lock.RUnlock()

	chunk, exists := s.chunks[id]

	if !exists {
		return nil
	}

	if chunk.Valid(id) {
		Log.Tracef("Load chunk %v id:%016x checksum:%08x size:%v", key, id, chunk.Checksum, chunk.Size)
		s.stack.Touch(id)
		return chunk.Read()
	}

	Log.Warningf("Purge invalid chunk %v id:%016x (exp:%016x) checksum:%08x (exp:%08x) size:%v", key, chunk.ID, id, chunk.Checksum, chunk.calculateChecksum(), chunk.Size)
	s.stack.Purge(id)
	return nil
}

// Store stores a chunk in the RAM and adds it to the disk storage queue
func (s *Storage) Store(key string, bytes []byte) (err error) {
	id := keyToID(key)
	s.lock.RLock()

	// Avoid storing same chunk multiple times
	chunk, exists := s.chunks[id]
	if exists {
		if chunk.Valid(id) {
			Log.Tracef("Create chunk %v %v/%v (exists)", key, s.stack.Len(), s.MaxChunks)
			s.lock.RUnlock()
			return nil
		}
		Log.Debugf("Create chunk %v %v/%v (overwrite)", key, s.stack.Len(), s.MaxChunks)
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

			chunk.Reset()
			if err := s.updateJournal(chunk); nil != err {
				return err
			}

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

	chunk.Update(id, bytes)
	s.chunks[id] = chunk
	s.stack.Push(id)

	if err := s.updateJournal(chunk); nil != err {
		return err
	}

	return nil
}

// keyToID converts string key to internal uint64 representation
func keyToID(key string) uint64 {
	return crc64.Checksum([]byte(key), crc64Table)
}

// updateJournal writes a chunks metadata to the database
func (s *Storage) updateJournal(c *Chunk) (err error) {
	if nil == s.ChunkFile {
		return nil
	}
	if c.ID == 0 {
		return s.cache.DeleteChunk(c.Offset)
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err = enc.Encode(c)
	if nil != err {
		return
	}
	err = s.cache.StoreChunk(c.Offset, buf.Bytes())
	return
}

// loadJournal restores a chunks metadata from the database
func (s *Storage) loadJournal(c *Chunk) (err error) {
	if nil == s.ChunkFile {
		return nil
	}
	offset := c.Offset
	var data []byte
	data, err = s.cache.LoadChunk(offset)
	if nil != err || nil == data {
		return
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	err = dec.Decode(c)
	if nil != err {
		c.Reset()
		c.Offset = offset
		return
	}
	return
}
