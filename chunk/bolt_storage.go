package chunk

import (
	"fmt"

	. "github.com/claudetech/loggo/default"
	bolt "go.etcd.io/bbolt"
)

var bChunks = []byte("chunks")

// BoltStorage caches chunks using BoltDB
type BoltStorage struct {
	db    *bolt.DB
	stack *Stack
}

func NewBoltStorage(chunkSize int64, maxChunks int, chunkFile string) (Storage, error) {
	if "" == chunkFile {
		return nil, fmt.Errorf("Missing chunk cache path")
	}
	db, err := bolt.Open(chunkFile, 0600, nil)
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not open chunk cache file")
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bChunks); nil != err {
			return err
		}
		return nil
	})
	if nil != err {
		Log.Debugf("%v", err)
		return nil, fmt.Errorf("Could not initialize chunk cache file")
	}
	Log.Infof("Created chunk cache file %v", db.Path())
	storage := BoltStorage{
		db:    db,
		stack: NewStack(maxChunks),
	}
	return &storage, nil
}

// Check if a chunk exists in the database
func (s *BoltStorage) Check(id string) bool {
	tx, err := s.db.Begin(false)
	defer tx.Rollback()
	if nil != err {
		Log.Errorf("Checking chunk %v – transaction failed: %v", id, err)
		return false
	}
	b := tx.Bucket(bChunks)
	if chunk := b.Get([]byte(id)); nil != chunk {
		s.stack.Touch(id)
		return true
	}
	return false
}

// Load a chunk from the database
func (s *BoltStorage) Load(id string) ([]byte, func()) {
	tx, err := s.db.Begin(false)
	if nil != err {
		Log.Errorf("Loading chunk %v – transaction failed: %v", id, err)
		return nil, nil
	}
	b := tx.Bucket(bChunks)
	if chunk := b.Get([]byte(id)); nil != chunk {
		// Log.Debugf("Loaded chunk %v (found)", id)
		s.stack.Touch(id)
		return chunk, func() { tx.Rollback() }
	}
	tx.Rollback()
	Log.Warningf("Loaded chunk %v (missing)", id)
	return nil, nil
}

// Store stores a chunk in the database
func (s *BoltStorage) Store(id string, chunk []byte) (err error) {
	// Avoid storing same chunk multiple times
	if s.Check(id) {
		// Log.Debugf("Create chunk %v %v / %v (exists)", id, s.stack.Len(), s.stack.MaxSize)
		return nil
	}
	deleteID := s.stack.Pop()
	err = s.db.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bChunks)

		if "" != deleteID {
			if err := b.Delete([]byte(deleteID)); nil != err {
				return err
			}
		}

		if err := b.Put([]byte(id), chunk); nil != err {
			return err
		}
		return nil
	})
	if nil != err {
		s.stack.Unshift(deleteID)
		Log.Errorf("Storing chunk %v - transaction failed: %v (rollback)", id, err)
		return fmt.Errorf("Failed to commit chunk %v", id)
	}
	s.stack.Push(id)
	if "" != deleteID {
		Log.Debugf("Create chunk %v / Delete chunk %v %v/%v (reclaimed)", id, deleteID, s.stack.Len(), s.stack.MaxSize)
	} else {
		Log.Debugf("Create chunk %v %v/%v (stored)", id, s.stack.Len(), s.stack.MaxSize)
	}
	return
}

// Clear removes all old chunks on disk
func (s *BoltStorage) Clear() error {
	return nil
}

// Close the chunk database
func (s *BoltStorage) Close() error {
	Log.Debugf("Closing chunk cache file")
	s.db.Close()
	return nil
}
