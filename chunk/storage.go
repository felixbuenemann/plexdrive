package chunk

import (
	. "github.com/claudetech/loggo/default"
)

// Storage is a chunk storage
type Storage interface {
	Check(id string) bool
	Load(id string) ([]byte, func())
	Store(id string, bytes []byte) error
	Clear() error
	Close() error
}

// NewStorage creates a new storage
func NewStorage(chunkSize int64, maxChunks int, chunkFile string) (storage Storage, err error) {
	// Non-empty string in chunkFile enables BoltDB disk storage for chunks
	if chunkFile != "" {
		Log.Infof("Initializing Bolt Storage...")
		storage, err = NewBoltStorage(chunkSize, maxChunks, chunkFile)
	} else {
		Log.Infof("Initializing MMap Storage...")
		storage, err = NewMMapStorage(chunkSize, maxChunks)
	}
	return
}
