package chunk

import (
	"fmt"
	"os"

	"github.com/plexdrive/plexdrive/drive"
)

// Manager manages chunks on disk
type Manager struct {
	ChunkSize  int64
	LoadAhead  int
	downloader *Downloader
	storage    *Storage
	queue      chan *queueEntry
}

type queueEntry struct {
	request  *Request
	response chan Response
}

// Request represents a chunk request
type Request struct {
	id             string
	object         *drive.APIObject
	offsetStart    int64
	offsetEnd      int64
	chunkOffset    int64
	chunkOffsetEnd int64
	sequence       int
	preload        bool
}

// Response represetns a chunk response
type Response struct {
	Sequence int
	Error    error
	Bytes    []byte
}

// NewManager creates a new chunk manager
func NewManager(
	chunkFile string,
	chunkSize int64,
	loadAhead,
	checkThreads int,
	loadThreads int,
	client *drive.Client,
	maxChunks int) (*Manager, error) {

	pageSize := int64(os.Getpagesize())
	if chunkSize < pageSize {
		return nil, fmt.Errorf("Chunk size must not be < %v", pageSize)
	}
	if chunkSize%pageSize != 0 {
		return nil, fmt.Errorf("Chunk size must be divideable by %v", pageSize)
	}
	if maxChunks < 2 || maxChunks < loadAhead {
		return nil, fmt.Errorf("max-chunks must be greater than 2 and bigger than the load ahead value")
	}

	bufferPool, err := NewBufferPool(maxChunks+loadThreads, chunkSize, chunkFile)
	if nil != err {
		return nil, err
	}

	storage := NewStorage(chunkSize, maxChunks, bufferPool)

	downloader, err := NewDownloader(loadThreads, client, storage)
	if nil != err {
		return nil, err
	}

	manager := Manager{
		ChunkSize:  chunkSize,
		LoadAhead:  loadAhead,
		downloader: downloader,
		storage:    storage,
		queue:      make(chan *queueEntry, 100),
	}

	if err := manager.storage.Clear(); nil != err {
		return nil, err
	}

	for i := 0; i < checkThreads; i++ {
		go manager.thread()
	}

	return &manager, nil
}

// GetChunk loads one chunk and starts the preload for the next chunks
func (m *Manager) GetChunk(object *drive.APIObject, offset, size, padding int64) ([]byte, error) {
	// Log.Tracef("Get %v offset %v size %v", object.ObjectID, offset, size)
	maxOffset := int64(object.Size)
	if offset > maxOffset {
		return nil, fmt.Errorf("Tried to read past EOF of %v at offset %v", object.ObjectID, offset)
	}
	if offset+size > maxOffset {
		size = int64(object.Size) - offset
	}

	ranges := splitChunkRanges(offset, size, m.ChunkSize)
	responses := make(chan Response, len(ranges))

	for i, r := range ranges {
		m.requestChunk(object, r.offset, r.size, i, responses)
	}

	data := make([]byte, size+padding, size+padding)
	var err error
	for i := 0; i < cap(responses); i++ {
		res := <-responses
		if nil != res.Error {
			err = res.Error
			continue
		}

		if err == nil {
			dataOffset := ranges[res.Sequence].offset - offset + padding

			n := copy(data[dataOffset:], res.Bytes)
			if n == 0 {
				err = fmt.Errorf("Request %v slice %v has empty response", object.ObjectID, res.Sequence)
			}
		}
	}
	close(responses)

	if nil != err {
		return nil, err
	}

	return data, nil
}

func (m *Manager) requestChunk(object *drive.APIObject, offset, size int64, sequence int, response chan Response) {
	chunkOffset := offset % m.ChunkSize
	offsetStart := offset - chunkOffset
	offsetEnd := offsetStart + m.ChunkSize
	id := fmt.Sprintf("%v:%v", object.ObjectID, offsetStart)

	request := &Request{
		id:             id,
		object:         object,
		offsetStart:    offsetStart,
		offsetEnd:      offsetEnd,
		chunkOffset:    chunkOffset,
		chunkOffsetEnd: chunkOffset + size,
		sequence:       sequence,
		preload:        false,
	}

	m.queue <- &queueEntry{
		request:  request,
		response: response,
	}

	for i := m.ChunkSize; i < (m.ChunkSize * int64(m.LoadAhead+1)); i += m.ChunkSize {
		aheadOffsetStart := offsetStart + i
		aheadOffsetEnd := aheadOffsetStart + m.ChunkSize
		if uint64(aheadOffsetStart) < object.Size && uint64(aheadOffsetEnd) < object.Size {
			id := fmt.Sprintf("%v:%v", object.ObjectID, aheadOffsetStart)
			request := &Request{
				id:          id,
				object:      object,
				offsetStart: aheadOffsetStart,
				offsetEnd:   aheadOffsetEnd,
				preload:     true,
			}
			m.queue <- &queueEntry{
				request: request,
			}
		}
	}
}

type byteRange struct {
	offset, size int64
}

// Calculate request ranges that span multiple chunks
//
// This can happen with Direct-IO and unaligned reads or
// if the size is bigger than the chunk size.
func splitChunkRanges(offset, size, chunkSize int64) []byteRange {
	ranges := make([]byteRange, 0, size/chunkSize+2)
	for remaining := size; remaining > 0; remaining -= size {
		size = min(remaining, chunkSize-offset%chunkSize)
		ranges = append(ranges, byteRange{offset, size})
		offset += size
	}
	return ranges
}

func (m *Manager) thread() {
	for {
		queueEntry := <-m.queue
		m.checkChunk(queueEntry.request, queueEntry.response)
	}
}

func (m *Manager) checkChunk(req *Request, response chan Response) {
	if chunk := m.storage.Load(req.id); nil != chunk {
		if nil != response {
			response <- Response{
				Sequence: req.sequence,
				Bytes:    adjustResponseChunk(req, chunk),
			}
		}
		return
	}

	m.downloader.Download(req, func(err error, chunk []byte) {
		if nil != err {
			if nil != response {
				response <- Response{
					Sequence: req.sequence,
					Error:    err,
				}
			}
			return
		}

		if nil != response {
			response <- Response{
				Sequence: req.sequence,
				Bytes:    adjustResponseChunk(req, chunk),
			}
		}
	})
}

func adjustResponseChunk(req *Request, bytes []byte) []byte {
	bytesLen := int64(len(bytes))
	sOffset := min(req.chunkOffset, bytesLen)
	eOffset := min(req.chunkOffsetEnd, bytesLen)
	return bytes[sOffset:eOffset]
}

func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}
