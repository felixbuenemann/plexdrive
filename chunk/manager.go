package chunk

import (
	"fmt"

	. "github.com/claudetech/loggo/default"
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
	Free     func()
}

// NewManager creates a new chunk manager
func NewManager(
	chunkSize int64,
	loadAhead,
	checkThreads int,
	loadThreads int,
	client *drive.Client,
	maxChunks int) (*Manager, error) {

	if chunkSize < 4096 {
		return nil, fmt.Errorf("Chunk size must not be < 4096")
	}
	if chunkSize%1024 != 0 {
		return nil, fmt.Errorf("Chunk size must be divideable by 1024")
	}
	if maxChunks < 2 || maxChunks < loadAhead {
		return nil, fmt.Errorf("max-chunks must be greater than 2 and bigger than the load ahead value")
	}

	downloader, err := NewDownloader(loadThreads, client)
	if nil != err {
		return nil, err
	}

	manager := Manager{
		ChunkSize:  chunkSize,
		LoadAhead:  loadAhead,
		downloader: downloader,
		storage:    NewStorage(chunkSize, maxChunks),
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
func (m *Manager) GetChunk(object *drive.APIObject, offset, size int64) ([]byte, error) {
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

	data := make([]byte, size, size)
	var err error
	for i := 0; i < cap(responses); i++ {
		res := <-responses
		if nil != res.Error {
			err = res.Error
			continue
		}

		if err == nil {
			dataOffset := ranges[res.Sequence].offset - offset

			n := copy(data[dataOffset:], res.Bytes)
			if n == 0 {
				err = fmt.Errorf("Request %v slice %v has empty response", object.ObjectID, res.Sequence)
			}
		}
		res.Free()
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
	if buffer := m.storage.Load(req.id); nil != buffer {
		if nil != response {
			buffer.Ref()
			bytes := buffer.Bytes()
			response <- Response{
				Sequence: req.sequence,
				Bytes:    adjustResponseChunk(req, bytes),
				Free:     func() { buffer.Unref() },
			}
		}
		buffer.Unref()
		return
	}

	m.downloader.Download(req, func(err error, buffer *Buffer) {
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
			buffer.Ref()
			bytes := buffer.Bytes()
			response <- Response{
				Sequence: req.sequence,
				Bytes:    adjustResponseChunk(req, bytes),
				Free:     func() { buffer.Unref() },
			}
		}

		if err := m.storage.Store(req.id, buffer); nil != err {
			Log.Warningf("Coult not store chunk %v", req.id)
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
