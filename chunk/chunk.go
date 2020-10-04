package chunk

import (
	"encoding/binary"
	"hash/crc32"
)

// Chunk of memory
type Chunk []byte

func (c Chunk) ID() uint64 {
	return binary.LittleEndian.Uint64(c[0:])
}

func (c Chunk) Checksum() uint32 {
	return binary.LittleEndian.Uint32(c[8:])
}

func (c Chunk) Size() uint32 {
	return binary.LittleEndian.Uint32(c[12:])
}

func (c Chunk) Magic() uint64 {
	return binary.LittleEndian.Uint64(c[16:])
}

func (c Chunk) Bytes() []byte {
	return c[pageSize:]
}

func (c Chunk) Clean() bool {
	return 0 == c[24]
}

func (c Chunk) Valid(seed uint64) bool {
	if 0 == c.ID() {
		return false
	}
	magic := c.GenerateMagic(seed)
	if magic == c.Magic() {
		if !c.Clean() {
			c[24] = 0 // Clear dirty byte
		}
		return true
	}
	checksum := crc32.Checksum(c.Bytes(), crc32Table)
	if checksum == c.Checksum() {
		binary.LittleEndian.PutUint64(c[16:], magic)
		c[24] = 0 // Clear dirty byte
		return true
	}
	binary.LittleEndian.PutUint64(c[0:], 0)
	return false
}

func (c Chunk) GenerateMagic(seed uint64) uint64 {
	return seed ^ c.ID() ^ (uint64(c.Checksum())<<32 | uint64(c.Size()))
}
