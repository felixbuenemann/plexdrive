package chunk

import (
	"hash/crc32"
)

// Chunk of memory
type Chunk struct {
	ID       uint64
	Offset   uint64
	Size     uint32
	Checksum uint32
	verify   bool
	verified bool
	valid    bool
	bytes    []byte
}

// Checks if chunk is valid
func (c *Chunk) Valid() bool {
	if !c.verify {
		return true
	}
	if !c.verified {
		c.valid = c.Checksum == crc32.Checksum(c.bytes, crc32Table)
		c.verified = true
	}
	return c.valid
}

func (c *Chunk) UpdateChecksum() bool {
	if !c.verify {
		return true
	}
	if nil == c.bytes {
		c.Checksum = 0
		c.verified = false
		c.valid = false
		return false
	}
	c.Checksum = crc32.Checksum(c.bytes, crc32Table)
	c.verified = true
	c.valid = true
	return true
}
