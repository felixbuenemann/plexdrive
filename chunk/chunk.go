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
func (c *Chunk) Valid(id uint64) bool {
	if c.ID != id {
		return false
	}
	if !c.verify {
		return true
	}
	if !c.verified {
		c.valid = c.Checksum == c.calculateChecksum()
		c.verified = true
	}
	return c.valid
}

func (c *Chunk) Read() []byte {
	return c.bytes[:c.Size]
}

func (c *Chunk) Reset() {
	c.ID = 0
	c.Size = 0
	c.Checksum = 0
	c.verified = false
	c.valid = false
}

func (c *Chunk) Update(id uint64, bytes []byte) {
	c.ID = id
	c.Size = uint32(copy(c.bytes, bytes))
	c.updateChecksum()
}

func (c *Chunk) updateChecksum() bool {
	if !c.verify {
		return true
	}
	if nil == c.bytes {
		c.Checksum = 0
		c.verified = false
		c.valid = false
		return false
	}
	c.Checksum = c.calculateChecksum()
	c.verified = true
	c.valid = true
	return true
}

func (c *Chunk) calculateChecksum() uint32 {
	if nil == c.bytes || 0 == c.Size {
		return 0
	}
	return crc32.Checksum(c.bytes[:c.Size], crc32Table)
}
