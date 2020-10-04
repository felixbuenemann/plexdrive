package chunk

import (
	"container/list"
	"sync"
)

// Stack is a thread safe list/stack implementation
type Stack struct {
	items   *list.List
	index   map[uint64]*list.Element
	lock    sync.Mutex
	maxSize int
}

// NewStack creates a new stack
func NewStack(maxChunks int) *Stack {
	return &Stack{
		items:   list.New(),
		index:   make(map[uint64]*list.Element, maxChunks),
		maxSize: maxChunks,
	}
}

// Len gets the number of items on the stack
func (s *Stack) Len() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.items.Len()
}

// Pop pops the first item from the stack
func (s *Stack) Pop() uint64 {
	s.lock.Lock()
	if s.items.Len() < s.maxSize {
		s.lock.Unlock()
		return 0
	}

	item := s.items.Front()
	if nil == item {
		s.lock.Unlock()
		return 0
	}
	s.items.Remove(item)
	id := item.Value.(uint64)
	delete(s.index, id)
	s.lock.Unlock()

	return id
}

// Touch moves the specified item to the last position of the stack
func (s *Stack) Touch(id uint64) {
	s.lock.Lock()
	if item, exists := s.index[id]; exists {
		s.items.MoveToBack(item)
	}
	s.lock.Unlock()
}

// Push adds a new item to the last position of the stack
func (s *Stack) Push(id uint64) {
	s.lock.Lock()
	if _, exists := s.index[id]; exists {
		s.lock.Unlock()
		return
	}
	s.index[id] = s.items.PushBack(id)
	s.lock.Unlock()
}

// Remove an item from the stack
func (s *Stack) Remove(id uint64) {
	s.lock.Lock()
	if item, exists := s.index[id]; exists {
		s.items.Remove(item)
		delete(s.index, id)
	}
	s.lock.Unlock()
}
