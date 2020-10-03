package chunk

import (
	"container/list"
	"sync"
)

// Stack is a thread safe list/stack implementation
type Stack struct {
	MaxSize int
	items   *list.List
	index   map[string]*list.Element
	lock    sync.Mutex
}

// NewStack creates a new stack
func NewStack(maxChunks int) *Stack {
	return &Stack{
		MaxSize: maxChunks,
		items:   list.New(),
		index:   make(map[string]*list.Element, maxChunks),
	}
}

// Len gets the number of items on the stack
func (s *Stack) Len() int {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.items.Len()
}

// Pop pops the first item from the stack
func (s *Stack) Pop() string {
	s.lock.Lock()
	if s.items.Len() < s.MaxSize {
		s.lock.Unlock()
		return ""
	}

	item := s.items.Front()
	if nil == item {
		s.lock.Unlock()
		return ""
	}
	s.items.Remove(item)
	id := item.Value.(string)
	delete(s.index, id)
	s.lock.Unlock()

	return id
}

// Touch moves the specified item to the last position of the stack
func (s *Stack) Touch(id string) {
	s.lock.Lock()
	item, exists := s.index[id]
	if exists {
		s.items.MoveToBack(item)
	} else {
		s.items.PushBack(id)
		s.index[id] = s.items.Back()
	}
	s.lock.Unlock()
}

// Push adds a new item to the last position of the stack
func (s *Stack) Push(id string) {
	if "" == id {
		return
	}
	s.lock.Lock()
	if _, exists := s.index[id]; exists {
		s.lock.Unlock()
		return
	}
	s.items.PushBack(id)
	s.index[id] = s.items.Back()
	s.lock.Unlock()
}

// Unshift adds a new item to the front of the stack
func (s *Stack) Unshift(id string) {
	if "" == id {
		return
	}
	s.lock.Lock()
	if _, exists := s.index[id]; exists {
		s.lock.Unlock()
		return
	}
	s.items.PushFront(id)
	s.index[id] = s.items.Front()
	s.lock.Unlock()
}
