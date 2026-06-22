package queue

import (
	"container/list"
	"sync"

)

type MemoryStore struct {
	store map[string]*Job
	mu sync.Mutex

	order *list.List
}

func New() *MemoryStore {
	return &MemoryStore{
		store: make(map[string]*Job),
		order: list.New(),
	}
}

func (m *MemoryStore) Enqueue(j *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store[j.ID] = j
	m.order.PushBack(j)

	return nil
}
