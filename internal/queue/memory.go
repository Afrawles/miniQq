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


var _ Store = (*MemoryStore)(nil)

func (m *MemoryStore) Enqueue(j *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store[j.ID] = j
	m.order.PushBack(j)

	return nil
}

func (m *MemoryStore) Dequeue() (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for e := m.order.Front(); e != nil ; e = e.Next() {
		j, ok := e.Value.(*Job)
		if !ok {
			break
		}

		if j.Status == StatusPending {
			j.Status = StatusProcessing

			m.order.Remove(e)

			return j, nil
		}

		continue
	}

	return nil, nil
}
