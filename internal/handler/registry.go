package handler

import (
	"context"
	"fmt"
	"sync"

	"github.com/Afrawles/miniQq/internal/queue"
)

type Handler func(ctx context.Context, payload queue.Payload) error

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func (r *Registry) Register(kind string, handler Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.handlers == nil {
		r.handlers = make(map[string]Handler)
	}

	if _, ok := r.handlers[kind]; ok {
		return fmt.Errorf("%q already exists", kind)
	}
	r.handlers[kind] = handler
	return nil
}

func (r *Registry) Get(kind string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	v, ok := r.handlers[kind]
	if !ok {
		return nil, false
	}
	return v, true
}
