package queue

import "context"

type Store interface {
	Enqueue(context.Context, *Job) error
	Dequeue(context.Context) (*Job, error)
	Close() error
}
