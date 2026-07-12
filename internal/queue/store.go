package queue

import "context"

type Store interface {
	Enqueue(ctx context.Context, j *Job, qname string) error
	Dequeue(ctx context.Context, qname string) (*Job, error)
	Close() error
}
