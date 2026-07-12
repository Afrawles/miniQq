package queue

import "context"

type Store interface {
	Enqueue(ctx context.Context, j *Job, qname string) error
	Dequeue(ctx context.Context, qname string) (*Job, error)
	Close() error
	Complete(ctx context.Context, id, qname string) error
	Fail(ctx context.Context, id, qname string, err error) error
}
