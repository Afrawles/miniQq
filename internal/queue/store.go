package queue

import (
	"context"
	"time"
)

type Store interface {
	Enqueue(ctx context.Context, j *Job, qname string) error
	Dequeue(ctx context.Context, qname string) (*Job, error)
	Close() error
	Complete(ctx context.Context, id, qname string) error
	Fail(ctx context.Context, id, qname string, err error) error
	EnqueueAt(ctx context.Context, job *Job, runAt time.Time, qname string) error 
	RequeueDueRetries(ctx context.Context, qname string) (int64, error)
	RequeueDueScheduled(ctx context.Context, qname string) (int64, error)
	ReapStale(ctx context.Context, qname string, timeout time.Duration) (int64, error)
}
