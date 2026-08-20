package miniqq

import (
	"context"

	"github.com/Afrawles/miniQq/internal/queue"
)

type Job = queue.Job
type JobState = queue.JobState
type Store = queue.Store

var ErrJobNotFound = queue.ErrJobNotFound

func NewRedisStore(ctx context.Context, addr string) (Store, error) {
	return queue.NewRedisStore(ctx, addr)
}
