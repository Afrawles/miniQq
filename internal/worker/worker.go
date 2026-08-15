package worker

import (
	"context"
	"errors"
	"time"

	"github.com/Afrawles/miniQq/internal/handler"
	"github.com/Afrawles/miniQq/internal/queue"
)

var interval = 500 * time.Millisecond

func Run(
	ctx context.Context, 
	store queue.Store, 
	registry *handler.Registry, 
	queues []string,
) error {
	pulse := time.NewTicker(interval)
	defer pulse.Stop()

	idx := 0

	for {
		select {
		case <-ctx.Done():
		return nil
		case <-pulse.C:
			qname := queues[idx]
			idx = (idx + 1) % len(queues)
			
			job, err := store.Dequeue(ctx, qname)
			if err != nil {
				if errors.Is(err, queue.ErrJobNotFound) {
					continue
				} else {
					return err
				}

			}

			hr, ok := registry.Get(job.Kind)
			if !ok {
				err = store.Fail(ctx, job.ID, qname, errors.New("job type/kind has not been registered yet"))
				if err != nil {
					if errors.Is(err, queue.ErrJobNotFound) {
						continue
					}
					return err
				}
				continue
			}

			if err := hr(ctx, job.Payload); err != nil {
				err = store.Fail(ctx, job.ID, qname, err)
				if err != nil {
					if errors.Is(err, queue.ErrJobNotFound) {
						continue
					}
					return err
				}
				continue
			}
			
			err = store.Complete(ctx, job.ID, qname)
			if err != nil {
				return err
			}
			
		}
		
	}
}
