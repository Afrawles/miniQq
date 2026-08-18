package worker

import (
	"context"
	"errors"

	"github.com/Afrawles/miniQq/internal/handler"
	"github.com/Afrawles/miniQq/internal/queue"
	"golang.org/x/sync/errgroup"
)

func RunPool(
	ctx context.Context,
	store queue.Store,
	registry *handler.Registry,
	queues []string,
	nWorkers int,
) error {
	g, ctx := errgroup.WithContext(ctx)

	for range nWorkers {
		g.Go(func () error  {
			return Run(ctx, store, registry, queues)
		})
	}

	if err := g.Wait(); err != nil {

		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	return nil
}
