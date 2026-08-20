package miniqq

import (
	"context"

	"github.com/Afrawles/miniQq/internal/worker"
)

func RunPool(
	ctx context.Context,
	store Store,
	registry *Registry,
	queues []string,
	concurrency int,
) error {
	return worker.RunPool(ctx, store, registry, queues, concurrency)
}

func Run(
	ctx context.Context,
	store Store,
	registry *Registry,
	queues []string,
) error {
	return worker.Run(ctx, store, registry, queues)
}
