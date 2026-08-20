package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	miniqq "github.com/Afrawles/miniQq"
	"github.com/Afrawles/miniQq/internal/queue"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	store, err := miniqq.NewRedisStore(ctx, "localhost:6379")
	if err != nil {
		log.Fatal(err)
	}

	registry := miniqq.NewRegistry()
	registry.Register("email", func(ctx context.Context, payload queue.Payload) error {
		log.Println("prcoessing send email: ", string(payload))
		return nil
	})

	if err := miniqq.RunPool(ctx, store, registry, []string{"email"}, 10); err != nil {
		log.Fatal(err)
	}

}
