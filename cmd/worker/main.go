package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/Afrawles/miniQq/internal/handler"
	"github.com/Afrawles/miniQq/internal/queue"
	"github.com/Afrawles/miniQq/internal/worker"
)

func main() {
	addr := flag.String("addr", "localhost:6379", "Redis Address")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := queue.NewRedisStore(context.Background(), *addr)
	if err != nil {
		log.Fatal(err)
	}

	registry := &handler.Registry{}
	_ = registry.Register("kind", loghandler)

	go func() {
		<-ctx.Done()
		time.Sleep(30 * time.Second)
		log.Fatal("forceful shutdown: workers didnt finish in time")
	}()

	if err := worker.RunPool(ctx, store, registry, []string{"log"}, 2); err != nil {
		log.Fatal(err)
	}

	log.Println("gracefully shutdown")

}

func loghandler(_ context.Context, _ json.RawMessage) error {
	log.Println("<<<log handler >>>")
	return nil
}
