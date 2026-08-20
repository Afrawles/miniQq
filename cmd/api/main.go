package main

import (
	"context"
	"flag"
	"log"

	"github.com/Afrawles/miniQq/internal/api"
	"github.com/Afrawles/miniQq/internal/queue"
)

func main() {
	cfg := api.Config{}
	flag.StringVar(&cfg.RAddr, "addr", "localhost:6379", "Redis Address")
	flag.StringVar(&cfg.TrustedOrigin, "trusted-origins", "http://localhost:5173", "Trusted Orign for web UI")
	flag.Parse()

	store, err := queue.NewRedisStore(context.Background(), cfg.RAddr)
	if err != nil {
		log.Fatal(err)
	}

	app := api.App{
		Config: cfg,
		Store:  store,
	}

	app.Server()

}
