# miniQq
simple background jobs for Go

## Installation

```sh
go get github.com/Afrawles/miniQq
```

## Quick Start
```go
	store, _ := miniqq.NewRedisStore(ctx, "localhost:6379")
    registry := miniqq.NewRegistry()
    registry.Register("send_email", handleEmail)
    miniqq.RunPool(ctx, store, registry, []string{"send_email"}, 4)
```


## Public API

- `NewRedisStore(ctx, addr)`, `NewMemoryStore()` — create a Store
- `NewRegistry()` — create a handler Registry
- `RunPool(ctx, store, registry, queues, concurrency)` — run a worker pool
- `Run(ctx, store, registry, queues)` — run a single worker
- `Job`, `JobState`, `Store`, `Handler`, `Registry`, `ErrJobNotFound`


## UI
> still under development 
