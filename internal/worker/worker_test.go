package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Afrawles/miniQq/internal/handler"
	"github.com/Afrawles/miniQq/internal/queue"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
)

var (
	qname = "test-runq"
)

func TestRun(t *testing.T) {
	var ran bool

	testHandler := func(ctx context.Context, payload json.RawMessage) error {
		ran = true
		return nil
	}

	s := miniredis.RunT(t)

	ctx := context.Background()

	ms, err := queue.NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	want := uuid.NewString()

	j := queue.Job{
		ID:   want,
		Kind: "test",
	}
	registry := &handler.Registry{}

	if err := registry.Register(j.Kind, testHandler); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(ctx, interval+(200*time.Millisecond))
	defer cancel()

	queues := []string{qname}

	if err := ms.Enqueue(ctx, &j, qname); err != nil {
		t.Fatal(err)
	}

	if err := Run(ctx, ms, registry, queues); err != nil {
		t.Fatal(err)
	}

	if !ran {
		t.Error("expected handler to have run, but flag was not set")
	}
}

func TestRecoveryOnPanicFailsJob(t *testing.T) {
	testErr := "test recovery"
	testHandler := func(ctx context.Context, payload json.RawMessage) error {
		panic(testErr)
	}

	s := miniredis.RunT(t)

	ctx := context.Background()

	ms, err := queue.NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	want := uuid.NewString()
	var maxAttempts = int64(1)

	j := queue.Job{
		ID:          want,
		Kind:        "test",
		MaxAttempts: &maxAttempts,
	}
	registry := &handler.Registry{}

	if err := registry.Register(j.Kind, testHandler); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(ctx, interval+(200*time.Millisecond))
	defer cancel()

	queues := []string{qname}

	if err := ms.Enqueue(ctx, &j, qname); err != nil {
		t.Fatal(err)
	}

	if err := Run(ctx, ms, registry, queues); err != nil {
		t.Fatal(err)
	}

	attempts := s.HGet("job:"+j.ID, "attempts")
	if attempts != "1" {
		t.Errorf("expected attempts=1, got %v", attempts)
	}

	lastErr := s.HGet("job:"+j.ID, "last_error")
	if lastErr != testErr {
		t.Errorf("expected last_error=%q, got %v", testErr, lastErr)
	}

	status := s.HGet("job:"+j.ID, "status")
	if status != queue.StatusDead.String() {
		t.Errorf("expected status %s, got %v", queue.StatusDead.String(), status)
	}
}
