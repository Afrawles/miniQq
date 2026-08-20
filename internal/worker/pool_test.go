package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Afrawles/miniQq/internal/handler"
	"github.com/Afrawles/miniQq/internal/queue"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"go.uber.org/goleak"
)

func TestConcurrentWorkerJobProcessing(t *testing.T) {
	var (
		sm             sync.Map
		claimedJobs    uint64
		dupliccateJobs uint64
		nWorkers       = 10
		nJobs          = 1000
		jobType        = "concurrent_test"
	)
	qname := "miniqq:" + uuid.NewString()

	s := miniredis.RunT(t)

	ctx, cancel := context.WithCancel(context.Background())

	ms, err := queue.NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	// register job
	registry := &handler.Registry{}

	testHandler := func(ctx context.Context, payload queue.Payload) error {
		var job string

		if err := json.Unmarshal(payload, &job); err != nil {
			return err
		}

		if _, loaded := sm.LoadOrStore(job, struct{}{}); loaded {
			atomic.AddUint64(&dupliccateJobs, 1)
			return fmt.Errorf("duplicate job %v", job)
		}

		if count := atomic.AddUint64(&claimedJobs, 1); count == uint64(nJobs) {
			cancel()
		}

		return nil
	}

	t.Run("register job handler", func(t *testing.T) {
		if err := registry.Register(jobType, testHandler); err != nil {
			t.Fatal(err)
		}
	})

	for range nJobs {
		job := queue.Job{
			ID:   uuid.New().String(),
			Kind: jobType,
		}

		b, err := json.Marshal(job.ID)
		if err != nil {
			t.Fatal(err)
		}

		job.Payload = b

		if err := ms.Enqueue(ctx, &job, qname); err != nil {
			t.Fatal(err)
		}
	}

	queues := []string{qname}

	if err := RunPool(ctx, ms, registry, queues, nWorkers); err != nil {
		t.Fatal(err)
	}

	if got := atomic.LoadUint64(&dupliccateJobs); got != 0 {
		t.Errorf("expected 0 dupes , got: %d", got)
	}
	if got := atomic.LoadUint64(&claimedJobs); got != uint64(nJobs) {
		t.Errorf("expected jobs: %d , got %d", nJobs, got)
	}

	fmt.Printf("completed jobs: %d vs dulipcate %d", claimedJobs, dupliccateJobs)

	s.Close()
	goleak.VerifyNone(t)

}
