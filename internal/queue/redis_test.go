////go:build integration

package queue

import (
	"context"
	"errors"
	"flag"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	qNameReady      = "ready"
	qNameProcessing = "processing"
	raddr = flag.String("rddr", "localhost:6379", "Redis Address")
)


func setupRedisStoreTest(t *testing.T) (*RedisStore, context.Context) {
	t.Helper()
	ctx := context.Background()
	ms, err := NewRedisStore(ctx, *raddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ms.client.FlushDB(ctx).Err(); err != nil {
			t.Logf("failed to flush redis: %v", err)
		}
		ms.Close()
	})
	return ms, ctx
}

func TestRedisEnqueue(t *testing.T) {
	s := miniredis.RunT(t)

	ctx := context.Background()

	ms, err := NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	want := uuid.NewString()

	j := Job{
		ID: want,
	}

	if err := ms.Enqueue(ctx, &j, qNameReady); err != nil {
		t.Fatal(err)
	}

	t.Run("pushes job id to queue", func(t *testing.T) {
		id, err := s.Lpop("queue:default:" + qNameReady)
		if err != nil {
			t.Fatal(err)
		}

		if id != want {
			t.Errorf("list id = %q, want = %q", id, want)
		}
	})

	t.Run("loads id from hash", func(t *testing.T) {
		if got := s.HGet("job:"+want, "id"); got != want {
			t.Errorf("got : %s -> want : %s", got, want)
		}
	})

}

func TestRedisDequeue(t *testing.T) {
	s := miniredis.RunT(t)

	ms, err := NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	ctx := context.Background()

	want := uuid.NewString()
	job := Job{ID: want}

	t.Run("enqueue job", func(t *testing.T) {
		if err := ms.Enqueue(ctx, &job, qNameReady); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("dequeue", func(t *testing.T) {
		j, err := ms.Dequeue(ctx, qNameReady)
		if err != nil {
			t.Fatal(err)
		}

		if j.ID != want {
			t.Errorf("want job: %q, got %q", want, j.ID)
		}

		if j.Status != StatusProcessing {
			t.Errorf("want %q, got %q", StatusProcessing, j.Status)
		}
	})
}

func TestRedisDequeueMovesToProcessing(t *testing.T) {
	s := miniredis.RunT(t)
	ctx := context.Background()

	ms, err := NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

	job := Job{ID: uuid.NewString()}

	if err := ms.Enqueue(ctx, &job, qNameReady); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Dequeue(ctx, qNameReady); err != nil {
		t.Fatal(err)
	}

	readyQ, _ := s.List("queue:default:ready")

	if len(readyQ) != 0 {
		t.Errorf("expected empty ready queue, got %v", readyQ)
	}

	processingQ, _ := s.List("queue:default:" + qNameProcessing)

	if len(processingQ) != 1 || processingQ[0] != job.ID {
		t.Errorf("expected queue to contain %q, got %v", job.ID, processingQ)
	}
}

func TestConcurrentNoDoubleClaim(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	// ensure each unique test run uses unique queue
	uniqueQname := "miniqq:" + uuid.NewString()

	var (
		sm         sync.Map
		wg         sync.WaitGroup
		dupCount   uint64
		claimCnt   uint64
		numJobs    = 500
		numWorkers = 30
	)

	for range numJobs {
		job := Job{ID: uuid.NewString()}
		if err := ms.Enqueue(ctx, &job, uniqueQname); err != nil {
			t.Fatal(err)
		}
	}

	for i := range numWorkers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			for {

				j, err := ms.Dequeue(ctx, uniqueQname)

				if err != nil {
					if errors.Is(err, redis.Nil) {
						return
					}
					t.Errorf("dequeue by worker <%d> failed: %v", i, err)
					return
				}

				if _, loaded := sm.LoadOrStore(j.ID, struct{}{}); loaded {
					atomic.AddUint64(&dupCount, 1)
					t.Errorf("job %s was dequeued twice", j.ID)
					return
				}

				atomic.AddUint64(&claimCnt, 1)
			}

		}(i)
	}

	wg.Wait()

	if got := atomic.LoadUint64(&claimCnt); got != uint64(numJobs) {
		t.Errorf("expected jobs: %d , got %d", numJobs, got)
	}

	if got := atomic.LoadUint64(&dupCount); got != 0 {
		t.Errorf("expected 0 dupes , got: %d", got)
	}

}

func TestClaimCompleteJob(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	id := uuid.NewString()
	job := Job{ID: id}
	qTestReady := "test-" + uuid.NewString()
	qTestProcessing := "test-" + uuid.NewString()

	t.Run("enqeue job", func(t *testing.T) {
		if err := ms.Enqueue(ctx, &job, qTestReady); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("dequeue job", func(t *testing.T) {
		if _, err := ms.Dequeue(ctx, qTestReady); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("complete job", func(t *testing.T) {
		if err := ms.Complete(ctx, id, qTestProcessing); err != nil {
			t.Fatal(err)
		}
	})

	if n, err := ms.client.LLen(ctx, "queue:default:"+qTestProcessing).Result(); err != nil {
		t.Fatal(err)
	} else {
		if n != 0 {
			t.Errorf("expected empty queue %s, got %d", "queue:default:"+qTestProcessing, n)
		}
	}

	status, err := ms.client.HGet(ctx, "job:"+id, "status").Result()
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if status != StatusDone.String() {
		t.Errorf("expected status %s got %v", StatusDone.String(), status)
	}
}

func TestClaimFailJob(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	id := uuid.NewString()
	job := Job{ID: id}
	qTestReady := "test-" + uuid.NewString()
	qTestProcessing := "test-" + uuid.NewString()

	t.Run("enqueue job", func(t *testing.T) {
		if err := ms.Enqueue(ctx, &job, qTestReady); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("dequeue job", func(t *testing.T) {
		if _, err := ms.Dequeue(ctx, qTestReady); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("fail job", func(t *testing.T) {
		if err := ms.Fail(ctx, id, qTestProcessing, errors.New("terror")); err != nil {
			t.Fatal(err)
		}
	})

	if n, err := ms.client.LLen(ctx, "queue:default:"+qTestProcessing).Result(); err != nil {
		t.Fatal(err)
	} else if n != 0 {
		t.Errorf("expected empty queue, got %d", n)
	}

	attempts, err := ms.client.HGet(ctx, "job:"+id, "attempts").Result()
	if err != nil || attempts != "1" {
		t.Errorf("expected attempts=1, got %v (err=%v)", attempts, err)
	}

	lastErr, err := ms.client.HGet(ctx, "job:"+id, "last_error").Result()
	if err != nil || lastErr != "terror" {
		t.Errorf("expected last_error=terror, got %v (err=%v)", lastErr, err)
	}
}
