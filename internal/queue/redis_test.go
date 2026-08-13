////go:build integration

package queue

import (
	"context"
	"errors"
	"flag"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

var (
	qNameReady      = "ready"
	qNameProcessing = "processing"
	raddr           = flag.String("rddr", "localhost:6379", "Redis Address")
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
		id, err := s.Lpop(readKey(qNameReady))
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

	ctx := context.Background()

	ms, err := NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer ms.Close()

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

	readyQ, _ := s.List(readKey(qNameReady))

	if len(readyQ) != 0 {
		t.Errorf("expected empty ready queue, got %v", readyQ)
	}

	processingQ, _ := s.List(processingKey(qNameReady))

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
					if errors.Is(err, ErrJobNotFound) {
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
		if err := ms.Complete(ctx, id, qTestReady); err != nil {
			t.Fatal(err)
		}
	})

	if n, err := ms.client.LLen(ctx, processingKey(qTestReady)).Result(); err != nil {
		t.Fatal(err)
	} else {
		if n != 0 {
			t.Errorf("expected empty queue %s, got %d", processingKey(qTestReady), n)
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
		if err := ms.Fail(ctx, id, qTestReady, errors.New("terror")); err != nil {
			t.Fatal(err)
		}
	})

	if n, err := ms.client.LLen(ctx, processingKey(qTestReady)).Result(); err != nil {
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

func TestJobFailsTwiceInRetry(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	id := uuid.NewString()
	job := Job{ID: id}
	qTestReady := "test-" + uuid.NewString()

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

	for i := 1; i <= 2; i++ {
		t.Run("fail: job->"+strconv.Itoa(i), func(t *testing.T) {
			if err := ms.Fail(ctx, id, qTestReady, errors.New("terror")); err != nil {
				t.Error(err)
			}
		})
	}

	_, err := ms.client.ZScore(ctx, retryKey(qTestReady), job.ID).Result()
	if err != nil {
		t.Errorf("expected job in retry zset, got err: %v", err)
	}

	attempts, err := ms.client.HGet(ctx, "job:"+job.ID, "attempts").Result()
	if err != nil || attempts != "2" {
		t.Errorf("expected attempts=2, got %v (err=%v)", attempts, err)
	}

}

// one where it fails until max_attempts and ends up in dead list with status dead.
func TestJobFailsUntilMaxAttemptInDeadList(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	id := uuid.NewString()
	maxAttempts := int64(10)
	job := Job{ID: id, MaxAttempts: &maxAttempts}
	qName := "test-" + uuid.NewString()

	t.Run("enqueue job", func(t *testing.T) {
		if err := ms.Enqueue(ctx, &job, qName); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("dequeue job", func(t *testing.T) {
		if _, err := ms.Dequeue(ctx, qName); err != nil {
			t.Fatal(err)
		}
	})

	for i := range maxAttempts {
		t.Run("fail: job->"+strconv.Itoa(int(i)), func(t *testing.T) {
			if err := ms.Fail(ctx, id, qName, errors.New("terror")); err != nil {
				t.Error(err)
			}

			if i < maxAttempts-1 {
				stillInZset(t, ms, ctx, qName, &job)
			}

		})
	}

	result, _ := ms.client.LRange(ctx, deadKey(qName), 0, -1).Result()

	if len(result) != 1 || result[0] != job.ID {
		t.Errorf("expected queue to contain %q, got %v", job.ID, result)
	}

	status, err := ms.client.HGet(ctx, "job:"+id, "status").Result()
	if err != nil || status != StatusDead.String() {
		t.Errorf("expected status %s, got %v (err=%v)", StatusDead.String(), status, err)
	}
}

func stillInZset(t testing.TB, ms *RedisStore, ctx context.Context, qName string, job *Job) {
	t.Helper()

	_, err := ms.client.ZScore(ctx, retryKey(qName), job.ID).Result()
	if err != nil {
		t.Errorf("expected job in retry zset, got err: %v", err)
	}

	inDead, err := ms.client.LPos(ctx, deadKey(qName), job.ID, redis.LPosArgs{}).Result()
	if err == nil {
		t.Errorf("job should not be in dead list yet, but found at position %d", inDead)
	}
}

func TestFailedJobInRetry(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	id := uuid.NewString()
	job := Job{ID: id}
	qTestReady := "test-" + uuid.NewString()

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
		if err := ms.Fail(ctx, id, qTestReady, errors.New("terror")); err != nil {
			t.Error(err)
		}
		// force a deterministic, already-due score so this test isn't racy
		if err := ms.client.ZAdd(ctx, retryKey(qTestReady), redis.Z{
			Score:  float64(time.Now().Add(-time.Second).Unix()),
			Member: id,
		}).Err(); err != nil {
			t.Fatal(err)
		}
	})

	_, err := ms.RequeueDueRetries(ctx, qTestReady)
	if err != nil {
		t.Fatal(err)
	}

	if n, err := ms.client.LLen(ctx, readKey(qTestReady)).Result(); err != nil {
		t.Fatal(err)
	} else {
		if n != 1 {
			t.Errorf("expected one item in queue %s, got %d", readKey(qTestReady), n)
		}
	}

	t.Run("assert correct job", func(t *testing.T) {
		result, _ := ms.client.LRange(ctx, readKey(qTestReady), 0, -1).Result()

		if len(result) != 1 || result[0] != job.ID {
			t.Errorf("expected queue to contain %q, got %v", job.ID, result)
		}
	})

	status, err := ms.client.HGet(ctx, "job:"+id, "status").Result()
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if status != StatusPending.String() {
		t.Errorf("expected status %s got %v", StatusPending.String(), status)
	}

}

func TestScheduledJobDequeue(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	id := uuid.NewString()
	job := Job{ID: id}
	qTestReady := "test-" + uuid.NewString()
	base := time.Now()

	t.Run("schedule job 5 sec out", func(t *testing.T) {
		when := base.Add(5 * time.Second)
		if err := ms.EnqueueAt(ctx, &job, when, qTestReady); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("try dequeue job", func(t *testing.T) {
		job, err := ms.Dequeue(ctx, qTestReady)
		if err != nil {
			if !errors.Is(err, ErrJobNotFound) {
				t.Fatal(err)
			}
		}
		if job != nil {
			t.Errorf("expected no job, got %q", job.ID)
		}
	})

	// set past date as score
	if err := ms.client.ZAdd(ctx, scheduledKey(qTestReady), redis.Z{
		Score:  float64(base.Add(-5 * time.Second).Unix()),
		Member: id,
	}).Err(); err != nil {
		t.Fatal(err)
	}

	_, err := ms.RequeueDueScheduled(ctx, qTestReady)
	if err != nil {
		t.Fatal(err)
	}

	if n, err := ms.client.LLen(ctx, readKey(qTestReady)).Result(); err != nil {
		t.Fatal(err)
	} else {
		if n != 1 {
			t.Errorf("expected one item in queue %s, got %d", readKey(qTestReady), n)
		}
	}

	t.Run("assert correct job", func(t *testing.T) {
		result, _ := ms.client.LRange(ctx, readKey(qTestReady), 0, -1).Result()

		if len(result) != 1 || result[0] != job.ID {
			t.Errorf("expected queue to contain %q, got %v", job.ID, result)
		}
	})

	status, err := ms.client.HGet(ctx, "job:"+id, "status").Result()
	if err != nil {
		t.Fatalf("HGet failed: %v", err)
	}
	if status != StatusPending.String() {
		t.Errorf("expected status %s got %v", StatusPending.String(), status)
	}

	t.Run("dequeue scheduled job", func(t *testing.T) {
		j, err := ms.Dequeue(ctx, qTestReady)
		if err != nil {
			t.Fatal(err)
		}

		if job.ID != j.ID {
			t.Errorf("expcted job : % v, got %v", job.ID, j.ID)
		}
	})

}

func TestReapStale(t *testing.T) {
	ms, ctx := setupRedisStoreTest(t)

	id := uuid.NewString()
	job := Job{ID: id}
	qname := "test-" + uuid.NewString()

	t.Run("claim job", func(t *testing.T) {
		t.Run("enqueue", func(t *testing.T) {
			if err := ms.Enqueue(ctx, &job, qname); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("dequeue", func(t *testing.T) {
			j, err := ms.Dequeue(ctx, qname)
			if err != nil {
				t.Fatal(err)
			}

			if j.ID != id {
				t.Fatalf("expected job %q , got %q", id, j.ID)
			}

			// backdate claimed at
			if err := ms.client.HSet(ctx, "job:"+id, "claimed_at", float64(time.Now().Add(-10*time.Second).Unix())).Err(); err != nil {
				t.Fatal(err)
			}

		})

	})

	t.Run("reap stale", func(t *testing.T) {
		timeout := 5 * time.Second
		_, err := ms.ReapStale(ctx, qname, timeout)
		if err != nil {
			t.Fatal(err)
		}

		t.Run("dequeue-able", func(t *testing.T) {
			j, err := ms.Dequeue(ctx, qname)
			if err != nil {
				t.Fatal(err)
			}

			if job.ID != j.ID {
				t.Errorf("expcted job : % v, got %v", job.ID, j.ID)
			}
		})
	})
}
