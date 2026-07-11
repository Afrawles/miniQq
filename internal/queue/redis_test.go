package queue

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
)

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

	if err := ms.Enqueue(ctx, &j); err != nil {
		t.Fatal(err)
	}

	t.Run("pushes job id to queue", func(t *testing.T) {
		id, err := s.Lpop("queue:default:ready")
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
		if err := ms.Enqueue(ctx, &job); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("dequeue", func(t *testing.T) {
		j, err := ms.Dequeue(ctx)
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

	if err := ms.Enqueue(ctx, &job); err != nil {
		t.Fatal(err)
	}

	if _, err := ms.Dequeue(ctx); err != nil {
		t.Fatal(err)
	}

	readyQ, _ := s.List("queue:default:ready")

	if len(readyQ) != 0 {
		t.Errorf("expected empty reqdy queue, got %v", readyQ)
	}

	processingQ, _ := s.List("queue:default:processing")

	if len(processingQ) != 1 || processingQ[0] != job.ID {
		t.Errorf("expected queue to contain %q, got %v", job.ID, processingQ)
	}
}
