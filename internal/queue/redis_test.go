package queue

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
)

func TestRedisEnqueue(t *testing.T) {
	t.Parallel()

	s := miniredis.RunT(t)

	ms := NewRedisStore(s.Addr())

	ctx := context.Background()

	want := uuid.NewString()

	j := Job{
		ID: want,
	}

	if err := ms.Enqueue(ctx, &j); err != nil {
		t.Fatal(err)
	}

	t.Run("pushes jon id to queue", func(t *testing.T) {
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
