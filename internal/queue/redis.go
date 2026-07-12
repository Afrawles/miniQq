package queue

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(ctx context.Context, addr string) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	fmt.Println("\n\t\t<<< PING >>>")
	if res, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("redis connect: %w", err)
	} else {
		fmt.Printf("\n\t\t>>> %s <<<\n\n", res)
	}

	return &RedisStore{client: client}, nil
}

var _ Store = (*RedisStore)(nil)

func (r *RedisStore) Enqueue(ctx context.Context, j *Job, qname string) error {
	err := r.client.HSet(ctx, "job:"+j.ID, j).Err()
	if err != nil {
		return err
	}

	if err := r.client.LPush(ctx, "queue:default:"+qname, j.ID).Err(); err != nil {
		return err
	}
	return nil
}

func (r *RedisStore) Dequeue(ctx context.Context, qname string) (*Job, error) {
	id, err := r.client.LMove(ctx, "queue:default:"+qname, "queue:default:processing", "RIGHT", "LEFT").Result()
	if err != nil {
		return nil, err
	}

	k := "job:" + id
	var job Job
	if err := r.client.HGetAll(ctx, k).Scan(&job); err != nil {
		return nil, err
	}

	if job.ID == "" {
		return nil, fmt.Errorf("job: %s not found", id)
	}

	job.Status = StatusProcessing

	if err := r.client.HSet(ctx, k, "status", StatusProcessing).Err(); err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *RedisStore) Close() error {
	return r.client.Close()
}

func (m *RedisStore) Complete(ctx context.Context, id, qname string) error {
	if err := m.client.LRem(ctx, "queue:default:"+qname, 1, id).Err(); err != nil {
		return err
	}

	k := "job:" + id
	if err := m.client.HSet(ctx, k, "status", StatusDone).Err(); err != nil {
		return err
	}

	if err := m.client.Incr(ctx, "stats:"+qname+":done").Err(); err != nil {
		log.Printf("WARNING: failed to increment done stats for queue %q : %v", qname, err)
	}

	return nil
}

func (m *RedisStore) Fail(ctx context.Context, id, qname string, err error) error {
	if err := m.client.LRem(ctx, "queue:default:"+qname, 1, id).Err(); err != nil {
		return err
	}

	k := "job:" + id

	if err := m.client.HIncrBy(ctx, k, "attempts", 1).Err(); err != nil {
		return err
	}

	if err := m.client.HSet(ctx, k, "last_error", err.Error()).Err(); err != nil {
		return err
	}
	return nil
}
