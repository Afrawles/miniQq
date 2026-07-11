package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(ctx context.Context, addr string) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	if res, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("redis connect: %w", err)
	} else {
		fmt.Printf("\n>>> %s <<<\n", res)
	}

	return &RedisStore{client: client}, nil
}

var _ Store = (*RedisStore)(nil)

func (r *RedisStore) Enqueue(ctx context.Context, j *Job) error {
	_, err := r.client.HSet(ctx, fmt.Sprintf("job:%s", j.ID), j).Result()
	if err != nil {
		return err
	}

	if _, err := r.client.LPush(ctx, "queue:default:ready", j.ID).Result(); err != nil {
		return err
	}
	return nil
}

func (r *RedisStore) Dequeue(ctx context.Context) (*Job, error) {
	id, err := r.client.LMove(ctx, "queue:default:ready", "queue:default:processing", "RIGHT", "LEFT").Result()
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

	if _, err := r.client.HSet(ctx, "job:"+job.ID, job).Result(); err != nil {
		return nil, err
	}

	return &job, nil
}

func (r *RedisStore) Close() error {
	return r.client.Close()
}
