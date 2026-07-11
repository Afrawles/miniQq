package queue

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(addr string) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisStore{
		client: client,
	}
}

var _ Store = (*RedisStore)(nil)

func (r *RedisStore) Enqueue(ctx context.Context, j *Job) error {
	_, err := r.client.HSet(ctx, fmt.Sprintf("job:%s", j.ID), j).Result()
	if err != nil {
		return err
	}

	_, err = r.client.LPush(ctx, fmt.Sprintf("queue:%s:ready", "default"), j.ID).Result()

	if err != nil {
		return err
	}
	return nil
}

func (r *RedisStore) Dequeue(_ context.Context) (*Job, error) {
	return nil, nil
}
