package queue

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

var defaulMaxAttempts = 5

type RedisStore struct {
	client      *redis.Client
	maxAttempts *int64
	baseDelay   time.Duration
	maxDelay    time.Duration
}

func NewRedisStore(ctx context.Context, addr string) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	var (
		baseDelay = 100 * time.Millisecond
		maxDelay  = 10 * time.Second
	)

	fmt.Println("\n\t\t<<< PING >>>")
	if res, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("redis connect: %w", err)
	} else {
		fmt.Printf("\n\t\t>>> %s <<<\n\n", res)
	}

	maxAttempts, err := safeMaxAttempts(baseDelay)
	if err != nil {
		return nil, err
	}

	return &RedisStore{
		client:      client,
		baseDelay:   baseDelay,
		maxAttempts: &maxAttempts,
		maxDelay:    maxDelay,
	}, nil
}

var _ Store = (*RedisStore)(nil)

func (r *RedisStore) Enqueue(ctx context.Context, j *Job, qname string) error {
	if j.MaxAttempts == nil {
		v := int64(defaulMaxAttempts)
		j.MaxAttempts = &v
	}
	err := r.client.HSet(ctx, "job:"+j.ID, j).Err()
	if err != nil {
		return err
	}

	if err := r.client.LPush(ctx, readKey(qname), j.ID).Err(); err != nil {
		return err
	}
	return nil
}

func (r *RedisStore) Dequeue(ctx context.Context, qname string) (*Job, error) {
	id, err := r.client.LMove(ctx, readKey(qname), processingKey(qname), "RIGHT", "LEFT").Result()
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

func (r *RedisStore) Complete(ctx context.Context, id, qname string) error {
	if err := r.client.LRem(ctx, processingKey(qname), 1, id).Err(); err != nil {
		return err
	}

	k := "job:" + id
	if err := r.client.HSet(ctx, k, "status", StatusDone).Err(); err != nil {
		return err
	}

	if err := r.client.Incr(ctx, "stats:"+qname+":done").Err(); err != nil {
		log.Printf("WARNING: failed to increment done stats for queue %q : %v", qname, err)
	}

	return nil
}

func (r *RedisStore) Fail(ctx context.Context, id, qname string, lastErrr error) error {
	var (
		job Job
		b   time.Duration
	)
	k := "job:" + id

	exists, err := r.client.Exists(ctx, k).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("job not found %v", id)
	}

	if err := r.client.LRem(ctx, processingKey(qname), 1, id).Err(); err != nil {
		return err
	}

	if err := r.client.HIncrBy(ctx, k, "attempts", 1).Err(); err != nil {
		return err
	}

	if err := r.client.HSet(ctx, k, "last_error", lastErrr.Error()).Err(); err != nil {
		return err
	}

	if err := r.client.HGetAll(ctx, k).Scan(&job); err != nil {
		return err
	}

	if job.Attempts < job.GetMaxAttempts() {
		b = r.nextBackoff(&job)
		now := time.Now()
		job.RunAt = now.Add(b)

		r.client.ZAdd(ctx, retryKey(qname), redis.Z{
			Score:  float64(now.Add(b).Unix()),
			Member: job.ID,
		})

	} else {
		err := r.client.LPush(ctx, deadKey(qname), job.ID).Err()
		if err != nil {
			return err
		}
		if err := r.client.HSet(ctx, k, "status", StatusDead).Err(); err != nil {
			return err
		}
	}

	return nil
}

func safeMaxAttempts(baseDelay time.Duration) (int64, error) {
	if baseDelay <= 0 {
		return 0, fmt.Errorf("baseDelay must be postive : %v", baseDelay)
	}

	// baseDelay * 2^attempts <= maxInt64
	// 2^attempts <= maxInt64 / baseDelay
	// attempts <= log2(maxInt64/baseDelay)
	n := math.Log2(float64(math.MaxInt64) / float64(baseDelay))

	return int64(n), nil
}

func (m *RedisStore) nextBackoff(j *Job) time.Duration {
	attempts := j.Attempts

	if int64(attempts) > *m.maxAttempts {
		attempts = *m.maxAttempts
	}

	b := m.baseDelay << uint(attempts)
	if b > m.maxDelay || b < 0 {
		b = m.maxDelay
	}

	return time.Duration(rand.Int63n(int64(b) + 1))
}

func processingKey(qname string) string {
	return "queue:" + qname + ":processing"
}

func retryKey(qname string) string {
	return "queue:" + qname + ":retry"
}

func readKey(qname string) string {
	return "queue:" + qname + ":ready"
}

func doneKey(qname string) string {
	return "queue:" + qname + ":done"
}

func deadKey(qname string) string {
	return "queue:" + qname + ":dead"
}


func (j *Job) GetMaxAttempts() int64 {
	if j.MaxAttempts == nil {
		return int64(defaulMaxAttempts)
	}
	return *j.MaxAttempts
}
