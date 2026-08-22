package queue

import (
	"cmp"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed scripts/requeue.lua
var script string

var defaulMaxAttempts = 5
var ErrJobNotFound = errors.New("job not found")

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

// EnqueueAt schedules job to run in future
func (r *RedisStore) EnqueueAt(ctx context.Context, job *Job, runAt time.Time, qname string) error {
	if job.MaxAttempts == nil {
		dv := int64(defaulMaxAttempts)
		job.MaxAttempts = &dv
	}
	err := r.client.HSet(ctx, "job:"+job.ID, job).Err()
	if err != nil {
		return err
	}

	if err := r.client.ZAdd(ctx, scheduledKey(qname), redis.Z{
		Member: job.ID,
		Score:  float64(runAt.Unix()),
	}).Err(); err != nil {
		return err
	}

	job.RunAt.TT = runAt

	return nil
}

func (r *RedisStore) Dequeue(ctx context.Context, qname string) (*Job, error) {
	id, err := r.client.LMove(ctx, readKey(qname), processingKey(qname), "RIGHT", "LEFT").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrJobNotFound
		}
		return nil, err
	}

	k := "job:" + id
	var job Job
	if err := r.client.HGetAll(ctx, k).Scan(&job); err != nil {
		return nil, err
	}

	if job.ID == "" {
		return nil, ErrJobNotFound
	}

	job.Status = StatusProcessing

	if err := r.client.HSet(ctx, k, "status", StatusProcessing).Err(); err != nil {
		return nil, err
	}

	job.ClaimedAt = UnixTime{TT: time.Now()}

	if err := r.client.HSet(ctx, k, "claimed_at", job.ClaimedAt.TT.Unix()).Err(); err != nil {
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

	if err := r.client.HIncrBy(ctx, doneKey(qname), "count", 1).Err(); err != nil {
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
		return ErrJobNotFound
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
		job.RunAt = UnixTime{TT: now.Add(b)}

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

// RequeueDueRetries moves due jobs back to retry queue
func (m *RedisStore) RequeueDueRetries(ctx context.Context, qname string) (int64, error) {
	keys := []string{retryKey(qname), readKey(qname)}
	args := []any{
		float64(time.Now().Unix()),
		StatusPending.String(),
	}

	// KEYS[1]=retry, KEYS[2]=ready — must match requeue.lua)
	count, err := callEvalScript(ctx, m, script, keys, args)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (m *RedisStore) RequeueDueScheduled(ctx context.Context, qname string) (int64, error) {
	keys := []string{scheduledKey(qname), readKey(qname)}
	args := []any{
		float64(time.Now().Unix()),
		StatusPending.String(),
	}

	count, err := callEvalScript(ctx, m, script, keys, args)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// ReapStale moves staled jobs back to ready whose claimed_at < cutoff
//
// this project currently provides at-least-once delivery, not
// exactly once. job handlers must be idempotent.
func (m *RedisStore) ReapStale(ctx context.Context, qname string, timeout time.Duration) (int64, error) {
	elems, err := m.client.LRange(ctx, processingKey(qname), 0, -1).Result()
	if err != nil {
		return 0, err
	}

	if len(elems) == 0 {
		return 0, nil
	}

	var count int64
	rPipe := m.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(elems))

	for i, jobId := range elems {
		cmds[i] = rPipe.HGet(ctx, "job:"+jobId, "claimed_at")
	}

	_, _ = rPipe.Exec(ctx)
	wPipe := m.client.TxPipeline()

	for i, jobId := range elems {
		result, err := cmds[i].Result()
		if err != nil {
			continue
		}

		claimedAt, err := strconv.ParseInt(result, 10, 64)
		if err != nil {
			fmt.Println(err)
			continue
		}

		if claimedAt < time.Now().Add(-timeout).Unix() {

			wPipe.LRem(ctx, processingKey(qname), 1, jobId)
			wPipe.LPush(ctx, readKey(qname), jobId)
			wPipe.HSet(ctx, "job:"+jobId, "status", StatusPending)

			count++
		}

	}

	if count > 0 {
		_, err = wPipe.Exec(ctx)
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}

// TODO: add pagination
func (m *RedisStore) List(ctx context.Context, filters Filters) (jobs []Job, err error) {
	var (
		cursor   uint64
		queues   []string
		match    = "queue:"
		pageSize = int64(10)
	)

	if filters.PageSize == nil || *filters.PageSize == 0 {
		filters.PageSize = &pageSize
	}

	if filters.Queue == "" {
		match = match + "*"
	} else {
		match = fmt.Sprintf("%s%s:*", match, filters.Queue)
	}

	for {

		keys, nextCoursor, err := m.client.Scan(ctx, cursor, match, *filters.PageSize).Result()
		if err != nil {
			return nil, err
		}

		queues = append(queues, keys...)

		cursor = nextCoursor

		if cursor == 0 {
			break
		}
	}

	for _, v := range queues {
		var elems []string
		var err error

		if strings.HasSuffix(v, ":done") || strings.HasSuffix(v, ":dead") {
			continue
		}

		if strings.HasSuffix(v, ":scheduled") || strings.HasSuffix(v, ":retry") {
			elems, err = m.client.ZRange(ctx, v, 0, -1).Result()
		} else {
			elems, err = m.client.LRange(ctx, v, 0, -1).Result()
		}

		if err != nil {
			return nil, err
		}

		for _, j := range elems {
			var job Job
			if err := m.client.HGetAll(ctx, "job:"+j).Scan(&job); err != nil {
				return nil, err
			}

			if filters.Status != "" {
				if job.Status.String() != filters.Status {
					continue
				}
			}
			jobs = append(jobs, job)

			if len(jobs) >= int(pageSize) {
				break
			}
		}

	}

	if jobs == nil {
		jobs = []Job{}
	}

	slices.SortFunc(jobs, func(a, b Job) int {
		return cmp.Compare(b.CreatedAt.TT.Unix(), a.CreatedAt.TT.Unix())
	})

	return jobs, nil

}

func (m *RedisStore) Stats(ctx context.Context, qname string) (stats QueueStats, err error) {
	var (
		done       int
		ready      int64
		processing int64
		scheduled  int64
		retry      int64
		dead       int64
	)

	stats.Queue = qname

	value, err := m.client.HGet(ctx, doneKey(qname), "count").Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {

			return QueueStats{}, err
		}
		value = "0"
	}

	done, err = strconv.Atoi(value)
	if err != nil {
		return QueueStats{}, err
	}

	stats.Done = int64(done)

	processing, err = m.client.LLen(ctx, processingKey(qname)).Result()
	if err != nil {
		return QueueStats{}, err
	}

	stats.Processing = processing

	retry, err = m.client.ZCard(ctx, retryKey(qname)).Result()
	if err != nil {
		return QueueStats{}, err
	}

	stats.Retry = retry

	dead, err = m.client.LLen(ctx, deadKey(qname)).Result()
	if err != nil {
		return QueueStats{}, err
	}

	stats.Dead = dead

	ready, err = m.client.LLen(ctx, readKey(qname)).Result()
	if err != nil {
		return QueueStats{}, err
	}

	stats.Ready = ready

	scheduled, err = m.client.ZCard(ctx, scheduledKey(qname)).Result()
	if err != nil {
		return QueueStats{}, err
	}

	stats.Scheduled = scheduled

	return
}

func callEvalScript(ctx context.Context, m *RedisStore, script string, keys []string, args []any) (int64, error) {
	n, err := m.client.Eval(ctx, script, keys, args...).Result()
	if err != nil {
		return 0, err
	}

	count, ok := n.(int64)
	if !ok {
		return 0, errors.New("wrong data type for count")
	}

	return count, nil
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

func scheduledKey(qname string) string {
	return "queue:" + qname + ":scheduled"
}

func (j *Job) GetMaxAttempts() int64 {
	if j.MaxAttempts == nil {
		return int64(defaulMaxAttempts)
	}
	return *j.MaxAttempts
}
