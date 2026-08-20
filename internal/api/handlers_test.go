package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Afrawles/miniQq/internal/queue"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
)

func TestPostJobs(t *testing.T) {
	var (
		qname   = "handler-test"
		jType   = "handler-type"
		payload = []byte(`"test payload"`)
	)
	s := miniredis.RunT(t)
	ctx := context.Background()

	ms, err := queue.NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		RAddr: s.Addr(),
	}

	app := App{
		Config: cfg,
		Store:  ms,
	}

	defer ms.Close()

	input := struct {
		Queue   string          `json:"queue"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}{
		Queue:   qname,
		Type:    jType,
		Payload: payload,
	}

	inputSerialized, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("create jobs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(string(inputSerialized)))

		resp := httptest.NewRecorder()

		app.HandleEnqueueJob(resp, req)

		if resp.Code != http.StatusCreated {
			t.Errorf("expected `201` got: %d", resp.Code)
		}

		var job queue.Job

		if err := json.Unmarshal([]byte(resp.Body.String()), &job); err != nil {
			t.Fatal(err)
		}

		if string(payload) != string(job.Payload) {
			t.Errorf("expected %v , got %v", string(payload), string(job.Payload))
		}

		if job.ID == "" {
			t.Fatal("expected job to have an ID")
		}

	})
}


func TestJobListQueueStats(t *testing.T) {
	var (
		qname   = "handler-test"
		jobType   = "handler-type"
		nJobs = 10
	)
	s := miniredis.RunT(t)
	ctx := context.Background()

	ms, err := queue.NewRedisStore(ctx, s.Addr())
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		RAddr: s.Addr(),
	}

	app := App{
		Config: cfg,
		Store:  ms,
	}

	defer ms.Close()

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

	t.Run("list jobs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)

		resp := httptest.NewRecorder()

		app.HandleGetJobs(resp, req)

		if resp.Code != http.StatusOK {
			t.Errorf("expcetd sttaus `200` , got %d", resp.Code)
		}

		var jobs map[string][]queue.Job

		if err := json.Unmarshal([]byte(resp.Body.String()), &jobs); err != nil {
			t.Fatal(err)
		}

		if jobs == nil {
			t.Error("expected to get data got nothing")
		}

		if len(jobs["jobs"]) != nJobs {
			t.Fatalf("expected %d : got %d", nJobs, len(jobs["jobs"]))
		}

		job1 := jobs["jobs"][0]

		if job1.Kind != jobType {
			t.Errorf("expected job type %s, got %s", jobType, job1.Kind)
		}

	})

	t.Run("stats jobs", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/queues/{name}/stats", nil)

		req.SetPathValue("name", qname)

		resp := httptest.NewRecorder()

		app.HandleGetQueueStats(resp, req)

		if resp.Code != http.StatusOK {
			t.Errorf("expcetd sttaus `200` , got %d", resp.Code)
		}

		var stats queue.QueueStats
		
		if err := json.Unmarshal([]byte(resp.Body.String()), &stats); err != nil {
			t.Fatal(err)
		}

		if stats.Queue != qname {
			t.Errorf("expected %s, got %s", qname, stats.Queue)
		}

		if stats.Ready != int64(nJobs) {
			t.Errorf("expected %d , got %d", nJobs, stats.Ready)
		}

		if (
		stats.Done != 0 || 
		stats.Scheduled != 0 || 
		stats.Processing != 0 || 
		stats.Dead != 0 || 
		stats.Retry != 0) {
			t.Error("expected remaining queues to have no elems")
		}
	})
}
