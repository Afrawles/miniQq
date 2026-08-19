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
		req, err := http.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(string(inputSerialized)))
		if err != nil {
			t.Fatal(err)
		}

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

// TODO: list jobs and stats tests
