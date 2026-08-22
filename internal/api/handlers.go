package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/Afrawles/miniQq/internal/queue"
	"github.com/google/uuid"
)

// TODO: logging
func (app *App) HandleEnqueueJob(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Queue   string        `json:"queue"`
		Type    string        `json:"type"`
		Payload queue.Payload `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "invalid Json: "+err.Error(), http.StatusBadRequest)
		return
	}

	ut := queue.UnixTime{TT: time.Now()}

	job := queue.Job{
		ID:        uuid.NewString(),
		Payload:   input.Payload,
		Kind:      input.Type,
		Queue:     input.Queue,
		CreatedAt: ut,
	}

	if err := app.Store.Enqueue(r.Context(), &job, job.Queue); err != nil {
		http.Error(w, "could not process request:  "+err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(job)
	if err != nil {
		http.Error(w, "could not process request", http.StatusInternalServerError)
		return
	}

	b = append(b, '\n')

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	w.Write(b)

}

func (app *App) HandleGetJobs(w http.ResponseWriter, r *http.Request) {
	var input queue.Filters

	q := r.URL.Query()

	if qname := q.Get("queue"); qname != "" {
		input.Queue = qname
	}

	if status := q.Get("status"); status != "" {
		input.Status = status
	}

	if pageSize := q.Get("page_size"); pageSize != "" {
		if ps, err := strconv.Atoi(pageSize); err != nil {
			http.Error(w, "fialed to process reequest: "+err.Error(), http.StatusBadRequest)
			return
		} else {
			pageSize := int64(ps)
			input.PageSize = &pageSize
		}

	}

	jobs, err := app.Store.List(r.Context(), input)
	if err != nil {
		http.Error(w, "unable to process queues", http.StatusInternalServerError)
		return
	}

	var results map[string][]queue.Job

	results = make(map[string][]queue.Job)
	results["jobs"] = jobs

	b, err := json.Marshal(results)
	if err != nil {
		http.Error(w, "could not process your request", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	w.Write(b)

}

func (app *App) HandleGetQueueStats(w http.ResponseWriter, r *http.Request) {
	qname := r.PathValue("name")
	if qname == "" {
		http.Error(w, "provide queue name", http.StatusBadRequest)
		return
	}

	stats, err := app.Store.Stats(r.Context(), qname)
	if err != nil {
		http.Error(w, "could not process requuest", http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(stats)
	if err != nil {
		http.Error(w, "could not process request", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	w.Write(b)
}
