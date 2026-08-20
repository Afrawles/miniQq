package queue

import (
	"time"
)

type JobState int

const (
	StatusPending JobState = iota + 1
	StatusProcessing
	StatusDone
	StatusFailed
	StatusDead
	StatusUnknown
)

func (js JobState) String() string {
	switch js {
	case StatusPending:
		return "pending"
	case StatusProcessing:
		return "processing"
	case StatusDone:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusDead:
		return "dead"
	default:
		return "unknown"
	}
}

func (js JobState) MarshalBinary() ([]byte, error) {
	return []byte(js.String()), nil
}

func (js *JobState) UnmarshalBinary(data []byte) error {
	switch string(data) {
	case "pending":
		*js = StatusPending
	case "processing":
		*js = StatusProcessing
	case "completed":
		*js = StatusDone
	case "dead":
		*js = StatusDead
	default:
		*js = StatusUnknown
	}
	return nil
}

type Job struct {
	ID          string          `redis:"id" json:"id"`
	Queue       string          `redis:"queue" json:"queue"`
	Payload     Payload `redis:"payload" json:"payload"`
	Status      JobState        `redis:"status" json:"status"`
	Attempts    int64           `redis:"attempts" json:"attempts"`
	MaxAttempts *int64          `redis:"max_attempts" json:"max_attempts"`
	RunAt       time.Time       `redis:"run_at" json:"run_at"`
	CreatedAt   time.Time       `redis:"created_at" json:"created_at"`
	LastError   string          `redis:"last_error" json:"last_error"`
	ClaimedAt   int64           `redis:"claimed_at" json:"claimed_at"`
	Kind        string          `redis:"kind" json:"kind"`
}

type Payload []byte

func (p Payload) MarshalBinary() ([]byte, error) {
	return p, nil
}

func (p *Payload) UnmarshalBinary(data []byte) error {
	return nil
}

func (p Payload) MarshalJSON() ([]byte, error) {
	if len(p) == 0 {
		return []byte("null"), nil
	}

	return p, nil
}

func (p *Payload) UnmarshalJSON(data []byte) error {
	*p = data
	return nil
}

type Filters struct {
	Queue    string
	Status   string
	PageSize *int64
}

type QueueStats struct {
	Queue      string `json:"queue"`
	Ready      int64  `json:"ready"`
	Processing int64  `json:"processing"`
	Scheduled  int64  `json:"scheduled"`
	Retry      int64  `json:"retry"`
	Done       int64  `json:"done"`
	Dead       int64  `json:"dead"`
}
