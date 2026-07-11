package queue

import "time"

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
	ID          string    `redis:"id"`
	Queue       string    `redis:"queue"`
	Type        string    `redis:"type"`
	Payload     []byte    `redis:"payload"`
	Status      JobState  `redis:"status"`
	Attempts    int       `redis:"attempts"`
	MaxAttempts int       `redis:"max_attempts"`
	RunAt       time.Time `redis:"run_at"`
	CreatedAt   time.Time `redis:"created_at"`
	LastError   string    `redis:"last_error"`
}
