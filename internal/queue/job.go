package queue

import "time"

type JobState int
const (
	StatusPending  JobState = iota + 1
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

type Job struct {
	ID string
	Queue string 
	Type string
	Payload []byte
	Status JobState
	Attempts int
	MaxAttempts int
	RunAt time.Time
	CreatedAt time.Time
	LastError string
}
