package queue

import (
	"encoding/json"
	"testing"
)

func TestJobStateUnmarshalJSON(t *testing.T) {
	cases := map[string]JobState{
		`"pending"`:    StatusPending,
		`"processing"`: StatusProcessing,
		`"completed"`:  StatusDone,
		`"failed"`:     StatusFailed,
		`"dead"`:       StatusDead,
	}

	for input, want := range cases {
		var got JobState
		if err := json.Unmarshal([]byte(input), &got); err != nil {
			t.Fatalf("Unmarshal(%s) returned error: %v", input, err)
		}
		if got != want {
			t.Errorf("Unmarshal(%s) = %v, want %v", input, got, want)
		}
	}
}
