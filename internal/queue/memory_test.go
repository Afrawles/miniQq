package queue

import (
	"testing"

)

const (
	firstJob = "first_job"
	secondJob = "second_job"
)

func TestEnqueue(t *testing.T) {
	ms := New()

	job1 := Job{
		ID: firstJob,
		Status: StatusPending,
	}	

	err := ms.Enqueue(&job1)

	if err != nil {
		t.Fatal(err)
	}

	job2 := Job{
		ID: secondJob,
		Status: StatusPending,
	}
	err = ms.Enqueue(&job2)

	if err != nil {
		t.Fatal(err)
	}

	eJob1 := ms.order.Front()

	if eJob1 == nil {
		t.Fatal("queue empty!")
	}

	gotJob1 := eJob1.Value.(*Job)

	if gotJob1.ID != firstJob {
		t.Errorf("got: %v, want: %v", gotJob1.ID, firstJob)
	}

}

func TestDequeue(t *testing.T) {
	ms := New()

	job1 := Job{
		ID: firstJob,
		Status: StatusPending,
	}

	err := ms.Enqueue(&job1)

	if err != nil {
		t.Fatal(err)
	}

	job2 := Job{
		ID: secondJob,
		Status: StatusPending,
	}

	err = ms.Enqueue(&job2)

	if err != nil {
		t.Fatal(err)
	}

	j, err := ms.Dequeue()

	if err != nil {
		t.Fatal(err)
	}

	if j.ID != firstJob {
		t.Errorf("got: %v, want: %v", j.ID, firstJob)
	}
}
