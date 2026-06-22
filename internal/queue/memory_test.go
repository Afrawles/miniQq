package queue

import (
	"testing"

)

func TestEnqueue(t *testing.T) {
	ms := New()

	firstJob, secondJob := "first_job", "second_job"

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
		t.Errorf("got: %v, want: %v", gotJob1, firstJob)
	}

}
