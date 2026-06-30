package queue

import (
	"fmt"
	"sync"
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

func TestMultipleEnqueueDequeue(t *testing.T) {
	ms := New()
	var sm sync.Map
	var wg sync.WaitGroup
	numWorker := 10
	numJobsPerWorker := 2

	for i:=1; i<=20; i++ {
		job :=  Job{
			ID: fmt.Sprintf("job_%d", i), 
			Status: StatusPending,
		}

		err := ms.Enqueue(&job)
		if err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}
	}

	for range numWorker {
		wg.Add(1)
		
		go func() {
			defer wg.Done()

			for range numJobsPerWorker {
				j, err := ms.Dequeue()
				if err != nil {
					t.Errorf("failed to edequeu: %v", err)
					return
				}

				_, loaded := sm.LoadOrStore(j.ID, struct{}{})
				if loaded {
					t.Errorf("job %s was dequeued twice", j.ID)
					return
				}

			}
		}()
	}

	wg.Wait()
}
