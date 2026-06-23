package queue

type Store interface {
	Enqueue(*Job) error 
	Dequeue() (*Job, error) 
}
