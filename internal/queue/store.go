package queue

type Store interface {
	Enqueue(*Job) error 
	Dequeue() error 
}
