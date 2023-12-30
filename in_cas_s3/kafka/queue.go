package kafka

import (
	"context"
)

// Queue specifies methods used for managing persisted queue, e.g. - in kafka.
type Queue[T any] interface {
	// Peek allows you to read 'count' number of elements from the queue
	// without taking them out of the queue.
	Peek(ctx context.Context, count int) ([]T, error)
	// Dequeue takes out 'count' number of elements from the queue.
	Dequeue(ctx context.Context, count int) ([]T, error)
	// Enqueue add elements to the queue.
	Enqueue(ctx context.Context, items ...T) error
	
}
