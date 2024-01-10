package kafka

import (
	"context"

	"github.com/SharedCode/sop"
)

type queue[T any] struct {
	items []T
}

// TODO: NewQueue manages the deleted Items in Kafka or something similar/for simple enqueue/dequeue.
// Below is just a mock so we can move forward prototyping the system. Finalize the API as well, e.g. perhaps
// we can use one (generics) Queue implemented to talk to Kafka and can take in any struct type.
func NewMockQueue[T any]() Queue[T] {
	return &queue[T]{
		items: make([]T, 0, 25),
	}
}

func (q *queue[T]) Enqueue(ctx context.Context, items ...T) []sop.KeyValuePair[string, error] {
	q.items = append(q.items, items...)
	return nil
}

func (q *queue[T]) Peek(ctx context.Context, count int) ([]T, error) {
	batch := make([]T, count)
	copy(batch, q.items)
	return batch, nil
}

func (q *queue[T]) Dequeue(ctx context.Context, count int) ([]T, error) {
	batch := make([]T, count)
	copy(batch, q.items)
	if count >= len(q.items) {
		q.items = make([]T, 0, 20)
		return batch, nil
	}
	newSlice := make([]T, len(q.items)-count)
	copy(newSlice, q.items[count:])
	q.items = newSlice
	return batch, nil
}
