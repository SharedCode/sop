package dynamic

import (
"context"
)

// LLM provides an interface to interact with a semantic language model,
// simulating agentic reasoning to automatically generate Categories for data
// when it lacks semantic structuring.
type LLM[T any] interface {
// GenerateCategory invokes the model to synthesize a new Category
// based off the underlying data structure's payload.
GenerateCategory(ctx context.Context, payload T) (*Category, error)
}
