package embed

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestGeminiEmbedderRequestsOutputDimensionality(t *testing.T) {
	originalTransport := http.DefaultTransport
	defer func() { http.DefaultTransport = originalTransport }()

	var seen map[string]any
	http.DefaultTransport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		defer req.Body.Close()

		if err := json.Unmarshal(body, &seen); err != nil {
			return nil, err
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"embeddings":[{"values":[1,2,3]}]}`)),
			Header:     make(http.Header),
		}, nil
	})

	embedder := NewGemini("test-key", "gemini-embedding-2")
	_, err := embedder.EmbedTexts(context.Background(), []string{"hello"})
	if err != nil {
		t.Fatalf("EmbedTexts returned an unexpected error: %v", err)
	}

	if seen == nil {
		t.Fatal("expected the Gemini request body to be captured")
	}

	requests, ok := seen["requests"].([]any)
	if !ok || len(requests) != 1 {
		t.Fatalf("expected one Gemini request payload, got %#v", seen["requests"])
	}

	request, ok := requests[0].(map[string]any)
	if !ok {
		t.Fatalf("expected request object, got %#v", requests[0])
	}

	if got := request["outputDimensionality"]; got != float64(768) {
		t.Fatalf("expected outputDimensionality to be 768, got %#v", got)
	}

	if got := request["taskType"]; got != "RETRIEVAL_DOCUMENT" {
		t.Fatalf("expected taskType to be RETRIEVAL_DOCUMENT, got %#v", got)
	}

	if got := request["model"]; got != "models/gemini-embedding-2" {
		t.Fatalf("expected model to be models/gemini-embedding-2, got %#v", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
