package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestRunIngestSpace_PropagatesDecodeError(t *testing.T) {
	oldConfig := config
	defer func() { config = oldConfig }()

	config = Config{
		Databases: []DatabaseConfig{{Name: "testdb", Path: t.TempDir(), Mode: "standalone"}},
	}

	request := IngestSpaceRequest{
		DatabaseName: "testdb",
		SpaceName:    "medical",
		CustomData:   json.RawMessage(`{"items":[{"id":"broken"`),
	}

	err := runIngestSpace(context.Background(), request, nil, &MockGenerator{}, nil, nil)
	if err == nil {
		t.Fatal("expected ingest helper to return decode error")
	}
	if !strings.Contains(err.Error(), "failed to decode Space item") {
		t.Fatalf("expected decode error, got %v", err)
	}
}
