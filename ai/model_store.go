package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileModelStore implements ModelStore using the local file system.
type FileModelStore struct {
	basePath string
}

// NewFileModelStore creates a new file-based model store.
func NewFileModelStore(path string) (*FileModelStore, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create model store directory: %w", err)
	}
	return &FileModelStore{basePath: path}, nil
}

// Save persists a model with the given name.
func (s *FileModelStore) Save(ctx context.Context, name string, model any) error {
	data, err := json.MarshalIndent(model, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal model: %w", err)
	}
	filePath := filepath.Join(s.basePath, name+".json")
	return os.WriteFile(filePath, data, 0644)
}

// Load retrieves a model by name and populates the provided object.
func (s *FileModelStore) Load(ctx context.Context, name string, target any) error {
	filePath := filepath.Join(s.basePath, name+".json")
	data, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

// List returns the names of all stored models.
func (s *FileModelStore) List(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.basePath)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, strings.TrimSuffix(entry.Name(), ".json"))
		}
	}
	return names, nil
}

// Delete removes a model from the store.
func (s *FileModelStore) Delete(ctx context.Context, name string) error {
	filePath := filepath.Join(s.basePath, name+".json")
	return os.Remove(filePath)
}
