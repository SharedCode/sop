import sys, strings

with open('ai/database/database.go', 'r') as f:
    content = f.read()

injection = """
import "strings"

// GetStores returns the list of B-Tree stores, excluding system stores and Playbooks.
func (db *Database) GetStores(ctx context.Context) ([]string, error) {
...
