package confighub

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func requireKnowledgeBaseFixture(t *testing.T, storagePath string) Config {
	t.Helper()

	if _, err := os.Stat(storagePath); err != nil {
		t.Skipf("%s not present (gitignored, local-only); skipping", storagePath)
	}

	cfg, err := LoadConfig(storagePath)
	if err != nil {
		t.Fatalf("LoadConfig(%s) failed: %v", storagePath, err)
	}

	if cfg.SystemDB == nil || strings.TrimSpace(cfg.SystemDB.Path) == "" {
		t.Skip("config does not define a system database path")
	}

	if _, err := os.Stat(filepath.Join(cfg.SystemDB.Path, "dboptions.json")); err != nil {
		t.Skipf("knowledge base fixture not available at %s: %v", cfg.SystemDB.Path, err)
	}

	return cfg
}
