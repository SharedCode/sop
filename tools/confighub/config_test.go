package confighub

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveConfigPath_PrefersExplicitPath(t *testing.T) {
	got := ResolveConfigPath("/tmp/custom.json")
	if got != "/tmp/custom.json" {
		t.Fatalf("ResolveConfigPath() = %q, want %q", got, "/tmp/custom.json")
	}
}

func TestFindExistingConfigFile_FindsConfigInWorkingDir(t *testing.T) {
	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "config.json")
	if err := os.WriteFile(configPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)
	if err := os.Chdir(tmp); err != nil {
		t.Fatal(err)
	}

	got := FindExistingConfigFile("")
	want, err := filepath.EvalSymlinks(configPath)
	if err != nil {
		t.Fatal(err)
	}
	gotClean, err := filepath.EvalSymlinks(got)
	if err != nil {
		t.Fatal(err)
	}
	if gotClean != want {
		t.Fatalf("FindExistingConfigFile() = %q, want %q", gotClean, want)
	}
}

func TestLoadConfig_ResolvesRelativePaths(t *testing.T) {
	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "config.json")
	if err := os.WriteFile(configPath, []byte(`{
		"databases": [
			{"name": "db1", "path": "./data", "stores_folders": ["./stores"]}
		]
	}`), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if got := cfg.Databases[0].Path; got != filepath.Join(tmp, "data") {
		t.Fatalf("LoadConfig() database path = %q, want %q", got, filepath.Join(tmp, "data"))
	}
	if got := cfg.Databases[0].StoresFolders[0]; got != filepath.Join(tmp, "stores") {
		t.Fatalf("LoadConfig() store folder = %q, want %q", got, filepath.Join(tmp, "stores"))
	}
}
