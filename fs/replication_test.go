package fs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testFileIO is a controllable FileIO fake to induce failures on active or passive paths.
type testFileIO struct {
	failWriteActive  bool
	failWritePassive bool
	failMkdirActive  bool
	failMkdirPassive bool
	failReadActive   bool
	failRemoveActive bool
	data             map[string][]byte
}

func (f *testFileIO) WriteFile(_ context.Context, name string, data []byte, _ os.FileMode) error {
	// permissionType is aliased to os.FileMode in file scope; we don't depend on its value here.
	if f.failWritePassive && strings.Contains(name, "passive") {
		return errors.New("write passive failed")
	}
	if f.failWriteActive && strings.Contains(name, "active") {
		return errors.New("write active failed")
	}
	if f.data == nil {
		f.data = make(map[string][]byte)
	}
	// store payload by absolute path
	f.data[name] = append([]byte(nil), data...)
	return nil
}

func (f *testFileIO) ReadFile(_ context.Context, name string) ([]byte, error) {
	if f.failReadActive && strings.Contains(name, "active") {
		return nil, errors.New("read active failed")
	}
	if f.data == nil {
		return nil, errors.New("not found")
	}
	ba, ok := f.data[name]
	if !ok {
		return nil, errors.New("not found")
	}
	return append([]byte(nil), ba...), nil
}

func (f *testFileIO) Remove(_ context.Context, name string) error {
	if f.failRemoveActive && strings.Contains(name, "active") {
		return errors.New("remove active failed")
	}
	delete(f.data, name)
	return nil
}

func (f *testFileIO) Exists(_ context.Context, _ string) bool     { return true }
func (f *testFileIO) RemoveAll(_ context.Context, _ string) error { return nil }
func (f *testFileIO) MkdirAll(_ context.Context, path string, _ os.FileMode) error {
	if f.failMkdirActive && strings.Contains(path, "active") {
		return errors.New("mkdir active failed")
	}
	if f.failMkdirPassive && strings.Contains(path, "passive") {
		return errors.New("mkdir passive failed")
	}
	return nil
}
func (f *testFileIO) ReadDir(_ context.Context, _ string) ([]os.DirEntry, error) { return nil, nil }

// Table-driven coverage of typical failure/success paths for fileIO + replication.
func TestFileIOWithReplication_Scenarios(t *testing.T) {
	type scenarioFn func(t *testing.T, ctx context.Context, rt *replicationTracker, fio *fileIO, fake *testFileIO)

	cases := []struct {
		name     string
		fake     *testFileIO
		track    bool
		scenario scenarioFn
	}{
		{
			name:  "replicate fails on passive createStore (action type 2) and actions persist",
			fake:  &testFileIO{},
			track: true,
			scenario: func(t *testing.T, ctx context.Context, _ *replicationTracker, fio *fileIO, _ *testFileIO) {
				if err := fio.createStore(ctx, "s_passive_fail"); err != nil {
					t.Fatalf("createStore active: %v", err)
				}
				if len(fio.actionsDone) != 1 {
					t.Fatalf("expected 1 action after active create, got %d", len(fio.actionsDone))
				}
				// Induce passive mkdir failure for replicate step.
				if tf, ok := fio.fio.(*testFileIO); ok {
					tf.failMkdirPassive = true
				}
				if err := fio.replicate(ctx); err == nil {
					t.Fatalf("expected replicate mkdir passive failure")
				}
				if len(fio.actionsDone) == 0 {
					t.Fatalf("expected actions retained after failed replicate")
				}
			},
		},
		{
			name:  "createStore fails on active",
			fake:  &testFileIO{failMkdirActive: true},
			track: true,
			scenario: func(t *testing.T, ctx context.Context, rt *replicationTracker, fio *fileIO, _ *testFileIO) {
				if err := fio.createStore(ctx, "s1"); err == nil {
					t.Fatalf("expected error on createStore, got nil")
				}
				if len(fio.actionsDone) != 0 {
					t.Fatalf("expected no recorded actions on failure, got %d", len(fio.actionsDone))
				}
			},
		},
		{
			name:  "write fails on active",
			fake:  &testFileIO{failWriteActive: true},
			track: true,
			scenario: func(t *testing.T, ctx context.Context, rt *replicationTracker, fio *fileIO, _ *testFileIO) {
				if err := fio.createStore(ctx, "s2"); err != nil {
					t.Fatalf("unexpected createStore error: %v", err)
				}
				if err := fio.write(ctx, filepath.Join("s2", "file.bin"), []byte("x")); err == nil {
					t.Fatalf("expected error on write, got nil")
				}
				if len(fio.actionsDone) != 1 { // only createStore succeeded
					t.Fatalf("expected 1 recorded action, got %d", len(fio.actionsDone))
				}
			},
		},
		{
			name:  "read fails on active",
			fake:  &testFileIO{failReadActive: true},
			track: false,
			scenario: func(t *testing.T, ctx context.Context, rt *replicationTracker, fio *fileIO, _ *testFileIO) {
				if _, err := fio.read(ctx, filepath.Join("s3", "doesnotmatter.txt")); err == nil {
					t.Fatalf("expected read error, got nil")
				}
			},
		},
		{
			name:  "replicate fails on passive write and actions remain",
			fake:  &testFileIO{},
			track: true,
			scenario: func(t *testing.T, ctx context.Context, rt *replicationTracker, fio *fileIO, _ *testFileIO) {
				if err := fio.createStore(ctx, "s4"); err != nil {
					t.Fatalf("createStore: %v", err)
				}
				if err := fio.write(ctx, filepath.Join("s4", "file.bin"), []byte("payload")); err != nil {
					t.Fatalf("write active: %v", err)
				}
				if len(fio.actionsDone) != 2 {
					t.Fatalf("expected 2 recorded actions, got %d", len(fio.actionsDone))
				}
				// Enable passive write failure only for the replicate step.
				if tf, ok := FileIOSim.(*testFileIO); ok {
					tf.failWritePassive = true
				}
				if err := fio.replicate(ctx); err == nil {
					t.Fatalf("expected replicate error, got nil")
				}
				if len(fio.actionsDone) == 0 {
					t.Fatalf("expected actions to remain after failed replicate")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			active := filepath.Join(t.TempDir(), "active")
			passive := filepath.Join(t.TempDir(), "passive")
			rt, err := NewReplicationTracker(ctx, []string{active, passive}, true, nil)
			if err != nil {
				t.Fatalf("tracker: %v", err)
			}
			rt.ActiveFolderToggler = true

			old := FileIOSim
			FileIOSim = tc.fake
			defer func() { FileIOSim = old }()

			fio := newFileIOWithReplication(rt, NewManageStoreFolder(nil), tc.track)
			tc.scenario(t, ctx, rt, fio, tc.fake)
		})
	}
}
