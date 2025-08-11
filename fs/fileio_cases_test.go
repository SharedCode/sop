package fs

import (
    "os"
    "path/filepath"
    "testing"
)

func TestFileIOBasicOps(t *testing.T) {
    dir := t.TempDir()
    fio := NewFileIO()

    // WriteFile auto-creates parent directories.
    sub := filepath.Join(dir, "a", "b", "c.txt")
    data := []byte("hello")
    if err := fio.WriteFile(ctx, sub, data, 0o644); err != nil {
        t.Fatalf("WriteFile failed: %v", err)
    }

    // Exists should see both file and its parent directory.
    cases := []struct{
        path string
        want bool
    }{
        {sub, true},
        {filepath.Dir(sub), true},
        {filepath.Join(dir, "does-not-exist"), false},
    }
    for _, c := range cases {
        if got := fio.Exists(ctx, c.path); got != c.want {
            t.Errorf("Exists(%q)=%v, want %v", c.path, got, c.want)
        }
    }

    // Read back content.
    if got, err := fio.ReadFile(ctx, sub); err != nil {
        t.Fatalf("ReadFile failed: %v", err)
    } else if string(got) != string(data) {
        t.Errorf("ReadFile content = %q, want %q", string(got), string(data))
    }

    // ReadDir should list a single file under .../a/b
    if ents, err := fio.ReadDir(ctx, filepath.Dir(sub)); err != nil {
        t.Fatalf("ReadDir failed: %v", err)
    } else if len(ents) != 1 || ents[0].Name() != "c.txt" {
        t.Errorf("ReadDir got %+v, want one entry c.txt", ents)
    }

    // Remove and RemoveAll
    if err := fio.Remove(ctx, sub); err != nil {
        t.Fatalf("Remove failed: %v", err)
    }
    if fio.Exists(ctx, sub) {
        t.Errorf("file still exists after Remove")
    }
    // RemoveAll should clean the directory tree.
    if err := fio.RemoveAll(ctx, filepath.Join(dir, "a")); err != nil {
        t.Fatalf("RemoveAll failed: %v", err)
    }
    if _, err := os.Stat(filepath.Join(dir, "a")); !os.IsNotExist(err) {
        t.Errorf("directory still exists after RemoveAll, err=%v", err)
    }
}
