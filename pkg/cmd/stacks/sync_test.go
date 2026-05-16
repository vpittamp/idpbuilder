package stacks

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSnapshotFilesIncludesTrackedAndUntrackedExcludesIgnoredAndDeleted(t *testing.T) {
	ctx := context.Background()
	repo := t.TempDir()
	mustRunTest(t, repo, "git", "init", "-b", "main")
	mustRunTest(t, repo, "git", "config", "user.name", "test")
	mustRunTest(t, repo, "git", "config", "user.email", "test@example.com")
	mustWrite(t, filepath.Join(repo, ".gitignore"), "ignored.txt\n")
	mustWrite(t, filepath.Join(repo, "tracked.txt"), "tracked")
	mustWrite(t, filepath.Join(repo, "deleted.txt"), "delete me")
	mustRunTest(t, repo, "git", "add", ".")
	mustRunTest(t, repo, "git", "commit", "-m", "initial")
	mustWrite(t, filepath.Join(repo, "tracked.txt"), "modified")
	mustWrite(t, filepath.Join(repo, "untracked.txt"), "untracked")
	mustWrite(t, filepath.Join(repo, "ignored.txt"), "ignored")
	if err := os.Remove(filepath.Join(repo, "deleted.txt")); err != nil {
		t.Fatal(err)
	}

	files, err := snapshotFiles(ctx, repo)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{".gitignore", "tracked.txt", "untracked.txt"}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("snapshot files mismatch\nwant: %#v\n got: %#v", want, files)
	}
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func mustRunTest(t *testing.T, dir, name string, args ...string) {
	t.Helper()
	if err := run(context.Background(), dir, os.Environ(), name, args...); err != nil {
		t.Fatal(err)
	}
}
