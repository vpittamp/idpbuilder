package stacks

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestPushSnapshotNoopDoesNotCreateCommit(t *testing.T) {
	ctx := context.Background()
	source := newSourceRepo(t)
	remote := newBareRemote(t)
	opts := testSyncOptions(t, source)

	first, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.Noop || first.Commit == "" {
		t.Fatalf("expected initial sync commit, got %#v", first)
	}
	before := gitOutputTest(t, remote, "git", "rev-parse", "refs/heads/main")

	second, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Noop {
		t.Fatalf("expected no-op sync, got %#v", second)
	}
	after := gitOutputTest(t, remote, "git", "rev-parse", "refs/heads/main")
	if before != after {
		t.Fatalf("no-op sync advanced remote\nbefore: %s\nafter:  %s", before, after)
	}
}

func TestPushSnapshotIncrementalCommitIsDescendant(t *testing.T) {
	ctx := context.Background()
	source := newSourceRepo(t)
	remote := newBareRemote(t)
	opts := testSyncOptions(t, source)

	first, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(source, "tracked.txt"), "modified again")
	second, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}
	if second.Noop {
		t.Fatalf("expected second sync commit, got %#v", second)
	}
	parent := strings.TrimSpace(gitOutputTest(t, remote, "git", "rev-parse", "refs/heads/main^"))
	if parent != first.Commit {
		t.Fatalf("second sync is not a descendant\nwant parent: %s\n got parent: %s", first.Commit, parent)
	}
}

func TestPushSnapshotRemovesDeletedFilesFromCacheTree(t *testing.T) {
	ctx := context.Background()
	source := newSourceRepo(t)
	remote := newBareRemote(t)
	opts := testSyncOptions(t, source)

	if _, err := pushSnapshotToRemote(ctx, remote, opts); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(source, "tracked.txt")); err != nil {
		t.Fatal(err)
	}
	result, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Files != 1 {
		t.Fatalf("expected one changed file, got %#v", result)
	}
	tree := gitOutputTest(t, remote, "git", "ls-tree", "-r", "--name-only", "refs/heads/main")
	if strings.Contains(tree, "tracked.txt") {
		t.Fatalf("deleted file remained in remote tree:\n%s", tree)
	}
}

func TestPushSnapshotIncludesUntrackedNonIgnoredFiles(t *testing.T) {
	ctx := context.Background()
	source := newSourceRepo(t)
	remote := newBareRemote(t)
	opts := testSyncOptions(t, source)
	mustWrite(t, filepath.Join(source, "untracked.txt"), "untracked")
	mustWrite(t, filepath.Join(source, "ignored.txt"), "ignored")

	if _, err := pushSnapshotToRemote(ctx, remote, opts); err != nil {
		t.Fatal(err)
	}
	tree := gitOutputTest(t, remote, "git", "ls-tree", "-r", "--name-only", "refs/heads/main")
	if !strings.Contains(tree, "untracked.txt") {
		t.Fatalf("untracked file missing from remote tree:\n%s", tree)
	}
	if strings.Contains(tree, "ignored.txt") {
		t.Fatalf("ignored file was included in remote tree:\n%s", tree)
	}
}

func TestPushSnapshotIncludesTrackedIgnoredFiles(t *testing.T) {
	ctx := context.Background()
	source := newSourceRepo(t)
	remote := newBareRemote(t)
	opts := testSyncOptions(t, source)
	ignoredTracked := filepath.Join("deployment", "config", "gitea-values.yaml")
	mustWrite(t, filepath.Join(source, ".gitignore"), "ignored.txt\ndeployment/config/*\n")
	mustWrite(t, filepath.Join(source, ignoredTracked), "tracked but ignored")
	mustRunTest(t, source, "git", "add", ".gitignore")
	mustRunTest(t, source, "git", "add", "-f", ignoredTracked)
	mustRunTest(t, source, "git", "commit", "-m", "track ignored config")

	if _, err := pushSnapshotToRemote(ctx, remote, opts); err != nil {
		t.Fatal(err)
	}
	tree := gitOutputTest(t, remote, "git", "ls-tree", "-r", "--name-only", "refs/heads/main")
	if !strings.Contains(tree, ignoredTracked) {
		t.Fatalf("tracked ignored file missing from remote tree:\n%s", tree)
	}
}

func TestResetLocalHistoryCreatesRootCommit(t *testing.T) {
	ctx := context.Background()
	source := newSourceRepo(t)
	remote := newBareRemote(t)
	opts := testSyncOptions(t, source)
	first, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}

	mustWrite(t, filepath.Join(source, "tracked.txt"), "replacement")
	opts.ResetLocalHistory = true
	reset, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}
	if reset.Commit == first.Commit {
		t.Fatalf("reset did not replace history: %s", reset.Commit)
	}
	if err := run(context.Background(), remote, os.Environ(), "git", "rev-parse", "refs/heads/main^"); err == nil {
		t.Fatalf("reset commit unexpectedly has a parent")
	}
}

func TestMissingRemoteBranchRequiresResetLocalHistory(t *testing.T) {
	ctx := context.Background()
	source := newSourceRepo(t)
	remote := newBareRemote(t)
	seed := t.TempDir()
	mustRunTest(t, seed, "git", "init", "-b", "other")
	mustRunTest(t, seed, "git", "config", "user.name", "test")
	mustRunTest(t, seed, "git", "config", "user.email", "test@example.com")
	mustWrite(t, filepath.Join(seed, "other.txt"), "other")
	mustRunTest(t, seed, "git", "add", ".")
	mustRunTest(t, seed, "git", "commit", "-m", "other")
	mustRunTest(t, seed, "git", "push", remote, "HEAD:refs/heads/other")

	opts := testSyncOptions(t, source)
	_, err := pushSnapshotToRemote(ctx, remote, opts)
	if err == nil || !strings.Contains(err.Error(), "--reset-local-history") {
		t.Fatalf("expected reset-local-history error, got %v", err)
	}

	opts.ResetLocalHistory = true
	result, err := pushSnapshotToRemote(ctx, remote, opts)
	if err != nil {
		t.Fatal(err)
	}
	if result.Commit == "" || result.Noop {
		t.Fatalf("expected reset sync commit, got %#v", result)
	}
}

func newSourceRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	mustRunTest(t, repo, "git", "init", "-b", "main")
	mustRunTest(t, repo, "git", "config", "user.name", "test")
	mustRunTest(t, repo, "git", "config", "user.email", "test@example.com")
	mustWrite(t, filepath.Join(repo, ".gitignore"), "ignored.txt\n")
	mustWrite(t, filepath.Join(repo, "tracked.txt"), "tracked")
	mustRunTest(t, repo, "git", "add", ".")
	mustRunTest(t, repo, "git", "commit", "-m", "initial")
	return repo
}

func newBareRemote(t *testing.T) string {
	t.Helper()
	remote := filepath.Join(t.TempDir(), "remote.git")
	mustRunTest(t, "", "git", "init", "--bare", "-b", "main", remote)
	return remote
}

func testSyncOptions(t *testing.T, source string) *options {
	t.Helper()
	return &options{
		StacksRepo:    source,
		ClusterName:   "test",
		GiteaOwner:    "giteaadmin",
		GiteaRepo:     "stacks",
		GiteaUser:     "giteaadmin",
		GiteaPassword: "developer",
		Branch:        "main",
		CacheDir:      filepath.Join(t.TempDir(), "cache"),
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

func gitOutputTest(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	out, err := output(context.Background(), dir, os.Environ(), name, args...)
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(out)
}
