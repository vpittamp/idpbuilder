package stacks

import (
	"strings"
	"testing"
	"time"
)

func TestSeedImagesDefaultsDifferForCreateAndSync(t *testing.T) {
	cmd := newStacksCmd()
	createCmd, _, err := cmd.Find([]string{"create"})
	if err != nil {
		t.Fatal(err)
	}
	syncCmd, _, err := cmd.Find([]string{"sync"})
	if err != nil {
		t.Fatal(err)
	}

	if got := createCmd.Flags().Lookup("seed-images").DefValue; got != "true" {
		t.Fatalf("create --seed-images default = %s, want true", got)
	}
	if got := syncCmd.Flags().Lookup("seed-images").DefValue; got != "false" {
		t.Fatalf("sync --seed-images default = %s, want false", got)
	}
}

func TestSyncLockContentionAndRelease(t *testing.T) {
	opts := testSyncOptions(t, t.TempDir())

	first, err := acquireSyncLock(opts)
	if err != nil {
		t.Fatal(err)
	}
	_, err = acquireSyncLock(opts)
	if err == nil {
		t.Fatalf("expected lock contention")
	}
	if msg := err.Error(); !strings.Contains(msg, "cluster=test") || !strings.Contains(msg, "repo=giteaadmin/stacks") || !strings.Contains(msg, "branch=main") || !strings.Contains(msg, "cache=") {
		t.Fatalf("lock contention message missing context: %v", err)
	}
	first.release()

	second, err := acquireSyncLock(opts)
	if err != nil {
		t.Fatalf("expected lock after release, got %v", err)
	}
	second.release()
}

func TestSyncLockHeldForCallbackLifetime(t *testing.T) {
	opts := testSyncOptions(t, t.TempDir())
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		done <- withSyncLock(opts, func() error {
			close(started)
			<-release
			return nil
		})
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback to acquire lock")
	}

	if _, err := acquireSyncLock(opts); err == nil {
		t.Fatalf("expected lock contention while callback is running")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	lock, err := acquireSyncLock(opts)
	if err != nil {
		t.Fatalf("expected lock after callback exits, got %v", err)
	}
	lock.release()
}
