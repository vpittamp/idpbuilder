package stacks

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWatchAndSyncRetriesAfterSyncFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := testSyncOptions(t, t.TempDir())
	opts.WatchInterval = time.Millisecond
	opts.WatchDebounce = 5 * time.Millisecond

	hashCalls := 0
	hashFunc := func(context.Context, string) (string, error) {
		hashCalls++
		if hashCalls == 1 {
			return "initial", nil
		}
		return "changed", nil
	}
	syncCalls := 0
	syncFunc := func(context.Context, *options) (syncResult, error) {
		syncCalls++
		if syncCalls == 1 {
			return syncResult{}, errors.New("temporary failure")
		}
		cancel()
		return syncResult{
			Commit:                "1234567890abcdef",
			ChangedFiles:          []string{"packages/app/manifest.yaml"},
			AffectedApplications:  []string{"app"},
			RefreshedApplications: []string{"app"},
			Timings:               syncTimings{TotalDuration: time.Second},
		}, nil
	}

	err := watchAndSyncWithFuncs(ctx, opts, hashFunc, syncFunc)
	if err != nil {
		t.Fatalf("expected clean shutdown after successful retry, got %v", err)
	}
	if syncCalls != 2 {
		t.Fatalf("expected failed sync to be retried, got %d sync calls", syncCalls)
	}
}

func TestHotLoopReadinessVerdict(t *testing.T) {
	readyHook := giteaWebhookStatus{Ready: true, Exists: true}
	if got := hotLoopReadinessVerdict("abcdef", "abcdef", readyHook, true, nil, nil); got != "Hot loop ready" {
		t.Fatalf("ready verdict mismatch: %s", got)
	}
	if got := hotLoopReadinessVerdict("abcdef", "old", readyHook, true, nil, nil); got != "Hot loop degraded" {
		t.Fatalf("degraded revision verdict mismatch: %s", got)
	}
	if got := hotLoopReadinessVerdict("", "abcdef", readyHook, true, nil, nil); got != "Hot loop unavailable" {
		t.Fatalf("unavailable snapshot verdict mismatch: %s", got)
	}
}

func TestUnhealthyStackApplications(t *testing.T) {
	healthy := argoApplication{}
	healthy.Metadata.Name = "healthy"
	healthy.Status.Sync.Status = "Synced"
	healthy.Status.Health.Status = "Healthy"
	degraded := argoApplication{}
	degraded.Metadata.Name = "degraded"
	degraded.Status.Sync.Status = "OutOfSync"
	degraded.Status.Health.Status = "Degraded"

	problems := unhealthyStackApplications(argoApplicationList{Items: []argoApplication{healthy, degraded}})
	if len(problems) != 1 {
		t.Fatalf("expected one problem, got %#v", problems)
	}
	if problems[0].Name != "degraded" || problems[0].Sync != "OutOfSync" || problems[0].Health != "Degraded" {
		t.Fatalf("unexpected problem summary: %#v", problems[0])
	}
}
