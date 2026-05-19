package stacks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const giteaArgoCDWebhookURL = "http://argocd-server.argocd.svc.cluster.local/api/webhook"

type syncResult struct {
	Commit                string
	ChangedFiles          []string
	AffectedApplications  []string
	RefreshedApplications []string
	SkippedFiles          []string
	ManualApplications    []string
	UnsyncedApplications  []string
	Timings               syncTimings
	Noop                  bool
}

type syncTimings struct {
	PushDuration    time.Duration
	RefreshDuration time.Duration
	WaitDuration    time.Duration
	TotalDuration   time.Duration
}

type portForward struct {
	cmd  *exec.Cmd
	port int
}

func sync(ctx context.Context, o *options) (syncResult, error) {
	start := time.Now()
	if err := ensureGitWorktree(ctx, o.StacksRepo); err != nil {
		return syncResult{}, err
	}
	if o.PrintRefreshPlan {
		return syncResult{}, printRefreshPlan(ctx, o)
	}
	pf, err := startGiteaPortForward(ctx)
	if err != nil {
		return syncResult{}, err
	}
	defer pf.stop()

	if err := ensureGiteaRepository(ctx, pf.port, o); err != nil {
		return syncResult{}, err
	}
	if err := ensureGiteaArgoCDWebhook(ctx, pf.port, o); err != nil {
		return syncResult{}, err
	}
	pushStart := time.Now()
	result, err := pushSnapshot(ctx, pf.port, o)
	if err != nil {
		return syncResult{}, err
	}
	result.Timings.PushDuration = time.Since(pushStart)
	if result.Noop {
		result.Timings.TotalDuration = time.Since(start)
		fmt.Println("No changes to sync")
		return result, nil
	}
	if err := refreshAfterSync(ctx, o, &result); err != nil {
		result.Timings.TotalDuration = time.Since(start)
		return result, fmt.Errorf("synced snapshot %s but failed to refresh ArgoCD applications: %w", result.Commit, err)
	}
	result.Timings.TotalDuration = time.Since(start)
	fmt.Printf("Synced %d changed files from %s to %s/%s:%s at %s\n", len(result.ChangedFiles), o.StacksRepo, o.GiteaOwner, o.GiteaRepo, o.Branch, result.Commit)
	if len(result.AffectedApplications) == 0 && o.RefreshMode == refreshModeAffected {
		fmt.Println("Snapshot pushed; no ArgoCD apps affected")
	} else if len(result.RefreshedApplications) > 0 {
		fmt.Printf("Refreshed ArgoCD applications: %s\n", strings.Join(result.RefreshedApplications, ", "))
	}
	if len(result.SkippedFiles) > 0 {
		fmt.Printf("Skipped non-rendered files: %s\n", strings.Join(result.SkippedFiles, ", "))
	}
	if len(result.ManualApplications) > 0 {
		fmt.Printf("Manual applications requiring operator sync: %s\n", strings.Join(result.ManualApplications, ", "))
	}
	if len(result.UnsyncedApplications) > 0 {
		fmt.Printf("Applications not Synced after refresh: %s\n", strings.Join(result.UnsyncedApplications, ", "))
	}
	printSyncTimings(result.Timings)
	return result, nil
}

func printSyncTimings(t syncTimings) {
	fmt.Printf("Sync timings: push=%s refresh=%s wait=%s total=%s\n",
		roundDuration(t.PushDuration),
		roundDuration(t.RefreshDuration),
		roundDuration(t.WaitDuration),
		roundDuration(t.TotalDuration),
	)
}

func roundDuration(value time.Duration) time.Duration {
	if value <= 0 {
		return 0
	}
	return value.Round(time.Millisecond)
}

func refreshAfterSync(ctx context.Context, o *options, result *syncResult) error {
	switch o.RefreshMode {
	case refreshModeNone:
		return nil
	case refreshModeAll:
		apps, err := listStackApplications(ctx, o)
		if err != nil {
			return err
		}
		names := stackApplicationNamesFromList(apps)
		result.AffectedApplications = appendUniqueStrings(result.AffectedApplications, names...)
		if !containsString(names, rootApplicationName) {
			if err := timedRefreshApplications(ctx, o, result, names); err != nil {
				return err
			}
			result.RefreshedApplications = appendUniqueStrings(result.RefreshedApplications, names...)
			unsynced, err := timedWaitForApplicationsObserved(ctx, o, result, names)
			result.UnsyncedApplications = appendUniqueStrings(result.UnsyncedApplications, unsynced...)
			return err
		}

		if err := timedRefreshApplications(ctx, o, result, []string{rootApplicationName}); err != nil {
			return err
		}
		result.RefreshedApplications = appendUniqueStrings(result.RefreshedApplications, rootApplicationName)
		unsynced, err := timedWaitForApplicationsObserved(ctx, o, result, []string{rootApplicationName})
		result.UnsyncedApplications = appendUniqueStrings(result.UnsyncedApplications, unsynced...)
		if err != nil {
			return err
		}
		apps, err = listStackApplications(ctx, o)
		if err != nil {
			return err
		}
		names = stackApplicationNamesFromList(apps)
		result.AffectedApplications = appendUniqueStrings(result.AffectedApplications, names...)
		children := withoutString(names, rootApplicationName)
		if len(children) == 0 {
			return nil
		}
		if err := timedRefreshApplications(ctx, o, result, children); err != nil {
			return err
		}
		result.RefreshedApplications = appendUniqueStrings(result.RefreshedApplications, children...)
		unsynced, err = timedWaitForApplicationsObserved(ctx, o, result, children)
		result.UnsyncedApplications = appendUniqueStrings(result.UnsyncedApplications, unsynced...)
		return err
	case refreshModeAffected:
		apps, err := listStackApplications(ctx, o)
		if err != nil {
			return err
		}
		plan, err := planAffectedApplications(o.StacksRepo, apps, result.ChangedFiles)
		if err != nil {
			return err
		}
		applyPlanToResult(result, plan)
		if len(plan.AffectedApplications) == 0 {
			return nil
		}
		if plan.RootFirst {
			if err := timedRefreshApplications(ctx, o, result, []string{rootApplicationName}); err != nil {
				return err
			}
			result.RefreshedApplications = appendUniqueStrings(result.RefreshedApplications, rootApplicationName)
			unsynced, err := timedWaitForApplicationsObserved(ctx, o, result, []string{rootApplicationName})
			result.UnsyncedApplications = appendUniqueStrings(result.UnsyncedApplications, unsynced...)
			if err != nil {
				return err
			}
			apps, err = listStackApplications(ctx, o)
			if err != nil {
				return err
			}
			plan, err = planAffectedApplications(o.StacksRepo, apps, result.ChangedFiles)
			if err != nil {
				return err
			}
			replacePlanInResult(result, plan)
		}
		children := withoutString(plan.AffectedApplications, rootApplicationName)
		if len(children) == 0 {
			return nil
		}
		if err := timedRefreshApplications(ctx, o, result, children); err != nil {
			return err
		}
		result.RefreshedApplications = appendUniqueStrings(result.RefreshedApplications, children...)
		unsynced, err := timedWaitForApplicationsObserved(ctx, o, result, children)
		result.UnsyncedApplications = appendUniqueStrings(result.UnsyncedApplications, unsynced...)
		return err
	default:
		return fmt.Errorf("unsupported refresh mode %q", o.RefreshMode)
	}
}

func timedRefreshApplications(ctx context.Context, o *options, result *syncResult, names []string) error {
	start := time.Now()
	err := refreshApplications(ctx, o, names)
	result.Timings.RefreshDuration += time.Since(start)
	return err
}

func timedWaitForApplicationsObserved(ctx context.Context, o *options, result *syncResult, names []string) ([]string, error) {
	start := time.Now()
	unsynced, err := waitForApplicationsObserved(ctx, o, names, result.Commit)
	result.Timings.WaitDuration += time.Since(start)
	return unsynced, err
}

func ensureGitWorktree(ctx context.Context, repo string) error {
	out, err := output(ctx, repo, os.Environ(), "git", "rev-parse", "--is-inside-work-tree")
	if err != nil {
		return fmt.Errorf("%s is not a git worktree: %w", repo, err)
	}
	if strings.TrimSpace(out) != "true" {
		return fmt.Errorf("%s is not a git worktree", repo)
	}
	return nil
}

func startGiteaPortForward(ctx context.Context) (*portForward, error) {
	port, err := freeLocalPort()
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "kubectl", "port-forward", "-n", "gitea", "svc/gitea-http", fmt.Sprintf("%d:3000", port))
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting gitea port-forward: %w", err)
	}
	pf := &portForward{cmd: cmd, port: port}
	if err := waitForHTTP(ctx, fmt.Sprintf("http://127.0.0.1:%d/api/v1/version", port), 30*time.Second); err != nil {
		pf.stop()
		return nil, err
	}
	return pf, nil
}

func (p *portForward) stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Kill()
	_, _ = p.cmd.Process.Wait()
}

func freeLocalPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("allocating local port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func waitForHTTP(ctx context.Context, target string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 500 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out waiting for %s", target)
}

func ensureGiteaRepository(ctx context.Context, port int, o *options) error {
	if exists, err := giteaRepositoryExists(ctx, port, o); err != nil {
		return err
	} else if exists {
		return nil
	}

	body, err := json.Marshal(map[string]any{
		"name":           o.GiteaRepo,
		"private":        false,
		"auto_init":      false,
		"default_branch": o.Branch,
	})
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/api/v1/user/repos", port)
	var lastErr error
	for attempt := 1; attempt <= 6; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(body)))
		if err != nil {
			return err
		}
		req.SetBasicAuth(o.GiteaUser, o.GiteaPassword)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("creating Gitea repository %s/%s: %w", o.GiteaOwner, o.GiteaRepo, err)
		} else {
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			if resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusUnprocessableEntity {
				return nil
			}
			lastErr = fmt.Errorf("creating Gitea repository %s/%s returned %s: %s", o.GiteaOwner, o.GiteaRepo, resp.Status, strings.TrimSpace(string(data)))
			if resp.StatusCode < 500 {
				return lastErr
			}
		}
		if exists, err := giteaRepositoryExists(ctx, port, o); err != nil {
			lastErr = err
		} else if exists {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(time.Duration(attempt) * time.Second):
		}
	}
	return lastErr
}

func giteaRepositoryExists(ctx context.Context, port int, o *options) (bool, error) {
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/api/v1/repos/%s/%s", port, url.PathEscape(o.GiteaOwner), url.PathEscape(o.GiteaRepo))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return false, err
	}
	req.SetBasicAuth(o.GiteaUser, o.GiteaPassword)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("checking Gitea repository %s/%s: %w", o.GiteaOwner, o.GiteaRepo, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return false, fmt.Errorf("checking Gitea repository %s/%s returned %s: %s", o.GiteaOwner, o.GiteaRepo, resp.Status, strings.TrimSpace(string(data)))
	}
}

type giteaHook struct {
	ID     int               `json:"id"`
	Type   string            `json:"type"`
	Active bool              `json:"active"`
	Events []string          `json:"events"`
	Config map[string]string `json:"config"`
}

func ensureGiteaArgoCDWebhook(ctx context.Context, port int, o *options) error {
	hooks, err := listGiteaSystemHooks(ctx, port, o)
	if err != nil {
		return err
	}
	status := classifyGiteaArgoCDWebhook(hooks)
	if status.Ready {
		return nil
	}
	if status.Exists {
		return fmt.Errorf("Gitea ArgoCD system webhook %d targets %s but %s", status.ID, status.URL, status.Message)
	}
	return createGiteaArgoCDWebhook(ctx, port, o, giteaArgoCDWebhookURL)
}

type giteaWebhookStatus struct {
	ID      int
	URL     string
	Ready   bool
	Exists  bool
	Message string
}

func classifyGiteaArgoCDWebhook(hooks []giteaHook) giteaWebhookStatus {
	for _, hook := range hooks {
		if hook.Config["url"] != giteaArgoCDWebhookURL {
			continue
		}
		status := giteaWebhookStatus{
			ID:     hook.ID,
			URL:    giteaArgoCDWebhookURL,
			Exists: true,
		}
		switch {
		case !hook.Active:
			status.Message = "is inactive"
		case !hookHasEvent(hook, "push"):
			status.Message = "is missing push events"
		default:
			status.Ready = true
			status.Message = "active for push events"
		}
		return status
	}
	return giteaWebhookStatus{
		URL:     giteaArgoCDWebhookURL,
		Message: "missing",
	}
}

func listGiteaSystemHooks(ctx context.Context, port int, o *options) ([]giteaHook, error) {
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/api/v1/admin/hooks", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(o.GiteaUser, o.GiteaPassword)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("checking Gitea system webhooks: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("checking Gitea system webhooks returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
	}
	var hooks []giteaHook
	if err := json.Unmarshal(data, &hooks); err != nil {
		return nil, fmt.Errorf("parsing Gitea system webhooks: %w", err)
	}
	return hooks, nil
}

func createGiteaArgoCDWebhook(ctx context.Context, port int, o *options, webhookURL string) error {
	body, err := json.Marshal(map[string]any{
		"type":   "gitea",
		"active": true,
		"events": []string{"push"},
		"config": map[string]string{
			"url":               webhookURL,
			"content_type":      "json",
			"is_system_webhook": "true",
		},
	})
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("http://127.0.0.1:%d/api/v1/admin/hooks", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.SetBasicAuth(o.GiteaUser, o.GiteaPassword)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("creating Gitea ArgoCD system webhook: %w", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("creating Gitea ArgoCD system webhook returned %s: %s", resp.Status, strings.TrimSpace(string(data)))
}

func hookHasEvent(hook giteaHook, event string) bool {
	for _, candidate := range hook.Events {
		if candidate == event {
			return true
		}
	}
	return false
}

func pushSnapshot(ctx context.Context, port int, o *options) (syncResult, error) {
	return pushSnapshotToRemote(ctx, giteaRemoteURL(port, o), o)
}

func pushSnapshotToRemote(ctx context.Context, remote string, o *options) (syncResult, error) {
	files, err := snapshotFiles(ctx, o.StacksRepo)
	if err != nil {
		return syncResult{}, err
	}
	cacheDir, err := syncCachePath(o)
	if err != nil {
		return syncResult{}, err
	}
	if err := prepareSyncCache(ctx, cacheDir, remote, o); err != nil {
		return syncResult{}, err
	}
	reconcileFiles, forceBootstrapRewrite, err := planCacheReconcileFiles(o.StacksRepo, cacheDir, files, o)
	if err != nil {
		return syncResult{}, err
	}
	touched, err := reconcileCacheWorktree(o.StacksRepo, cacheDir, reconcileFiles, files)
	if err != nil {
		return syncResult{}, err
	}
	if o.RewriteBootstrapImagePins && (forceBootstrapRewrite || shouldRewriteBootstrapImagePins(touched)) {
		if err := rewriteBootstrapImagePins(ctx, o, cacheDir); err != nil {
			return syncResult{}, err
		}
	}
	if err := run(ctx, cacheDir, os.Environ(), "git", "add", "-A"); err != nil {
		return syncResult{}, err
	}
	if err := forceAddSnapshotFiles(ctx, cacheDir, files); err != nil {
		return syncResult{}, err
	}
	changed, err := stagedChangedFiles(ctx, cacheDir)
	if err != nil {
		return syncResult{}, err
	}
	if len(changed) == 0 {
		if o.RewriteBootstrapImagePins {
			_ = saveBootstrapRewriteMetadata(o.StacksRepo, cacheDir, files)
		}
		return syncResult{Noop: true}, nil
	}
	message := fmt.Sprintf("sync stacks snapshot %s", time.Now().Format(time.RFC3339))
	if err := run(ctx, cacheDir, os.Environ(), "git", "commit", "-m", message); err != nil {
		return syncResult{}, err
	}
	commit, err := output(ctx, cacheDir, os.Environ(), "git", "rev-parse", "HEAD")
	if err != nil {
		return syncResult{}, err
	}
	args := []string{"push", "origin", "HEAD:refs/heads/" + o.Branch}
	if o.ResetLocalHistory {
		args = []string{"push", "--force", "origin", "HEAD:refs/heads/" + o.Branch}
	}
	if _, err := output(ctx, cacheDir, os.Environ(), "git", args...); err != nil {
		if !o.ResetLocalHistory {
			return syncResult{}, fmt.Errorf("Refusing non-fast-forward push; run with --reset-local-history to replace local Gitea history: %w", err)
		}
		return syncResult{}, err
	}
	if o.RewriteBootstrapImagePins {
		_ = saveBootstrapRewriteMetadata(o.StacksRepo, cacheDir, files)
	}
	return syncResult{Commit: strings.TrimSpace(commit), ChangedFiles: changed}, nil
}

func syncCachePath(o *options) (string, error) {
	if o.CacheDir != "" {
		return filepath.Abs(o.CacheDir)
	}
	root := os.Getenv("XDG_CACHE_HOME")
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolving home directory for sync cache: %w", err)
		}
		root = filepath.Join(home, ".cache")
	}
	return filepath.Join(root, "idpbuilder", "stacks-sync", safeCacheSegment(o.ClusterName), safeCacheSegment(o.GiteaOwner), safeCacheSegment(o.GiteaRepo)), nil
}

func safeCacheSegment(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, string(filepath.Separator), "_")
	value = strings.ReplaceAll(value, "/", "_")
	value = strings.ReplaceAll(value, "\\", "_")
	if value == "" || value == "." || value == ".." {
		return "_"
	}
	return value
}

func prepareSyncCache(ctx context.Context, cacheDir, remote string, o *options) error {
	if _, err := os.Stat(filepath.Join(cacheDir, ".git")); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("checking sync cache %s: %w", cacheDir, err)
		}
		if err := os.RemoveAll(cacheDir); err != nil {
			return fmt.Errorf("clearing invalid sync cache %s: %w", cacheDir, err)
		}
		if err := os.MkdirAll(filepath.Dir(cacheDir), 0o755); err != nil {
			return fmt.Errorf("creating sync cache parent: %w", err)
		}
		if err := run(ctx, filepath.Dir(cacheDir), os.Environ(), "git", "clone", remote, cacheDir); err != nil {
			return err
		}
	} else {
		if err := run(ctx, cacheDir, os.Environ(), "git", "remote", "set-url", "origin", remote); err != nil {
			return err
		}
	}
	if err := run(ctx, cacheDir, os.Environ(), "git", "config", "user.name", "idpbuilder-stacks"); err != nil {
		return err
	}
	if err := run(ctx, cacheDir, os.Environ(), "git", "config", "user.email", "idpbuilder-stacks@cnoe.local"); err != nil {
		return err
	}
	if err := resetCacheGitState(ctx, cacheDir); err != nil {
		return err
	}
	if o.ResetLocalHistory {
		return checkoutOrphan(ctx, cacheDir, o.Branch)
	}
	if err := run(ctx, cacheDir, os.Environ(), "git", "fetch", "--prune", "origin"); err != nil {
		return err
	}
	if exists, err := gitRefExists(ctx, cacheDir, "refs/remotes/origin/"+o.Branch); err != nil {
		return err
	} else if exists {
		return run(ctx, cacheDir, os.Environ(), "git", "checkout", "-B", o.Branch, "refs/remotes/origin/"+o.Branch)
	}
	hasHeads, err := remoteHasAnyHead(ctx, cacheDir)
	if err != nil {
		return err
	}
	if hasHeads {
		return fmt.Errorf("Refusing non-fast-forward push; run with --reset-local-history to replace local Gitea history: remote branch %q is missing", o.Branch)
	}
	return checkoutOrphan(ctx, cacheDir, o.Branch)
}

func resetCacheGitState(ctx context.Context, cacheDir string) error {
	hasHead, err := gitRefExists(ctx, cacheDir, "HEAD")
	if err != nil {
		return err
	}
	if !hasHead {
		return nil
	}
	if err := run(ctx, cacheDir, os.Environ(), "git", "reset", "--hard"); err != nil {
		return err
	}
	return run(ctx, cacheDir, os.Environ(), "git", "clean", "-fdx")
}

func checkoutOrphan(ctx context.Context, dir, branch string) error {
	orphanBranch := fmt.Sprintf("idpbuilder-%s-reset-%d", safeCacheSegment(branch), time.Now().UnixNano())
	if err := run(ctx, dir, os.Environ(), "git", "checkout", "--orphan", orphanBranch); err != nil {
		return err
	}
	return nil
}

func gitRefExists(ctx context.Context, dir, ref string) (bool, error) {
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "--verify", "--quiet", ref)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 {
		return false, nil
	}
	return false, fmt.Errorf("checking git ref %s: %s", ref, strings.TrimSpace(stderr.String()))
}

func remoteHasAnyHead(ctx context.Context, dir string) (bool, error) {
	out, err := output(ctx, dir, os.Environ(), "git", "ls-remote", "--heads", "origin")
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(out) != "", nil
}

func planCacheReconcileFiles(srcRoot, dstRoot string, files []string, o *options) ([]string, bool, error) {
	if !o.RewriteBootstrapImagePins {
		return files, false, nil
	}
	metadata, err := loadBootstrapRewriteMetadata(dstRoot)
	if err != nil {
		metadata = map[string]bootstrapRewriteMetadataEntry{}
	}
	reconcileFiles := make([]string, 0, len(files))
	forceBootstrapRewrite := false
	for _, file := range files {
		if !isBootstrapRewriteInputPath(file) {
			reconcileFiles = append(reconcileFiles, file)
			continue
		}
		hash, err := hashSnapshotPath(srcRoot, file)
		if err != nil {
			return nil, false, err
		}
		entry := metadata[file]
		if entry.SourceHash == hash {
			cacheHash, err := hashSnapshotPath(dstRoot, file)
			if err == nil && entry.CacheHash == cacheHash {
				continue
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, false, err
			}
		}
		reconcileFiles = append(reconcileFiles, file)
		forceBootstrapRewrite = true
	}
	return reconcileFiles, forceBootstrapRewrite, nil
}

type bootstrapRewriteMetadataEntry struct {
	SourceHash string `json:"sourceHash"`
	CacheHash  string `json:"cacheHash"`
}

func loadBootstrapRewriteMetadata(cacheDir string) (map[string]bootstrapRewriteMetadataEntry, error) {
	path := bootstrapRewriteMetadataPath(cacheDir)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bootstrapRewriteMetadataEntry{}, nil
		}
		return nil, fmt.Errorf("reading bootstrap rewrite metadata: %w", err)
	}
	metadata := map[string]bootstrapRewriteMetadataEntry{}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("parsing bootstrap rewrite metadata: %w", err)
	}
	return metadata, nil
}

func saveBootstrapRewriteMetadata(srcRoot, cacheDir string, files []string) error {
	metadata := map[string]bootstrapRewriteMetadataEntry{}
	for _, file := range files {
		if !isBootstrapRewriteInputPath(file) {
			continue
		}
		sourceHash, err := hashSnapshotPath(srcRoot, file)
		if err != nil {
			return err
		}
		cacheHash, err := hashSnapshotPath(cacheDir, file)
		if err != nil {
			return err
		}
		metadata[file] = bootstrapRewriteMetadataEntry{
			SourceHash: sourceHash,
			CacheHash:  cacheHash,
		}
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	path := bootstrapRewriteMetadataPath(cacheDir)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("writing bootstrap rewrite metadata: %w", err)
	}
	return nil
}

func bootstrapRewriteMetadataPath(cacheDir string) string {
	return filepath.Join(cacheDir, ".git", "idpbuilder-bootstrap-source-hashes.json")
}

func hashSnapshotPath(srcRoot, rel string) (string, error) {
	data, err := os.ReadFile(filepath.Join(srcRoot, rel))
	if err != nil {
		return "", fmt.Errorf("reading %s: %w", rel, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func reconcileCacheWorktree(srcRoot, dstRoot string, files, wantedFiles []string) ([]string, error) {
	wanted := make(map[string]bool, len(wantedFiles))
	for _, file := range wantedFiles {
		wanted[filepath.ToSlash(file)] = true
	}
	touched, err := removeStaleCachePaths(dstRoot, wanted)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if same, err := snapshotPathMatches(srcRoot, dstRoot, file); err != nil {
			return nil, err
		} else if same {
			continue
		}
		if err := copySnapshotPath(srcRoot, dstRoot, file); err != nil {
			return nil, err
		}
		touched = append(touched, filepath.ToSlash(file))
	}
	if err := removeEmptyCacheDirs(dstRoot); err != nil {
		return nil, err
	}
	return sortedUniqueStrings(touched), nil
}

func removeStaleCachePaths(dstRoot string, wanted map[string]bool) ([]string, error) {
	var removed []string
	err := filepath.WalkDir(dstRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dstRoot {
			return nil
		}
		rel, err := filepath.Rel(dstRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == ".git" || strings.HasPrefix(rel, ".git/") {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			if wanted[rel] {
				if err := os.RemoveAll(path); err != nil {
					return fmt.Errorf("removing stale cache directory %s: %w", rel, err)
				}
				removed = append(removed, rel)
				return filepath.SkipDir
			}
			return nil
		}
		if !wanted[rel] {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("removing stale cache file %s: %w", rel, err)
			}
			removed = append(removed, rel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return removed, nil
}

func removeEmptyCacheDirs(dstRoot string) error {
	var dirs []string
	if err := filepath.WalkDir(dstRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dstRoot {
			return nil
		}
		rel, err := filepath.Rel(dstRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == ".git" || strings.HasPrefix(rel, ".git/") {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		if len(entries) == 0 {
			if err := os.Remove(dir); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func snapshotPathMatches(srcRoot, dstRoot, rel string) (bool, error) {
	src := filepath.Join(srcRoot, rel)
	dst := filepath.Join(dstRoot, rel)
	srcInfo, err := os.Lstat(src)
	if err != nil {
		return false, fmt.Errorf("stat %s: %w", src, err)
	}
	dstInfo, err := os.Lstat(dst)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat %s: %w", dst, err)
	}
	if srcInfo.Mode()&os.ModeSymlink != 0 {
		if dstInfo.Mode()&os.ModeSymlink == 0 {
			return false, nil
		}
		srcTarget, err := os.Readlink(src)
		if err != nil {
			return false, fmt.Errorf("reading symlink %s: %w", src, err)
		}
		dstTarget, err := os.Readlink(dst)
		if err != nil {
			return false, fmt.Errorf("reading symlink %s: %w", dst, err)
		}
		return srcTarget == dstTarget, nil
	}
	if !srcInfo.Mode().IsRegular() {
		return false, fmt.Errorf("unsupported snapshot file mode %s for %s", srcInfo.Mode(), rel)
	}
	if !dstInfo.Mode().IsRegular() {
		return false, nil
	}
	if srcInfo.Mode().Perm() != dstInfo.Mode().Perm() || srcInfo.Size() != dstInfo.Size() {
		return false, nil
	}
	equal, err := regularFilesEqual(src, dst)
	if err != nil {
		return false, err
	}
	if !equal {
		return false, nil
	}
	if err := syncSnapshotPathMetadata(srcInfo, dst); err != nil {
		return false, err
	}
	return true, nil
}

func shouldRewriteBootstrapImagePins(touched []string) bool {
	for _, file := range touched {
		if isBootstrapRewriteInputPath(file) {
			return true
		}
	}
	return false
}

func isBootstrapRewriteInputPath(file string) bool {
	file = filepath.ToSlash(file)
	if file == "packages/components/hub-spoke-appsets/release-pins/workflow-builder-images.yaml" {
		return true
	}
	return strings.HasPrefix(file, "packages/components/active-development/manifests/") && isKustomizationFileName(filepath.Base(file))
}

func isKustomizationFileName(name string) bool {
	switch name {
	case "kustomization.yaml", "kustomization.yml", "Kustomization":
		return true
	default:
		return false
	}
}

func sortedUniqueStrings(values []string) []string {
	seen := map[string]bool{}
	for _, value := range values {
		if value != "" {
			seen[value] = true
		}
	}
	return sortedKeys(seen)
}

func stagedChangedFiles(ctx context.Context, dir string) ([]string, error) {
	out, err := output(ctx, dir, os.Environ(), "git", "diff", "--cached", "--name-only", "-z")
	if err != nil {
		return nil, err
	}
	parts := strings.Split(out, "\x00")
	files := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			files = append(files, part)
		}
	}
	sort.Strings(files)
	return files, nil
}

func rewriteBootstrapImagePins(ctx context.Context, o *options, snapshotDir string) error {
	script := filepath.Join(o.StacksRepo, "deployment", "scripts", "bootstrap", "seed-ryzen-images.sh")
	args := []string{
		script,
		"--mode", "critical",
		"--rewrite-kustomizations", snapshotDir,
		"--rewrite-registry", bootstrapImageRewriteRegistry(o),
		"--skip-copy",
		"--quiet",
	}
	if o.SeedImagesMode == "release-pins" {
		args = append(args, "--pins", filepath.Join(o.StacksRepo, "packages", "components", "hub-spoke-appsets", "release-pins", "workflow-builder-images.yaml"))
	}
	if err := run(ctx, o.StacksRepo, withStacksEnv(o), "bash", args...); err != nil {
		return fmt.Errorf("rewriting bootstrap image pins: %w", err)
	}
	return nil
}

func bootstrapImageRewriteRegistry(o *options) string {
	if o.Provider == providerTalosDocker {
		return talosDockerHostRegistry(o)
	}
	return fmt.Sprintf("gitea.cnoe.localtest.me/%s", o.GiteaOwner)
}

func giteaRemoteURL(port int, o *options) string {
	u := url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("127.0.0.1:%d", port),
		Path:   fmt.Sprintf("/%s/%s.git", o.GiteaOwner, o.GiteaRepo),
		User:   url.UserPassword(o.GiteaUser, o.GiteaPassword),
	}
	return u.String()
}

func snapshotFiles(ctx context.Context, repo string) ([]string, error) {
	out, err := output(ctx, repo, os.Environ(), "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	if err != nil {
		return nil, err
	}
	parts := strings.Split(out, "\x00")
	files := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		clean := filepath.Clean(part)
		if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) || clean == ".." {
			return nil, fmt.Errorf("unsafe git path %q", part)
		}
		src := filepath.Join(repo, clean)
		st, err := os.Lstat(src)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("stat %s: %w", src, err)
		}
		if st.IsDir() {
			continue
		}
		files = append(files, clean)
	}
	sort.Strings(files)
	return files, nil
}

func copySnapshotPath(srcRoot, dstRoot, rel string) error {
	src := filepath.Join(srcRoot, rel)
	dst := filepath.Join(dstRoot, rel)
	st, err := os.Lstat(src)
	if err != nil {
		return fmt.Errorf("stat %s: %w", src, err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("creating destination dir for %s: %w", dst, err)
	}
	if st.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(src)
		if err != nil {
			return fmt.Errorf("reading symlink %s: %w", src, err)
		}
		return os.Symlink(target, dst)
	}
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("opening %s: %w", src, err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, st.Mode().Perm())
	if err != nil {
		return fmt.Errorf("creating %s: %w", dst, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return fmt.Errorf("copying %s: %w", rel, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", dst, err)
	}
	if err := syncSnapshotPathMetadata(st, dst); err != nil {
		return err
	}
	return nil
}

func syncSnapshotPathMetadata(srcInfo os.FileInfo, dst string) error {
	if err := os.Chmod(dst, srcInfo.Mode().Perm()); err != nil {
		return fmt.Errorf("chmod %s: %w", dst, err)
	}
	if err := os.Chtimes(dst, srcInfo.ModTime(), srcInfo.ModTime()); err != nil {
		return fmt.Errorf("preserving mtime for %s: %w", dst, err)
	}
	return nil
}

func regularFilesEqual(first, second string) (bool, error) {
	left, err := os.Open(first)
	if err != nil {
		return false, fmt.Errorf("opening %s: %w", first, err)
	}
	defer left.Close()
	right, err := os.Open(second)
	if err != nil {
		return false, fmt.Errorf("opening %s: %w", second, err)
	}
	defer right.Close()

	leftBuffer := make([]byte, 32*1024)
	rightBuffer := make([]byte, 32*1024)
	for {
		leftN, leftErr := left.Read(leftBuffer)
		rightN, rightErr := right.Read(rightBuffer)
		if leftN != rightN || !bytes.Equal(leftBuffer[:leftN], rightBuffer[:rightN]) {
			return false, nil
		}
		if leftErr == io.EOF && rightErr == io.EOF {
			return true, nil
		}
		if leftErr != nil && leftErr != io.EOF {
			return false, fmt.Errorf("reading %s: %w", first, leftErr)
		}
		if rightErr != nil && rightErr != io.EOF {
			return false, fmt.Errorf("reading %s: %w", second, rightErr)
		}
		if leftErr == io.EOF || rightErr == io.EOF {
			return false, nil
		}
	}
}

func forceAddSnapshotFiles(ctx context.Context, cacheDir string, files []string) error {
	if len(files) == 0 {
		return nil
	}
	pathspec, err := os.CreateTemp("", "idpbuilder-stacks-pathspec-*")
	if err != nil {
		return fmt.Errorf("creating git pathspec file: %w", err)
	}
	pathspecName := pathspec.Name()
	defer os.Remove(pathspecName)
	for _, file := range files {
		if _, err := pathspec.WriteString(file); err != nil {
			_ = pathspec.Close()
			return fmt.Errorf("writing git pathspec file: %w", err)
		}
		if _, err := pathspec.Write([]byte{0}); err != nil {
			_ = pathspec.Close()
			return fmt.Errorf("writing git pathspec file: %w", err)
		}
	}
	if err := pathspec.Close(); err != nil {
		return fmt.Errorf("closing git pathspec file: %w", err)
	}
	return run(ctx, cacheDir, os.Environ(), "git", "add", "-A", "--force", "--pathspec-from-file="+pathspecName, "--pathspec-file-nul")
}

func snapshotHash(ctx context.Context, repo string) (string, error) {
	files, err := snapshotFiles(ctx, repo)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, file := range files {
		_, _ = h.Write([]byte(file))
		_, _ = h.Write([]byte{0})
		data, err := os.ReadFile(filepath.Join(repo, file))
		if err != nil {
			return "", fmt.Errorf("reading %s: %w", file, err)
		}
		_, _ = h.Write(data)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type argoApplicationList struct {
	Items []argoApplication `json:"items"`
}

type argoApplication struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Spec struct {
		Source     argoApplicationSource   `json:"source"`
		Sources    []argoApplicationSource `json:"sources"`
		SyncPolicy struct {
			Automated map[string]any `json:"automated"`
		} `json:"syncPolicy"`
	} `json:"spec"`
	Status struct {
		Sync struct {
			Status    string   `json:"status"`
			Revision  string   `json:"revision"`
			Revisions []string `json:"revisions"`
		} `json:"sync"`
		OperationState struct {
			Phase string `json:"phase"`
		} `json:"operationState"`
	} `json:"status"`
}

type argoApplicationSource struct {
	RepoURL        string `json:"repoURL"`
	Path           string `json:"path"`
	TargetRevision string `json:"targetRevision"`
}

func refreshStackApplications(ctx context.Context, o *options) error {
	names, err := stackApplicationNames(ctx, o)
	if err != nil {
		return err
	}
	return refreshApplications(ctx, o, names)
}

func refreshApplications(ctx context.Context, o *options, names []string) error {
	var errs []error
	for _, name := range names {
		if err := run(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "annotate", "application", name, "-n", "argocd", "argocd.argoproj.io/refresh=hard", "--overwrite"); err != nil {
			errs = append(errs, fmt.Errorf("refreshing ArgoCD application %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func stackApplicationNames(ctx context.Context, o *options) ([]string, error) {
	apps, err := listStackApplications(ctx, o)
	if err != nil {
		return nil, err
	}
	names := stackApplicationNamesFromList(apps)
	if len(names) == 0 {
		return nil, fmt.Errorf("no ArgoCD applications source repo /%s/%s.git", o.GiteaOwner, o.GiteaRepo)
	}
	return names, nil
}

func stackApplicationNamesFromList(apps argoApplicationList) []string {
	seen := map[string]bool{}
	for _, app := range apps.Items {
		if app.Metadata.Name != "" {
			seen[app.Metadata.Name] = true
		}
	}
	return sortedKeys(seen)
}

func listStackApplications(ctx context.Context, o *options) (argoApplicationList, error) {
	raw, err := output(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "get", "application", "-n", "argocd", "-o", "json")
	if err != nil {
		return argoApplicationList{}, err
	}
	var apps argoApplicationList
	if err := json.Unmarshal([]byte(raw), &apps); err != nil {
		return argoApplicationList{}, fmt.Errorf("parsing ArgoCD applications: %w", err)
	}
	suffix := fmt.Sprintf("/%s/%s.git", o.GiteaOwner, o.GiteaRepo)
	filtered := argoApplicationList{}
	for _, app := range apps.Items {
		if applicationUsesRepo(app, suffix) {
			if !repoURLMatches(app.Spec.Source.RepoURL, suffix) {
				app.Spec.Source = argoApplicationSource{}
			}
			localSources := make([]argoApplicationSource, 0, len(app.Spec.Sources))
			for _, source := range app.Spec.Sources {
				if repoURLMatches(source.RepoURL, suffix) {
					localSources = append(localSources, source)
				}
			}
			app.Spec.Sources = localSources
			filtered.Items = append(filtered.Items, app)
		}
	}
	if len(filtered.Items) == 0 {
		return argoApplicationList{}, fmt.Errorf("no ArgoCD applications source repo %s", suffix)
	}
	return filtered, nil
}

func matchingStackApplicationNames(apps argoApplicationList, suffix string) []string {
	names := make([]string, 0, len(apps.Items))
	seen := map[string]bool{}
	for _, app := range apps.Items {
		if app.Metadata.Name == "" {
			continue
		}
		if applicationUsesRepo(app, suffix) {
			seen[app.Metadata.Name] = true
		}
	}
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func applicationUsesRepo(app argoApplication, repoSuffix string) bool {
	if repoURLMatches(app.Spec.Source.RepoURL, repoSuffix) {
		return true
	}
	for _, source := range app.Spec.Sources {
		if repoURLMatches(source.RepoURL, repoSuffix) {
			return true
		}
	}
	return false
}

func repoURLMatches(repoURL, repoSuffix string) bool {
	normalized := strings.TrimSuffix(strings.TrimSpace(repoURL), "/")
	return normalized == strings.TrimPrefix(repoSuffix, "/") || strings.HasSuffix(normalized, repoSuffix)
}

func (app argoApplication) localSources() []argoApplicationSource {
	sources := make([]argoApplicationSource, 0, 1+len(app.Spec.Sources))
	if app.Spec.Source.RepoURL != "" {
		sources = append(sources, app.Spec.Source)
	}
	sources = append(sources, app.Spec.Sources...)
	return sources
}

func applyPlanToResult(result *syncResult, plan refreshPlan) {
	result.AffectedApplications = appendUniqueStrings(result.AffectedApplications, plan.AffectedApplications...)
	result.SkippedFiles = appendUniqueStrings(result.SkippedFiles, plan.SkippedFiles...)
	result.ManualApplications = appendUniqueStrings(result.ManualApplications, plan.ManualApplications...)
}

func replacePlanInResult(result *syncResult, plan refreshPlan) {
	result.AffectedApplications = append([]string(nil), plan.AffectedApplications...)
	result.SkippedFiles = append([]string(nil), plan.SkippedFiles...)
	result.ManualApplications = append([]string(nil), plan.ManualApplications...)
}

func appendUniqueStrings(base []string, values ...string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(base)+len(values))
	for _, value := range base {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func withoutString(values []string, drop string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != drop {
			out = append(out, value)
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
