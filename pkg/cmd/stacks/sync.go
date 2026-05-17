package stacks

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

type syncResult struct {
	Commit string
	Files  int
}

type portForward struct {
	cmd  *exec.Cmd
	port int
}

func sync(ctx context.Context, o *options) (syncResult, error) {
	if err := ensureGitWorktree(ctx, o.StacksRepo); err != nil {
		return syncResult{}, err
	}
	pf, err := startGiteaPortForward(ctx)
	if err != nil {
		return syncResult{}, err
	}
	defer pf.stop()

	if err := ensureGiteaRepository(ctx, pf.port, o); err != nil {
		return syncResult{}, err
	}
	result, err := pushSnapshot(ctx, pf.port, o)
	if err != nil {
		return syncResult{}, err
	}
	refreshRootApplication(ctx, o)
	fmt.Printf("Synced %d files from %s to %s/%s:%s at %s\n", result.Files, o.StacksRepo, o.GiteaOwner, o.GiteaRepo, o.Branch, result.Commit)
	return result, nil
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

func pushSnapshot(ctx context.Context, port int, o *options) (syncResult, error) {
	files, err := snapshotFiles(ctx, o.StacksRepo)
	if err != nil {
		return syncResult{}, err
	}
	tmp, err := os.MkdirTemp("", "idpbuilder-stacks-snapshot-*")
	if err != nil {
		return syncResult{}, fmt.Errorf("creating snapshot temp dir: %w", err)
	}
	defer os.RemoveAll(tmp)

	for _, file := range files {
		if err := copySnapshotPath(o.StacksRepo, tmp, file); err != nil {
			return syncResult{}, err
		}
	}
	if o.RewriteBootstrapImagePins {
		if err := rewriteBootstrapImagePins(ctx, o, tmp); err != nil {
			return syncResult{}, err
		}
	}

	if err := run(ctx, tmp, os.Environ(), "git", "init", "-b", o.Branch); err != nil {
		return syncResult{}, err
	}
	if err := run(ctx, tmp, os.Environ(), "git", "config", "user.name", "idpbuilder-stacks"); err != nil {
		return syncResult{}, err
	}
	if err := run(ctx, tmp, os.Environ(), "git", "config", "user.email", "idpbuilder-stacks@cnoe.local"); err != nil {
		return syncResult{}, err
	}
	if err := run(ctx, tmp, os.Environ(), "git", "add", "-A"); err != nil {
		return syncResult{}, err
	}
	message := fmt.Sprintf("sync stacks snapshot %s", time.Now().Format(time.RFC3339))
	if err := run(ctx, tmp, os.Environ(), "git", "commit", "--allow-empty", "-m", message); err != nil {
		return syncResult{}, err
	}
	commit, err := output(ctx, tmp, os.Environ(), "git", "rev-parse", "HEAD")
	if err != nil {
		return syncResult{}, err
	}
	remote := giteaRemoteURL(port, o)
	if err := run(ctx, tmp, os.Environ(), "git", "push", "--force", remote, "HEAD:refs/heads/"+o.Branch); err != nil {
		return syncResult{}, err
	}
	return syncResult{Commit: strings.TrimSpace(commit), Files: len(files)}, nil
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
	return nil
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

func refreshRootApplication(ctx context.Context, o *options) {
	_ = run(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "annotate", "application", "root-application", "-n", "argocd", "argocd.argoproj.io/refresh=normal", "--overwrite")
}
