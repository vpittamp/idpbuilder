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
	if len(result.ChangedFiles) != 1 {
		t.Fatalf("expected one changed file, got %#v", result)
	}
	tree := gitOutputTest(t, remote, "git", "ls-tree", "-r", "--name-only", "refs/heads/main")
	if strings.Contains(tree, "tracked.txt") {
		t.Fatalf("deleted file remained in remote tree:\n%s", tree)
	}
}

func TestAffectedPlanRefreshesOnlyChangedChildManifestApp(t *testing.T) {
	repo := newPlannerRepo(t)
	apps := argoApplicationList{Items: []argoApplication{
		plannerApp("workflow-builder", "packages/components/active-development/manifests/workflow-builder", true),
		plannerApp("kueue-capacity", "packages/components/active-development/manifests/kueue-capacity", true),
	}}

	plan, err := planAffectedApplications(repo, apps, []string{"packages/components/active-development/manifests/workflow-builder/Deployment-workflow-builder.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"workflow-builder"}
	if !reflect.DeepEqual(plan.AffectedApplications, want) {
		t.Fatalf("affected apps mismatch\nwant: %#v\n got: %#v", want, plan.AffectedApplications)
	}
	if len(plan.SkippedFiles) != 0 {
		t.Fatalf("did not expect skipped files, got %v", plan.SkippedFiles)
	}
}

func TestAffectedPlanRefreshesRootBeforeChildForApplicationDefinitionChange(t *testing.T) {
	repo := newPlannerRepo(t)
	apps := argoApplicationList{Items: []argoApplication{
		plannerApp(rootApplicationName, "packages/overlays/ryzen", true),
		plannerApp("workflow-builder", "packages/components/active-development/manifests/workflow-builder", true),
		plannerApp("kueue-capacity", "packages/components/active-development/manifests/kueue-capacity", true),
	}}

	plan, err := planAffectedApplications(repo, apps, []string{"packages/components/active-development/apps/workflow-builder.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"root-application", "workflow-builder"}
	if !reflect.DeepEqual(plan.AffectedApplications, want) {
		t.Fatalf("affected apps mismatch\nwant: %#v\n got: %#v", want, plan.AffectedApplications)
	}
	if !plan.RootFirst {
		t.Fatalf("expected root-first plan")
	}
}

func TestAffectedPlanRefreshesSharedComponentDependentsOnly(t *testing.T) {
	repo := newPlannerRepo(t)
	apps := argoApplicationList{Items: []argoApplication{
		plannerApp("workflow-builder", "packages/components/active-development/manifests/workflow-builder", true),
		plannerApp("kueue-capacity", "packages/components/active-development/manifests/kueue-capacity", true),
	}}

	plan, err := planAffectedApplications(repo, apps, []string{"packages/components/shared/ConfigMap-shared.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"workflow-builder"}
	if !reflect.DeepEqual(plan.AffectedApplications, want) {
		t.Fatalf("affected apps mismatch\nwant: %#v\n got: %#v", want, plan.AffectedApplications)
	}
}

func TestAffectedPlanSkipsNonRenderedFiles(t *testing.T) {
	repo := newPlannerRepo(t)
	apps := argoApplicationList{Items: []argoApplication{
		plannerApp("workflow-builder", "packages/components/active-development/manifests/workflow-builder", true),
	}}

	plan, err := planAffectedApplications(repo, apps, []string{"docs/local-note.md"})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.AffectedApplications) != 0 {
		t.Fatalf("expected no affected apps, got %v", plan.AffectedApplications)
	}
	wantSkipped := []string{"docs/local-note.md"}
	if !reflect.DeepEqual(plan.SkippedFiles, wantSkipped) {
		t.Fatalf("skipped files mismatch\nwant: %#v\n got: %#v", wantSkipped, plan.SkippedFiles)
	}
}

func TestAffectedPlanIndexesRawManifestDirectories(t *testing.T) {
	repo := t.TempDir()
	mustWrite(t, filepath.Join(repo, "packages/base/manifests/raw/ConfigMap-raw.yaml"), "kind: ConfigMap\nmetadata:\n  name: raw\n")
	apps := argoApplicationList{Items: []argoApplication{
		plannerApp("raw", "packages/base/manifests/raw", true),
	}}

	plan, err := planAffectedApplications(repo, apps, []string{"packages/base/manifests/raw/ConfigMap-raw.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"raw"}
	if !reflect.DeepEqual(plan.AffectedApplications, want) {
		t.Fatalf("affected apps mismatch\nwant: %#v\n got: %#v", want, plan.AffectedApplications)
	}
}

func TestAffectedPlanFailsClosedForMissingLocalDependency(t *testing.T) {
	repo := t.TempDir()
	mustWrite(t, filepath.Join(repo, "packages/components/bad/kustomization.yaml"), "resources:\n  - missing.yaml\n")
	apps := argoApplicationList{Items: []argoApplication{
		plannerApp("bad", "packages/components/bad", true),
	}}

	_, err := planAffectedApplications(repo, apps, []string{"packages/components/bad/kustomization.yaml"})
	if err == nil || !strings.Contains(err.Error(), "missing.yaml") {
		t.Fatalf("expected missing dependency error, got %v", err)
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

func TestApplicationUsesRepoMatchesInternalAndExternalStackURLs(t *testing.T) {
	suffix := "/giteaadmin/stacks.git"
	app := argoApplication{}
	app.Metadata.Name = "workflow-builder"
	app.Spec.Source.RepoURL = "http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git"
	if !applicationUsesRepo(app, suffix) {
		t.Fatalf("expected internal Gitea repo URL to match")
	}

	app.Spec.Source.RepoURL = "https://gitea.cnoe.localtest.me/giteaadmin/stacks.git/"
	if !applicationUsesRepo(app, suffix) {
		t.Fatalf("expected external Gitea repo URL to match")
	}

	app.Spec.Source.RepoURL = "https://github.com/PittampalliOrg/stacks.git"
	if applicationUsesRepo(app, suffix) {
		t.Fatalf("did not expect unrelated owner URL to match")
	}
}

func TestApplicationUsesRepoMatchesMultiSourceApps(t *testing.T) {
	suffix := "/giteaadmin/stacks.git"
	app := argoApplication{}
	app.Metadata.Name = "multi-source"
	app.Spec.Source.RepoURL = "https://example.invalid/other/repo.git"
	app.Spec.Sources = []argoApplicationSource{
		{RepoURL: "oci://ghcr.io/pittampalliorg/chart"},
		{RepoURL: "http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git"},
	}
	if !applicationUsesRepo(app, suffix) {
		t.Fatalf("expected multi-source app to match stacks repo")
	}
}

func TestMatchingStackApplicationNamesSortsAndDeduplicates(t *testing.T) {
	suffix := "/giteaadmin/stacks.git"
	first := argoApplication{}
	first.Metadata.Name = "workflow-builder"
	first.Spec.Source.RepoURL = "http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git"
	duplicate := argoApplication{}
	duplicate.Metadata.Name = "workflow-builder"
	duplicate.Spec.Sources = []argoApplicationSource{{RepoURL: "https://gitea.cnoe.localtest.me/giteaadmin/stacks.git"}}
	second := argoApplication{}
	second.Metadata.Name = "root-application"
	second.Spec.Source.RepoURL = "giteaadmin/stacks.git"
	other := argoApplication{}
	other.Metadata.Name = "other"
	other.Spec.Source.RepoURL = "https://github.com/PittampalliOrg/stacks.git"

	names := matchingStackApplicationNames(argoApplicationList{Items: []argoApplication{first, duplicate, second, other}}, suffix)
	want := []string{"root-application", "workflow-builder"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("expected %v, got %v", want, names)
	}
}

func TestMatchingStackApplicationNamesReturnsEmptyWhenNoAppsMatch(t *testing.T) {
	app := argoApplication{}
	app.Metadata.Name = "workflow-builder"
	app.Spec.Source.RepoURL = "https://github.com/PittampalliOrg/stacks.git"

	names := matchingStackApplicationNames(argoApplicationList{Items: []argoApplication{app}}, "/giteaadmin/stacks.git")
	if len(names) != 0 {
		t.Fatalf("expected no matching applications, got %v", names)
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

func newPlannerRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	mustWrite(t, filepath.Join(repo, "packages/overlays/ryzen/kustomization.yaml"), `resources:
  - ../../components/active-development
`)
	mustWrite(t, filepath.Join(repo, "packages/components/active-development/kustomization.yaml"), `resources:
  - apps/workflow-builder.yaml
  - apps/kueue-capacity.yaml
`)
	mustWrite(t, filepath.Join(repo, "packages/components/active-development/apps/workflow-builder.yaml"), `apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: workflow-builder
spec:
  source:
    repoURL: http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git
    path: packages/components/active-development/manifests/workflow-builder
`)
	mustWrite(t, filepath.Join(repo, "packages/components/active-development/apps/kueue-capacity.yaml"), `apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: kueue-capacity
spec:
  source:
    repoURL: http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git
    path: packages/components/active-development/manifests/kueue-capacity
`)
	mustWrite(t, filepath.Join(repo, "packages/components/active-development/manifests/workflow-builder/kustomization.yaml"), `resources:
  - ../../../shared
  - Deployment-workflow-builder.yaml
`)
	mustWrite(t, filepath.Join(repo, "packages/components/active-development/manifests/workflow-builder/Deployment-workflow-builder.yaml"), "kind: Deployment\nmetadata:\n  name: workflow-builder\n")
	mustWrite(t, filepath.Join(repo, "packages/components/active-development/manifests/kueue-capacity/kustomization.yaml"), `resources:
  - ConfigMap-kueue-capacity.yaml
`)
	mustWrite(t, filepath.Join(repo, "packages/components/active-development/manifests/kueue-capacity/ConfigMap-kueue-capacity.yaml"), "kind: ConfigMap\nmetadata:\n  name: kueue-capacity\n")
	mustWrite(t, filepath.Join(repo, "packages/components/shared/kustomization.yaml"), `resources:
  - ConfigMap-shared.yaml
`)
	mustWrite(t, filepath.Join(repo, "packages/components/shared/ConfigMap-shared.yaml"), "kind: ConfigMap\nmetadata:\n  name: shared\n")
	mustWrite(t, filepath.Join(repo, "docs/local-note.md"), "note\n")
	return repo
}

func plannerApp(name, path string, automated bool) argoApplication {
	app := argoApplication{}
	app.Metadata.Name = name
	app.Spec.Source.RepoURL = "http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git"
	app.Spec.Source.Path = path
	if automated {
		app.Spec.SyncPolicy.Automated = map[string]any{}
	}
	app.Status.Sync.Status = "Synced"
	return app
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
