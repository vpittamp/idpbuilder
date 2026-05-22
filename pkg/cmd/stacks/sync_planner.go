package stacks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/yaml"
	sigsyaml "sigs.k8s.io/yaml"
)

const (
	refreshModeAffected = "affected"
	refreshModeAll      = "all"
	refreshModeNone     = "none"

	rootApplicationName = "root-application"
)

type refreshPlan struct {
	AffectedApplications []string
	SkippedFiles         []string
	ManualApplications   []string
	UnsyncedApplications []string
	RootFirst            bool
}

type appDependencyIndex struct {
	apps map[string]applicationDependency
}

type applicationDependency struct {
	name       string
	automated  bool
	status     string
	exactFiles map[string]bool
	childApps  map[string]bool
}

type kustomizationFile struct {
	Resources             []string              `yaml:"resources"`
	Components            []string              `yaml:"components"`
	Generators            []string              `yaml:"generators"`
	Transformers          []string              `yaml:"transformers"`
	Configurations        []string              `yaml:"configurations"`
	CRDs                  []string              `yaml:"crds"`
	PatchesStrategicMerge []string              `yaml:"patchesStrategicMerge"`
	PatchesJson6902       []kustomizePathObject `yaml:"patchesJson6902"`
	Patches               []kustomizePatch      `yaml:"patches"`
	Replacements          []kustomizePathObject `yaml:"replacements"`
	ConfigMapGenerator    []kustomizeGenerator  `yaml:"configMapGenerator"`
	SecretGenerator       []kustomizeGenerator  `yaml:"secretGenerator"`
	HelmCharts            []kustomizeHelmChart  `yaml:"helmCharts"`
	HelmChartInflationGen []kustomizeHelmChart  `yaml:"helmChartInflationGenerator"`
	OpenAPI               map[string]any        `yaml:"openapi"`
}

type kustomizePathObject struct {
	Path string `yaml:"path"`
}

type kustomizePatch struct {
	Path  string `yaml:"path"`
	Patch string `yaml:"patch"`
}

type kustomizeGenerator struct {
	Files []string `yaml:"files"`
	Envs  []string `yaml:"envs"`
}

type kustomizeHelmChart struct {
	ValuesFile       string   `yaml:"valuesFile"`
	AdditionalValues []string `yaml:"additionalValuesFiles"`
}

func validRefreshMode(mode string) bool {
	switch mode {
	case refreshModeAffected, refreshModeAll, refreshModeNone:
		return true
	default:
		return false
	}
}

func printRefreshPlan(ctx context.Context, o *options) error {
	apps, err := listStackApplications(ctx, o)
	if err != nil {
		return err
	}
	changed, err := workingTreeChangedFiles(ctx, o.StacksRepo)
	if err != nil {
		return err
	}
	plan, err := planAffectedApplications(o.StacksRepo, apps, changed)
	if err != nil {
		return err
	}
	printPlanSummary("Refresh plan", "", changed, plan)
	return nil
}

func planAffectedApplications(repo string, apps argoApplicationList, changed []string) (refreshPlan, error) {
	index, err := buildApplicationDependencyIndex(repo, apps)
	if err != nil {
		return refreshPlan{}, err
	}
	affected := map[string]bool{}
	skipped := make([]string, 0)
	for _, file := range normalizeChangedFiles(changed) {
		matched := false
		if ryzenOverlayPathAffectsRoot(file) {
			if _, ok := index.apps[rootApplicationName]; ok {
				affected[rootApplicationName] = true
				matched = true
			}
		}
		changedChildName, hasChangedChildName := parseApplicationName(filepath.Join(repo, file))
		if hasChangedChildName {
			if _, ok := index.apps[changedChildName]; ok {
				affected[changedChildName] = true
				matched = true
			}
		}
		for name, dep := range index.apps {
			if dep.exactFiles[file] {
				affected[name] = true
				matched = true
			}
			if hasChangedChildName {
				if dep.childApps[changedChildName] {
					affected[name] = true
					matched = true
				}
			}
		}
		if !matched {
			skipped = append(skipped, file)
		}
	}

	names := sortedKeys(affected)
	plan := refreshPlan{
		AffectedApplications: names,
		SkippedFiles:         skipped,
		RootFirst:            affected[rootApplicationName],
	}
	for _, name := range names {
		dep := index.apps[name]
		if !dep.automated {
			plan.ManualApplications = append(plan.ManualApplications, name)
		}
		if dep.status != "" && dep.status != "Synced" {
			plan.UnsyncedApplications = append(plan.UnsyncedApplications, name)
		}
	}
	return plan, nil
}

func buildApplicationDependencyIndex(repo string, apps argoApplicationList) (appDependencyIndex, error) {
	index := appDependencyIndex{apps: map[string]applicationDependency{}}
	for _, app := range apps.Items {
		if app.Metadata.Name == "" {
			continue
		}
		dep := applicationDependency{
			name:       app.Metadata.Name,
			automated:  app.Spec.SyncPolicy.Automated != nil,
			status:     app.Status.Sync.Status,
			exactFiles: map[string]bool{},
			childApps:  map[string]bool{},
		}
		for _, source := range app.localSources() {
			if strings.TrimSpace(source.Path) == "" {
				continue
			}
			if err := addKustomizeDependencies(repo, source.Path, dep.exactFiles, dep.childApps, map[string]bool{}); err != nil {
				return appDependencyIndex{}, fmt.Errorf("indexing ArgoCD application %s source path %q: %w", app.Metadata.Name, source.Path, err)
			}
		}
		index.apps[app.Metadata.Name] = dep
	}
	return index, nil
}

func addKustomizeDependencies(repo, rel string, files map[string]bool, childApps map[string]bool, seen map[string]bool) error {
	clean, err := cleanRepoRel(rel)
	if err != nil {
		return err
	}
	full := filepath.Join(repo, clean)
	st, err := os.Stat(full)
	if err != nil {
		return fmt.Errorf("stat %s: %w", clean, err)
	}
	if !st.IsDir() {
		files[clean] = true
		if name, ok := parseApplicationName(full); ok {
			childApps[name] = true
		}
		return nil
	}
	kustomization, err := findKustomizationFile(full)
	if err != nil {
		return addManifestDirectoryDependencies(repo, clean, files, childApps)
	}
	relKustomization, err := filepath.Rel(repo, kustomization)
	if err != nil {
		return err
	}
	relKustomization = filepath.ToSlash(relKustomization)
	if seen[relKustomization] {
		return nil
	}
	seen[relKustomization] = true
	files[relKustomization] = true

	data, err := os.ReadFile(kustomization)
	if err != nil {
		return fmt.Errorf("reading %s: %w", relKustomization, err)
	}
	var k kustomizationFile
	if err := sigsyaml.Unmarshal(data, &k); err != nil {
		return fmt.Errorf("parsing %s: %w", relKustomization, err)
	}
	base := filepath.Dir(relKustomization)
	refs := k.localPathRefs()
	for _, ref := range refs {
		if shouldSkipKustomizeRef(ref) {
			continue
		}
		next, err := joinRepoRel(base, ref)
		if err != nil {
			return fmt.Errorf("%s references unsupported path %q: %w", relKustomization, ref, err)
		}
		if err := addKustomizeDependencies(repo, next, files, childApps, seen); err != nil {
			return fmt.Errorf("%s reference %q: %w", relKustomization, ref, err)
		}
	}
	return nil
}

func addManifestDirectoryDependencies(repo, rel string, files map[string]bool, childApps map[string]bool) error {
	full := filepath.Join(repo, rel)
	return filepath.WalkDir(full, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".yaml" && ext != ".yml" && ext != ".json" {
			return nil
		}
		file, err := filepath.Rel(repo, path)
		if err != nil {
			return err
		}
		file = filepath.ToSlash(file)
		files[file] = true
		if name, ok := parseApplicationName(path); ok {
			childApps[name] = true
		}
		return nil
	})
}

func (k kustomizationFile) localPathRefs() []string {
	refs := make([]string, 0)
	refs = append(refs, k.Resources...)
	refs = append(refs, k.Components...)
	refs = append(refs, k.Generators...)
	refs = append(refs, k.Transformers...)
	refs = append(refs, k.Configurations...)
	refs = append(refs, k.CRDs...)
	refs = append(refs, k.PatchesStrategicMerge...)
	for _, p := range k.PatchesJson6902 {
		refs = append(refs, p.Path)
	}
	for _, p := range k.Patches {
		refs = append(refs, p.Path)
	}
	for _, r := range k.Replacements {
		refs = append(refs, r.Path)
	}
	for _, g := range append(k.ConfigMapGenerator, k.SecretGenerator...) {
		for _, file := range g.Files {
			refs = append(refs, generatorFilePath(file))
		}
		refs = append(refs, g.Envs...)
	}
	for _, h := range append(k.HelmCharts, k.HelmChartInflationGen...) {
		refs = append(refs, h.ValuesFile)
		refs = append(refs, h.AdditionalValues...)
	}
	if path, ok := k.OpenAPI["path"].(string); ok {
		refs = append(refs, path)
	}
	return refs
}

func generatorFilePath(value string) string {
	if before, after, found := strings.Cut(value, "="); found && before != "" && after != "" {
		return after
	}
	return value
}

func shouldSkipKustomizeRef(ref string) bool {
	ref = strings.TrimSpace(ref)
	return ref == "" ||
		strings.Contains(ref, "://") ||
		strings.Contains(ref, "::") ||
		strings.HasPrefix(ref, "github.com/") ||
		strings.HasPrefix(ref, "git@")
}

func findKustomizationFile(dir string) (string, error) {
	for _, name := range []string{"kustomization.yaml", "kustomization.yml", "Kustomization"} {
		path := filepath.Join(dir, name)
		if st, err := os.Stat(path); err == nil && !st.IsDir() {
			return path, nil
		}
	}
	return "", fmt.Errorf("directory %s has no kustomization file", dir)
}

func parseApplicationName(path string) (string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	for {
		var doc struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		err := decoder.Decode(&doc)
		if err == io.EOF {
			return "", false
		}
		if err != nil {
			return "", false
		}
		if doc.Kind == "Application" && doc.Metadata.Name != "" {
			return doc.Metadata.Name, true
		}
	}
}

func cleanRepoRel(rel string) (string, error) {
	clean := filepath.Clean(filepath.FromSlash(strings.TrimSpace(rel)))
	if clean == "." || clean == "" || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes repository")
	}
	return filepath.ToSlash(clean), nil
}

func joinRepoRel(base, ref string) (string, error) {
	joined := filepath.Join(filepath.FromSlash(base), filepath.FromSlash(ref))
	return cleanRepoRel(joined)
}

func normalizeChangedFiles(files []string) []string {
	seen := map[string]bool{}
	for _, file := range files {
		clean, err := cleanRepoRel(file)
		if err == nil {
			seen[clean] = true
		}
	}
	return sortedKeys(seen)
}

func workingTreeChangedFiles(ctx context.Context, repo string) ([]string, error) {
	out, err := output(ctx, repo, os.Environ(), "git", "status", "--porcelain=v1", "-z", "--untracked-files=all")
	if err != nil {
		return nil, err
	}
	parts := strings.Split(out, "\x00")
	seen := map[string]bool{}
	for i := 0; i < len(parts); i++ {
		entry := parts[i]
		if entry == "" || len(entry) < 4 {
			continue
		}
		status := entry[:2]
		path := strings.TrimSpace(entry[3:])
		if strings.HasPrefix(status, "R") || strings.HasPrefix(status, "C") {
			if i+1 < len(parts) {
				i++
			}
		}
		if path != "" {
			if clean, err := cleanRepoRel(path); err == nil {
				seen[clean] = true
			}
		}
	}
	return sortedKeys(seen), nil
}

func ryzenOverlayPathAffectsRoot(file string) bool {
	return file == "packages/overlays/ryzen" || strings.HasPrefix(file, "packages/overlays/ryzen/")
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func printPlanSummary(title, commit string, changed []string, plan refreshPlan) {
	if commit != "" {
		fmt.Printf("%s for %s\n", title, commit)
	} else {
		fmt.Println(title)
	}
	fmt.Printf("Changed files: %d\n", len(changed))
	if len(plan.AffectedApplications) == 0 {
		fmt.Println("Affected applications: none")
	} else {
		fmt.Printf("Affected applications: %s\n", strings.Join(plan.AffectedApplications, ", "))
	}
	if len(plan.SkippedFiles) > 0 {
		fmt.Printf("Skipped non-rendered files: %s\n", strings.Join(plan.SkippedFiles, ", "))
	}
	if len(plan.ManualApplications) > 0 {
		fmt.Printf("Manual applications requiring operator sync: %s\n", strings.Join(plan.ManualApplications, ", "))
	}
	if len(plan.UnsyncedApplications) > 0 {
		fmt.Printf("Applications already not Synced before refresh: %s\n", strings.Join(plan.UnsyncedApplications, ", "))
	}
}

func waitForApplicationsObserved(ctx context.Context, o *options, names []string, commit string) ([]string, error) {
	if len(names) == 0 || commit == "" {
		return nil, nil
	}
	deadline := time.Now().Add(o.SyncWaitTimeout)
	var last map[string]argoApplication
	for {
		apps, err := getApplicationsByName(ctx, o, names)
		if err != nil {
			return nil, err
		}
		last = apps
		pending := make([]string, 0)
		for _, name := range names {
			app, ok := apps[name]
			if !ok {
				pending = append(pending, name+" (missing)")
				continue
			}
			if app.Status.OperationState.Phase == "Running" {
				pending = append(pending, name+" (operation running)")
				continue
			}
			if !applicationObservedRevision(app, commit) {
				pending = append(pending, name+" (revision "+app.observedRevisionSummary()+")")
			}
		}
		if len(pending) == 0 {
			return unsyncedApplications(last, names), nil
		}
		if err := supersededByObservedApplicationRevision(ctx, o, names, commit, apps); err != nil {
			return unsyncedApplications(last, names), err
		}
		if time.Now().After(deadline) {
			if err := supersededByBranchRevision(ctx, o, commit); err != nil {
				return unsyncedApplications(last, names), err
			}
			return unsyncedApplications(last, names), fmt.Errorf("timed out after %s waiting for applications to observe %s: %s", o.SyncWaitTimeout, shortRevision(commit), strings.Join(pending, ", "))
		}
		select {
		case <-ctx.Done():
			return unsyncedApplications(last, names), context.Cause(ctx)
		case <-time.After(syncPollInterval(o)):
		}
	}
}

type syncSupersededError struct {
	Target       string
	SupersededBy string
	Detail       string
}

func (e syncSupersededError) Error() string {
	return fmt.Sprintf("sync to %s superseded by %s: %s", shortRevision(e.Target), shortRevision(e.SupersededBy), e.Detail)
}

func supersededByObservedApplicationRevision(ctx context.Context, o *options, names []string, commit string, apps map[string]argoApplication) error {
	for _, name := range names {
		app, ok := apps[name]
		if !ok {
			continue
		}
		for _, revision := range observedApplicationRevisions(app) {
			superseded, resolved := revisionSupersedesTarget(ctx, o, commit, revision)
			if superseded {
				return syncSupersededError{
					Target:       commit,
					SupersededBy: resolved,
					Detail:       fmt.Sprintf("application %s observed newer revision %s", name, shortRevision(resolved)),
				}
			}
		}
	}
	return nil
}

func supersededByBranchRevision(ctx context.Context, o *options, commit string) error {
	revision, ok := currentBranchRevision(ctx, o)
	if !ok {
		return nil
	}
	superseded, resolved := revisionSupersedesTarget(ctx, o, commit, revision)
	if !superseded {
		return nil
	}
	return syncSupersededError{
		Target:       commit,
		SupersededBy: resolved,
		Detail:       fmt.Sprintf("Gitea branch %s advanced past the pushed commit", o.Branch),
	}
}

func observedApplicationRevisions(app argoApplication) []string {
	values := make([]string, 0, 1+len(app.Status.Sync.Revisions))
	if app.Status.Sync.Revision != "" {
		values = append(values, app.Status.Sync.Revision)
	}
	values = append(values, app.Status.Sync.Revisions...)
	return values
}

func currentBranchRevision(ctx context.Context, o *options) (string, bool) {
	cacheDir, err := syncCachePath(o)
	if err != nil {
		return "", false
	}
	_ = runQuiet(ctx, cacheDir, "git", "fetch", "--quiet", "origin", o.Branch)
	for _, ref := range []string{"refs/remotes/origin/" + o.Branch, "refs/heads/" + o.Branch} {
		revision, err := gitCommitRevision(ctx, cacheDir, ref)
		if err == nil && revision != "" {
			return revision, true
		}
	}
	return "", false
}

func revisionSupersedesTarget(ctx context.Context, o *options, target, candidate string) (bool, string) {
	if revisionMatches(candidate, target) {
		return false, ""
	}
	cacheDir, err := syncCachePath(o)
	if err != nil {
		return false, ""
	}
	targetRevision, err := gitCommitRevision(ctx, cacheDir, target)
	if err != nil {
		return false, ""
	}
	candidateRevision, err := gitCommitRevision(ctx, cacheDir, candidate)
	if err != nil || candidateRevision == targetRevision {
		return false, ""
	}
	if gitCommitIsAncestor(ctx, cacheDir, targetRevision, candidateRevision) {
		return true, candidateRevision
	}
	return false, ""
}

func gitCommitRevision(ctx context.Context, dir, revision string) (string, error) {
	out, err := output(ctx, dir, os.Environ(), "git", "rev-parse", "--verify", "--quiet", revision+"^{commit}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func gitCommitIsAncestor(ctx context.Context, dir, ancestor, descendant string) bool {
	cmd := exec.CommandContext(ctx, "git", "merge-base", "--is-ancestor", ancestor, descendant)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	return cmd.Run() == nil
}

func runQuiet(ctx context.Context, dir string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	return cmd.Run()
}

func syncPollInterval(o *options) time.Duration {
	if o == nil || o.SyncPollInterval <= 0 {
		return defaultSyncPollInterval
	}
	return o.SyncPollInterval
}

func getApplicationsByName(ctx context.Context, o *options, names []string) (map[string]argoApplication, error) {
	result := map[string]argoApplication{}
	want := map[string]bool{}
	for _, name := range names {
		want[name] = true
	}
	raw, err := output(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "get", "application", "-n", "argocd", "-o", "json")
	if err != nil {
		return nil, err
	}
	var apps argoApplicationList
	if err := json.Unmarshal([]byte(raw), &apps); err != nil {
		return nil, fmt.Errorf("parsing ArgoCD applications: %w", err)
	}
	for _, app := range apps.Items {
		if want[app.Metadata.Name] {
			result[app.Metadata.Name] = app
		}
	}
	return result, nil
}

func applicationObservedRevision(app argoApplication, commit string) bool {
	if revisionMatches(app.Status.Sync.Revision, commit) {
		return true
	}
	for _, revision := range app.Status.Sync.Revisions {
		if revisionMatches(revision, commit) {
			return true
		}
	}
	return false
}

func revisionMatches(got, want string) bool {
	got = strings.TrimSpace(got)
	want = strings.TrimSpace(want)
	return got != "" && want != "" && (got == want || strings.HasPrefix(got, want) || strings.HasPrefix(want, got))
}

func shortRevision(revision string) string {
	if len(revision) > 12 {
		return revision[:12]
	}
	return revision
}

func (app argoApplication) observedRevisionSummary() string {
	values := make([]string, 0, 1+len(app.Status.Sync.Revisions))
	if app.Status.Sync.Revision != "" {
		values = append(values, shortRevision(app.Status.Sync.Revision))
	}
	for _, revision := range app.Status.Sync.Revisions {
		if revision != "" {
			values = append(values, shortRevision(revision))
		}
	}
	if len(values) == 0 {
		return "none"
	}
	return strings.Join(values, ",")
}

func unsyncedApplications(apps map[string]argoApplication, names []string) []string {
	unsynced := make([]string, 0)
	for _, name := range names {
		app, ok := apps[name]
		if ok && app.Status.Sync.Status != "" && app.Status.Sync.Status != "Synced" {
			unsynced = append(unsynced, name)
		}
	}
	return unsynced
}
