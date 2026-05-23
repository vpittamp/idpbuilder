package stacks

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestRenderArgoBootstrap(t *testing.T) {
	repo := t.TempDir()
	configDir := filepath.Join(repo, "deployment", "config")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatal(err)
	}
	template := "repo: ${GIT_REPO_URL}\nbranch: ${GIT_BRANCH}\npath: packages/overlays/${KUSTOMIZE_ENV}\n"
	if err := os.WriteFile(filepath.Join(configDir, "argocd-bootstrap.yaml.template"), []byte(template), 0o644); err != nil {
		t.Fatal(err)
	}
	o := defaultOptions()
	o.StacksRepo = repo
	o.Overlay = "packages/overlays/ryzen"
	path, cleanup, err := renderArgoBootstrap(o)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	for _, disallowed := range []string{"${GIT_REPO_URL}", "${GIT_BRANCH}", "${KUSTOMIZE_ENV}"} {
		if strings.Contains(got, disallowed) {
			t.Fatalf("rendered bootstrap still contains %s: %s", disallowed, got)
		}
	}
	if !strings.Contains(got, "http://gitea-http.gitea.svc.cluster.local:3000/giteaadmin/stacks.git") {
		t.Fatalf("rendered bootstrap missing internal repo url: %s", got)
	}
	if !strings.Contains(got, "path: packages/overlays/ryzen") {
		t.Fatalf("rendered bootstrap missing overlay: %s", got)
	}
}

func TestTalosDockerCreateArgsFromClaim(t *testing.T) {
	repo := t.TempDir()
	claimDir := filepath.Join(repo, "packages", "components", "crossplane-hetzner-talos", "manifests", "crossplane-hcloud-compositions")
	if err := os.MkdirAll(claimDir, 0o755); err != nil {
		t.Fatal(err)
	}
	claim := `apiVersion: platform.pittampalli.io/v1alpha1
kind: TalosSpokeClusterClaim
spec:
  parameters:
    talos:
      version: "1.12.4"
      kubernetesVersion: "1.32.0"
`
	if err := os.WriteFile(filepath.Join(claimDir, "TalosSpokeClusterClaim-dev.yaml"), []byte(claim), 0o644); err != nil {
		t.Fatal(err)
	}
	o := defaultOptions()
	o.StacksRepo = repo
	o.ClusterName = "ryzen-dev"
	args, err := talosDockerCreateArgs(o)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(args, " ")
	for _, want := range []string{
		"cluster create docker",
		"--name ryzen-dev",
		"--kubernetes-version 1.32.0",
		"--image ghcr.io/siderolabs/talos:v1.12.4",
		"--subnet 10.6.0.0/24",
		"--exposed-ports 9443:443/tcp",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("args missing %q: %s", want, got)
		}
	}
}

func TestTalosDockerBootstrapPatch(t *testing.T) {
	o := defaultOptions()
	o.StacksRepo = t.TempDir()
	o.ClusterName = "ryzen"
	o.TalosSubnet = "10.6.0.0/24"
	keyDir := filepath.Join(o.StacksRepo, "ref-implementation", "azure-workload-identity", "keys")
	if err := os.MkdirAll(keyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "sa.key"), []byte("test-signing-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	path, cleanup, err := renderTalosDockerBootstrapPatch(o)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	for _, want := range []string{
		"kind: RegistryMirrorConfig",
		"name: gitea.cnoe.localtest.me:9443",
		"name: gitea.cnoe.localtest.me:8443",
		"- url: https://gitea.cnoe.localtest.me",
		"name: docker.io",
		"- url: https://mirror.gcr.io",
		"service-account-issuer: \"https://oidcissuer65846b7df97b.z13.web.core.windows.net/\"",
		"serviceAccount:",
		"key: \"dGVzdC1zaWduaW5nLWtleQ==\"",
		"kind: RegistryTLSConfig",
		"kind: RegistryAuthConfig",
		"kind: StaticHostConfig",
		"name: 10.6.0.2",
		"stacks.io/swebench-pool: dev-benchmark",
		"nameservers:",
		"- 10.6.0.1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("patch missing %q: %s", want, got)
		}
	}
}

func TestTalosDockerProviderDefaults(t *testing.T) {
	repo := t.TempDir()
	o := defaultOptions()
	o.Provider = providerTalosDocker
	o.StacksRepo = repo
	o.applyProviderDefaults(&cobra.Command{})
	if o.ClusterName != defaultClusterName {
		t.Fatalf("cluster default = %q, want %q", o.ClusterName, defaultClusterName)
	}
	if o.Overlay != defaultOverlay {
		t.Fatalf("overlay default = %q, want %q", o.Overlay, defaultOverlay)
	}
	wantProfile := filepath.Join(repo, defaultReadinessProfile)
	if o.ReadinessProfile != wantProfile {
		t.Fatalf("readiness profile = %q, want %q", o.ReadinessProfile, wantProfile)
	}
	if o.WaitTarget != waitTargetInnerLoop {
		t.Fatalf("wait target = %q, want %q", o.WaitTarget, waitTargetInnerLoop)
	}
}

func TestTalosDockerSkopeoProviderDefaultsUseSerialImageSeeding(t *testing.T) {
	repo := t.TempDir()
	o := defaultOptions()
	o.Provider = providerTalosDocker
	o.StacksRepo = repo
	o.SeedImagePushEngine = seedImagePushEngineSkopeo
	o.applyProviderDefaults(&cobra.Command{})
	if o.SeedImageJobs != 1 {
		t.Fatalf("seed image jobs = %d, want 1", o.SeedImageJobs)
	}
}

func TestKindProviderDefaultsRemainLegacyKind(t *testing.T) {
	repo := t.TempDir()
	o := defaultOptions()
	o.Provider = providerKind
	o.StacksRepo = repo
	o.applyProviderDefaults(&cobra.Command{})
	if o.ClusterName != defaultClusterName {
		t.Fatalf("cluster default = %q, want %q", o.ClusterName, defaultClusterName)
	}
	if o.Overlay != defaultLegacyKindOverlay {
		t.Fatalf("overlay default = %q, want %q", o.Overlay, defaultLegacyKindOverlay)
	}
	wantProfile := filepath.Join(repo, defaultLegacyKindReadinessProfile)
	if o.ReadinessProfile != wantProfile {
		t.Fatalf("readiness profile = %q, want %q", o.ReadinessProfile, wantProfile)
	}
	if o.WaitTarget != waitTargetAll {
		t.Fatalf("wait target = %q, want %q", o.WaitTarget, waitTargetAll)
	}
	if o.SeedImageJobs != 4 {
		t.Fatalf("seed image jobs = %d, want 4", o.SeedImageJobs)
	}
}

func TestWaitCohortsThrough(t *testing.T) {
	got := waitCohortsThrough(waitTargetInnerLoop)
	want := []string{waitTargetBootstrap, waitTargetGitOpsCore, waitTargetInnerLoop}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("cohorts = %v, want %v", got, want)
	}
	if !validWaitTarget(waitTargetAll) {
		t.Fatalf("expected %q to be a valid wait target", waitTargetAll)
	}
	if validWaitTarget("access") {
		t.Fatalf("access should not be a blocking wait target")
	}
}

func TestTalosClusterExistsRequiresNodeRows(t *testing.T) {
	showOutput := `PROVISIONER           docker
NAME                  ryzen
NETWORK NAME

NODES:

NAME   TYPE   IP   CPU   RAM   DISK
`
	if talosClusterShowHasNodes(showOutput) {
		t.Fatalf("empty cluster show output was treated as an existing cluster")
	}
	showOutput = `PROVISIONER           docker
NAME                  ryzen

NODES:

NAME                         TYPE           IP          CPU   RAM    DISK
ryzen-controlplane-1   controlplane   10.6.0.2    2     2GiB
`
	if !talosClusterShowHasNodes(showOutput) {
		t.Fatalf("cluster show output with node rows was not treated as existing")
	}
}

func TestTalosDockerHostRegistryUsesExposedHTTPSPort(t *testing.T) {
	o := defaultOptions()
	o.GiteaOwner = "giteaadmin"
	if got, want := talosDockerHostRegistry(o), "gitea.cnoe.localtest.me:9443/giteaadmin"; got != want {
		t.Fatalf("host registry = %q, want %q", got, want)
	}
	o.TalosExposedPorts = []string{"10443:443/tcp"}
	if got, want := talosDockerHostRegistry(o), "gitea.cnoe.localtest.me:10443/giteaadmin"; got != want {
		t.Fatalf("host registry = %q, want %q", got, want)
	}
}

func TestBootstrapImageRewriteRegistryUsesTalosHostPort(t *testing.T) {
	o := defaultOptions()
	o.Provider = providerTalosDocker
	o.GiteaOwner = "giteaadmin"
	if got, want := bootstrapImageRewriteRegistry(o), "gitea.cnoe.localtest.me:9443/giteaadmin"; got != want {
		t.Fatalf("rewrite registry = %q, want %q", got, want)
	}
}

func TestTalosDockerCreateArgsIncludeResourceLimits(t *testing.T) {
	o := defaultOptions()
	o.Provider = providerTalosDocker
	o.ClusterName = "ryzen"
	o.KubeVersion = "v1.32.0"
	o.TalosImage = "ghcr.io/siderolabs/talos:v1.12.4"
	args, err := talosDockerCreateArgs(o)
	if err != nil {
		t.Fatalf("talosDockerCreateArgs returned error: %v", err)
	}
	want := map[string]string{
		"--memory-controlplanes": "6GiB",
		"--memory-workers":       "10GiB",
		"--cpus-controlplanes":   "4.0",
		"--cpus-workers":         "5.0",
	}
	for flag, value := range want {
		if !argsContainPair(args, flag, value) {
			t.Fatalf("talosDockerCreateArgs missing %s %s in %v", flag, value, args)
		}
	}
}

func TestTalosDockerCreateArgsIncludeWorkerCapacityPatch(t *testing.T) {
	o := defaultOptions()
	o.Provider = providerTalosDocker
	o.ClusterName = "ryzen"
	o.KubeVersion = "v1.32.0"
	o.TalosImage = "ghcr.io/siderolabs/talos:v1.12.4"
	o.TalosWorkerConfigPatches = []string{"@worker-capacity.yaml"}
	args, err := talosDockerCreateArgs(o)
	if err != nil {
		t.Fatalf("talosDockerCreateArgs returned error: %v", err)
	}
	if !argsContainPair(args, "--config-patch-workers", "@worker-capacity.yaml") {
		t.Fatalf("talosDockerCreateArgs missing worker config patch in %v", args)
	}
}

func TestTalosDockerWorkerCapacityPatch(t *testing.T) {
	o := defaultOptions()
	path, cleanup, err := renderTalosDockerWorkerCapacityPatch(o)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	for _, want := range []string{
		"feature-gates: MemoryQoS=true",
		"kubeReserved:",
		"cpu: 6450m",
		"memory: 21Gi",
		"systemReserved:",
		"memory: 1Gi",
		"memory.available: 512Mi",
		"memoryThrottlingFactor: 0.85",
		"memoryReservationPolicy: TieredReservation",
		"5000m CPU / 8Gi memory inside a 10GiB node",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("worker capacity patch missing %q: %s", want, got)
		}
	}
}

func TestTalosDockerKubeconfigNameMatches(t *testing.T) {
	for _, name := range []string{"admin@ryzen", "admin@ryzen-1", "admin@ryzen-12"} {
		if !talosDockerKubeconfigNameMatches(name, "admin@ryzen") {
			t.Fatalf("expected %q to match admin@ryzen", name)
		}
	}
	for _, name := range []string{"admin@ryzen-talos", "admin@ryzen-prod", "ryzen"} {
		if talosDockerKubeconfigNameMatches(name, "admin@ryzen") {
			t.Fatalf("expected %q not to match admin@ryzen", name)
		}
	}
}

func argsContainPair(args []string, flag, value string) bool {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}
