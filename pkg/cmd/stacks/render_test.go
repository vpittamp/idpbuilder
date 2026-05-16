package stacks

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	o.Overlay = "packages/overlays/kind-ryzen"
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
	if !strings.Contains(got, "path: packages/overlays/kind-ryzen") {
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
	o.ClusterName = "ryzen-talos-dev"
	args, err := talosDockerCreateArgs(o)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(args, " ")
	for _, want := range []string{
		"cluster create docker",
		"--name ryzen-talos-dev",
		"--kubernetes-version 1.32.0",
		"--image ghcr.io/siderolabs/talos:v1.12.4",
		"--subnet 10.6.0.0/24",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("args missing %q: %s", want, got)
		}
	}
}
