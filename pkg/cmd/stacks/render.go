package stacks

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func renderKindConfig(o *options) (string, func(), error) {
	templatePath := filepath.Join(o.StacksRepo, "deployment", "config", "kind-config.yaml.template")
	raw, err := os.ReadFile(templatePath)
	if err != nil {
		return "", nil, fmt.Errorf("reading kind config template: %w", err)
	}
	rendered := strings.ReplaceAll(string(raw), "${STACKS_DIR}", o.StacksRepo)
	return writeTempYAML("idpbuilder-stacks-kind-*.yaml", rendered)
}

func renderArgoBootstrap(o *options) (string, func(), error) {
	templatePath := filepath.Join(o.StacksRepo, "deployment", "config", "argocd-bootstrap.yaml.template")
	raw, err := os.ReadFile(templatePath)
	if err != nil {
		return "", nil, fmt.Errorf("reading argocd bootstrap template: %w", err)
	}
	kustomizeEnv := strings.TrimPrefix(o.Overlay, "packages/overlays/")
	rendered := string(raw)
	replacements := map[string]string{
		"${KUSTOMIZE_ENV}": kustomizeEnv,
		"${GIT_REPO_URL}":  fmt.Sprintf("http://gitea-http.gitea.svc.cluster.local:3000/%s/%s.git", o.GiteaOwner, o.GiteaRepo),
		"${GIT_BRANCH}":    o.Branch,
	}
	for old, newValue := range replacements {
		rendered = strings.ReplaceAll(rendered, old, newValue)
	}
	return writeTempYAML("idpbuilder-stacks-argocd-bootstrap-*.yaml", rendered)
}

func writeTempYAML(pattern, content string) (string, func(), error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", nil, fmt.Errorf("creating temp file: %w", err)
	}
	path := f.Name()
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return "", nil, fmt.Errorf("writing temp file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", nil, fmt.Errorf("closing temp file: %w", err)
	}
	return path, func() { _ = os.Remove(path) }, nil
}
