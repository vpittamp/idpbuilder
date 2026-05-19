package stacks

import (
	"context"
	"fmt"
	"os"
	"strings"
)

func status(ctx context.Context, o *options) error {
	fmt.Printf("Profile: %s\n", o.Profile)
	fmt.Printf("Provider: %s\n", o.Provider)
	fmt.Printf("Cluster: %s\n", o.ClusterName)
	fmt.Printf("Stacks repo: %s\n", o.StacksRepo)
	fmt.Printf("Overlay: %s\n", o.Overlay)
	fmt.Printf("Gitea ref: %s/%s:%s\n", o.GiteaOwner, o.GiteaRepo, o.Branch)

	switch o.Provider {
	case providerKind:
		exists, err := kindClusterExists(ctx, o.ClusterName)
		if err != nil {
			fmt.Printf("Kind cluster: unknown (%v)\n", err)
		} else {
			fmt.Printf("Kind cluster: %t\n", exists)
		}
	case providerTalosDocker:
		fmt.Printf("Talos Docker cluster: %t\n", talosClusterExists(ctx, o.ClusterName))
	}

	if out, err := output(ctx, o.StacksRepo, os.Environ(), "kubectl", "config", "current-context"); err == nil {
		fmt.Printf("Kubectl context: %s\n", strings.TrimSpace(out))
	} else {
		fmt.Printf("Kubectl context: unavailable (%v)\n", err)
	}

	appJSONPaths := []struct {
		label string
		path  string
	}{
		{"Root app sync", "{.status.sync.status}"},
		{"Root app health", "{.status.health.status}"},
		{"Root app repo", "{.spec.source.repoURL}"},
		{"Root app target", "{.spec.source.targetRevision}"},
	}
	for _, item := range appJSONPaths {
		out, err := output(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "get", "application", "root-application", "-n", "argocd", "-o", "jsonpath="+item.path)
		if err != nil {
			fmt.Printf("%s: unavailable\n", item.label)
			continue
		}
		fmt.Printf("%s: %s\n", item.label, strings.TrimSpace(out))
	}
	if commit, err := latestGiteaCommit(ctx, o); err == nil {
		fmt.Printf("Last Gitea snapshot: %s\n", commit)
	} else {
		fmt.Printf("Last Gitea snapshot: unavailable\n")
	}
	if hook, err := giteaArgoCDWebhookStatus(ctx, o); err == nil {
		if hook.Ready {
			fmt.Printf("Gitea ArgoCD webhook: ready (id=%d url=%s)\n", hook.ID, hook.URL)
		} else {
			fmt.Printf("Gitea ArgoCD webhook: %s (url=%s)\n", hook.Message, hook.URL)
		}
	} else {
		fmt.Printf("Gitea ArgoCD webhook: unavailable (%v)\n", err)
	}

	if out, err := output(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "get", "pods", "-n", "gitea", "--no-headers"); err == nil {
		fmt.Printf("Gitea pods:\n%s", out)
	} else {
		fmt.Printf("Gitea pods: unavailable\n")
	}
	if out, err := output(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "get", "pods", "-n", "argocd", "--no-headers"); err == nil {
		fmt.Printf("ArgoCD pods:\n%s", out)
	} else {
		fmt.Printf("ArgoCD pods: unavailable\n")
	}
	return nil
}

func giteaArgoCDWebhookStatus(ctx context.Context, o *options) (giteaWebhookStatus, error) {
	pf, err := startGiteaPortForward(ctx)
	if err != nil {
		return giteaWebhookStatus{}, err
	}
	defer pf.stop()
	hooks, err := listGiteaSystemHooks(ctx, pf.port, o)
	if err != nil {
		return giteaWebhookStatus{}, err
	}
	return classifyGiteaArgoCDWebhook(hooks), nil
}

func latestGiteaCommit(ctx context.Context, o *options) (string, error) {
	pf, err := startGiteaPortForward(ctx)
	if err != nil {
		return "", err
	}
	defer pf.stop()
	out, err := output(ctx, o.StacksRepo, os.Environ(), "git", "ls-remote", giteaRemoteURL(pf.port, o), "refs/heads/"+o.Branch)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(out)
	if len(fields) == 0 {
		return "", fmt.Errorf("no ref returned for %s", o.Branch)
	}
	return fields[0], nil
}
