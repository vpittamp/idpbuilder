package stacks

import (
	"context"
	"fmt"
	"os"
	"sort"
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
		{"Root app revision", "{.status.sync.revision}"},
		{"Root app repo", "{.spec.source.repoURL}"},
		{"Root app target", "{.spec.source.targetRevision}"},
	}
	rootRevision := ""
	for _, item := range appJSONPaths {
		out, err := output(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "get", "application", "root-application", "-n", "argocd", "-o", "jsonpath="+item.path)
		if err != nil {
			fmt.Printf("%s: unavailable\n", item.label)
			continue
		}
		value := strings.TrimSpace(out)
		if item.label == "Root app revision" {
			rootRevision = value
		}
		fmt.Printf("%s: %s\n", item.label, value)
	}
	snapshot := ""
	if commit, err := latestGiteaCommit(ctx, o); err == nil {
		snapshot = commit
		fmt.Printf("Last Gitea snapshot: %s\n", commit)
	} else {
		fmt.Printf("Last Gitea snapshot: unavailable\n")
	}
	var hook giteaWebhookStatus
	hookAvailable := false
	if currentHook, err := giteaArgoCDWebhookStatus(ctx, o); err == nil {
		hook = currentHook
		hookAvailable = true
		if hook.Ready {
			fmt.Printf("Gitea ArgoCD webhook: ready (id=%d url=%s)\n", hook.ID, hook.URL)
		} else {
			fmt.Printf("Gitea ArgoCD webhook: %s (url=%s)\n", hook.Message, hook.URL)
		}
	} else {
		fmt.Printf("Gitea ArgoCD webhook: unavailable (%v)\n", err)
	}
	apps, appsErr := listStackApplications(ctx, o)
	printHotLoopReadiness(snapshot, rootRevision, hook, hookAvailable, apps, appsErr)

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

type stackAppProblem struct {
	Name   string
	Sync   string
	Health string
}

func printHotLoopReadiness(snapshot, rootRevision string, hook giteaWebhookStatus, hookAvailable bool, apps argoApplicationList, appsErr error) {
	rootObserved := snapshot != "" && revisionMatches(rootRevision, snapshot)
	problems := unhealthyStackApplications(apps)
	verdict := hotLoopReadinessVerdict(snapshot, rootRevision, hook, hookAvailable, appsErr, problems)
	fmt.Println("Hot loop readiness:")
	if snapshot == "" || rootRevision == "" {
		fmt.Println("  Snapshot observed by root: unavailable")
	} else {
		fmt.Printf("  Snapshot observed by root: %t\n", rootObserved)
	}
	if appsErr != nil {
		fmt.Printf("  Stack applications: unavailable (%v)\n", appsErr)
	} else {
		fmt.Printf("  Stack applications: %d total, %d degraded\n", len(apps.Items), len(problems))
		for i, app := range problems {
			if i >= 5 {
				fmt.Printf("  Additional degraded applications: %d\n", len(problems)-i)
				break
			}
			fmt.Printf("  Degraded application: %s sync=%s health=%s\n", app.Name, app.Sync, app.Health)
		}
	}
	fmt.Printf("  Verdict: %s\n", verdict)
}

func hotLoopReadinessVerdict(snapshot, rootRevision string, hook giteaWebhookStatus, hookAvailable bool, appsErr error, problems []stackAppProblem) string {
	if snapshot == "" || rootRevision == "" || !hookAvailable || appsErr != nil {
		return "Hot loop unavailable"
	}
	if !revisionMatches(rootRevision, snapshot) || !hook.Ready || len(problems) > 0 {
		return "Hot loop degraded"
	}
	return "Hot loop ready"
}

func unhealthyStackApplications(apps argoApplicationList) []stackAppProblem {
	problems := make([]stackAppProblem, 0)
	for _, app := range apps.Items {
		syncStatus := strings.TrimSpace(app.Status.Sync.Status)
		healthStatus := strings.TrimSpace(app.Status.Health.Status)
		if syncStatus == "" {
			syncStatus = "Unknown"
		}
		if healthStatus == "" {
			healthStatus = "Unknown"
		}
		if syncStatus != "Synced" || healthStatus != "Healthy" {
			problems = append(problems, stackAppProblem{
				Name:   app.Metadata.Name,
				Sync:   syncStatus,
				Health: healthStatus,
			})
		}
	}
	sort.Slice(problems, func(i, j int) bool { return problems[i].Name < problems[j].Name })
	return problems
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
