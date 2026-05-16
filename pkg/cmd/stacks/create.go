package stacks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func create(ctx context.Context, o *options) (err error) {
	startReadinessRun(ctx, o)
	defer func() {
		if finishErr := finishReadiness(ctx, o); err == nil && finishErr != nil {
			err = finishErr
		}
	}()

	if !o.SkipAzureCheck {
		if err := withReadinessPhase(ctx, o, "azure-check", func() error {
			return run(ctx, o.StacksRepo, withStacksEnv(o), "bash", "-lc", fmt.Sprintf("source %q && check-azure-auth", filepath.Join(o.StacksRepo, "deployment", "scripts", "cluster-menu.sh")))
		}); err != nil {
			return err
		}
	}
	switch o.Provider {
	case providerKind:
		return createKind(ctx, o)
	case providerTalosDocker:
		return createTalosDocker(ctx, o)
	default:
		return fmt.Errorf("unsupported provider %q", o.Provider)
	}
}

func createKind(ctx context.Context, o *options) error {
	exists, err := kindClusterExists(ctx, o.ClusterName)
	if err != nil {
		return err
	}
	if exists && o.Recreate {
		_ = withReadinessPhase(ctx, o, "tailscale-cleanup-pre-delete", func() error {
			cleanupTailscaleDevices(ctx, o, tailscaleCleanupNoWait)
			return nil
		})
		if err := withReadinessPhase(ctx, o, "kind-delete", func() error {
			return run(ctx, o.StacksRepo, withStacksEnv(o), "kind", "delete", "cluster", "--name", o.ClusterName)
		}); err != nil {
			return err
		}
		_ = withReadinessPhase(ctx, o, "tailscale-cleanup-post-delete", func() error {
			cleanupTailscaleDevices(ctx, o, tailscaleCleanupWait)
			return nil
		})
		exists = false
	}
	if !exists {
		_ = withReadinessPhase(ctx, o, "tailscale-cleanup-pre-create", func() error {
			cleanupTailscaleDevices(ctx, o, tailscaleCleanupWaitIfDeleted)
			return nil
		})
		if err := withReadinessPhase(ctx, o, "registry-auth", func() error {
			return run(ctx, o.StacksRepo, withStacksEnv(o), filepath.Join(o.StacksRepo, "deployment", "scripts", "setup-registry-auth.sh"))
		}); err != nil {
			return err
		}
		kindConfig, cleanup, err := renderKindConfig(o)
		if err != nil {
			return err
		}
		defer cleanup()
		if err := withReadinessPhase(ctx, o, "kind-create", func() error {
			return run(ctx, o.StacksRepo, withStacksEnv(o), "kind", "create", "cluster", "--name", o.ClusterName, "--config", kindConfig)
		}); err != nil {
			return err
		}
		if err := withReadinessPhase(ctx, o, "registry-ip-patch", func() error {
			return run(ctx, o.StacksRepo, withStacksEnv(o), "bash", "-lc", fmt.Sprintf("source %q && patch_gitea_registry_ip %q", filepath.Join(o.StacksRepo, "deployment", "scripts", "setup-registry-auth.sh"), o.ClusterName))
		}); err != nil {
			return err
		}
		_ = withReadinessPhase(ctx, o, "preload-tailscale-images", func() error {
			preloadTailscaleImages(ctx, o)
			return nil
		})
	} else {
		fmt.Printf("Kind cluster %q already exists; reconciling bootstrap and Git snapshot\n", o.ClusterName)
	}

	return bootstrapStacksGitOps(ctx, o)
}

func createTalosDocker(ctx context.Context, o *options) error {
	if o.Recreate {
		_ = withReadinessPhase(ctx, o, "talos-docker-delete", func() error {
			return run(ctx, o.StacksRepo, withStacksEnv(o), "talosctl", "cluster", "destroy", "--name", o.ClusterName)
		})
	}
	exists := talosClusterExists(ctx, o.ClusterName)
	if !exists {
		args, err := talosDockerCreateArgs(o)
		if err != nil {
			return err
		}
		if err := withReadinessPhase(ctx, o, "talos-docker-create", func() error {
			return run(ctx, o.StacksRepo, withStacksEnv(o), "talosctl", args...)
		}); err != nil {
			return err
		}
	} else {
		fmt.Printf("Talos Docker cluster %q already exists; reconciling bootstrap and Git snapshot\n", o.ClusterName)
	}
	return bootstrapStacksGitOps(ctx, o)
}

func bootstrapStacksGitOps(ctx context.Context, o *options) error {
	env := withStacksEnv(o)
	if o.Provider == providerTalosDocker {
		// bootstrap-local-infra.sh uses CLUSTER_HOST only for kind-node registry
		// verification. Talos Docker does not expose kind nodes, so skip that
		// verification while preserving the rest of the shared bootstrap.
		env = append(env, "CLUSTER_HOST=")
	}
	if err := withReadinessPhase(ctx, o, "bootstrap-local-infra", func() error {
		return run(ctx, o.StacksRepo, env, filepath.Join(o.StacksRepo, "deployment", "scripts", "bootstrap-local-infra.sh"))
	}); err != nil {
		return err
	}
	if err := withReadinessPhase(ctx, o, "seed-gitea-repos", func() error {
		return run(ctx, o.StacksRepo, env, "bash", "-lc", fmt.Sprintf("source %q && ensure-kargo-repos %q && seed-workflow-builder-gitea-repo", filepath.Join(o.StacksRepo, "deployment", "scripts", "cluster-menu.sh"), o.ClusterName))
	}); err != nil {
		return err
	}
	syncOptions := *o
	syncOptions.RewriteBootstrapImagePins = o.SeedImages && o.SeedImagesMode == "release-pins"
	if err := withReadinessPhase(ctx, o, "sync-stacks-gitea", func() error {
		_, err := sync(ctx, &syncOptions)
		return err
	}); err != nil {
		return err
	}
	if o.SeedImages {
		if err := withReadinessPhase(ctx, o, "seed-bootstrap-images", func() error {
			return seedBootstrapImages(ctx, o)
		}); err != nil {
			return err
		}
	}
	if err := withReadinessPhase(ctx, o, "install-argocd", func() error {
		return run(ctx, o.StacksRepo, env, filepath.Join(o.StacksRepo, "deployment", "scripts", "01-install-argocd.sh"))
	}); err != nil {
		return err
	}
	if err := withReadinessPhase(ctx, o, "argocd-repo-secret", func() error {
		return ensureArgoRepoSecret(ctx, o)
	}); err != nil {
		return err
	}
	bootstrapFile, cleanup, err := renderArgoBootstrap(o)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := withReadinessPhase(ctx, o, "apply-root-app", func() error {
		return run(ctx, o.StacksRepo, env, "kubectl", "apply", "-f", bootstrapFile)
	}); err != nil {
		return err
	}
	_ = withReadinessPhase(ctx, o, "sync-jwks", func() error {
		syncJWKS(ctx, o)
		return nil
	})
	if !o.SkipArgocdInit {
		_ = withReadinessPhase(ctx, o, "argocd-init", func() error {
			authEnv := append([]string{}, env...)
			authEnv = append(authEnv, "ARGOCD_AUTH_1PASSWORD=disabled", "ARGOCD_LOCAL_PASSWORD=developer")
			return run(ctx, o.StacksRepo, authEnv, "bash", "-lc", fmt.Sprintf("source %q && sleep 2 && argocd-auth-init && sync-manual-apps", filepath.Join(o.StacksRepo, "deployment", "scripts", "cluster-menu.sh")))
		})
	}
	for _, cohort := range []string{"bootstrap", "gitops-core", "inner-loop", "observability", "all"} {
		if err := withReadinessPhase(ctx, o, "wait-"+cohort, func() error {
			return waitReadinessCohort(ctx, o, cohort)
		}); err != nil {
			return err
		}
	}
	accessPhase := "check-access"
	accessCheck := checkReadinessCohort
	if o.StrictAccess {
		accessPhase = "wait-access"
		accessCheck = waitReadinessCohort
	}
	accessErr := withReadinessPhase(ctx, o, accessPhase, func() error {
		return accessCheck(ctx, o, "access")
	})
	if accessErr != nil {
		if o.StrictAccess {
			return accessErr
		}
		fmt.Fprintf(os.Stderr, "warning: remote access cohort is not ready yet; continuing without blocking local recreate: %v\n", accessErr)
	}
	if o.RefreshKubeconfig {
		if err := withReadinessPhase(ctx, o, "refresh-kubeconfig", func() error {
			return refreshRyzenKubeconfig(ctx, o)
		}); err != nil {
			return err
		}
	}
	fmt.Println("Stacks local GitOps bootstrap reconciled")
	return nil
}

func seedBootstrapImages(ctx context.Context, o *options) error {
	script := filepath.Join(o.StacksRepo, "deployment", "scripts", "bootstrap", "seed-ryzen-images.sh")
	args := []string{script, "--mode", "critical"}
	if o.SeedImagesMode == "release-pins" {
		args = append(args, "--pins", filepath.Join(o.StacksRepo, "packages", "components", "hub-spoke-appsets", "release-pins", "workflow-builder-images.yaml"))
	}
	return run(ctx, o.StacksRepo, withStacksEnv(o), "bash", args...)
}

func refreshRyzenKubeconfig(ctx context.Context, o *options) error {
	script := filepath.Join(o.StacksRepo, "deployment", "scripts", "tailscale", "refresh-ryzen-kubeconfig.sh")
	args := []string{script, "--cluster", o.ClusterName, "--service", o.ClusterName + "-k8s-api"}
	if o.StrictAccess {
		args = append(args, "--strict-remote-verify")
	}
	if o.PushKubeconfigHost != "" {
		args = append(args, "--push-host", o.PushKubeconfigHost)
	}
	return run(ctx, o.StacksRepo, withStacksEnv(o), "bash", args...)
}

func ensureArgoRepoSecret(ctx context.Context, o *options) error {
	repoURL := fmt.Sprintf("http://gitea-http.gitea.svc.cluster.local:3000/%s/%s.git", o.GiteaOwner, o.GiteaRepo)
	args := []string{
		"create", "secret", "generic", "repo-stacks-internal",
		"-n", "argocd",
		"--from-literal=url=" + repoURL,
		"--from-literal=username=" + o.GiteaUser,
		"--from-literal=password=" + o.GiteaPassword,
		"--from-literal=type=git",
		"--from-literal=project=default",
		"--dry-run=client", "-o", "yaml",
	}
	out, err := output(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", args...)
	if err != nil {
		return err
	}
	cmd := commandWithStdin(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(out)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("applying argocd repository secret: %w", err)
	}
	if err := run(ctx, o.StacksRepo, withStacksEnv(o), "kubectl", "label", "secret", "repo-stacks-internal", "-n", "argocd", "argocd.argoproj.io/secret-type=repository", "--overwrite"); err != nil {
		return err
	}
	return nil
}

func syncJWKS(ctx context.Context, o *options) {
	jwks := filepath.Join(o.StacksRepo, "ref-implementation", "azure-workload-identity", "sync-jwks-to-azure.sh")
	if _, err := os.Stat(jwks); err == nil {
		_ = run(ctx, o.StacksRepo, withStacksEnv(o), jwks)
	}
}

type tailscaleCleanupWaitMode string

const (
	tailscaleCleanupNoWait        tailscaleCleanupWaitMode = ""
	tailscaleCleanupWait          tailscaleCleanupWaitMode = "--wait"
	tailscaleCleanupWaitIfDeleted tailscaleCleanupWaitMode = "--wait-if-deleted"
)

func cleanupTailscaleDevices(ctx context.Context, o *options, waitMode tailscaleCleanupWaitMode) {
	script := filepath.Join(o.StacksRepo, "deployment", "scripts", "tailscale", "cleanup-old-devices.sh")
	if _, err := os.Stat(script); err != nil {
		return
	}
	args := []string{script, "--cluster", o.ClusterName}
	if waitMode != tailscaleCleanupNoWait {
		args = append(args, string(waitMode))
	}
	if err := run(ctx, o.StacksRepo, withStacksEnv(o), "bash", args...); err != nil {
		fmt.Fprintf(os.Stderr, "warning: Tailscale cleanup failed: %v\n", err)
	}
}

func preloadTailscaleImages(ctx context.Context, o *options) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	images := []string{"tailscale/k8s-operator:v1.92.4", "tailscale/tailscale:v1.92.4"}
	for _, image := range images {
		_ = run(ctx, o.StacksRepo, withStacksEnv(o), "docker", "pull", image)
		_ = run(ctx, o.StacksRepo, withStacksEnv(o), "kind", "load", "docker-image", image, "--name", o.ClusterName)
	}
}

func kindClusterExists(ctx context.Context, name string) (bool, error) {
	out, err := output(ctx, "", os.Environ(), "kind", "get", "clusters")
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.TrimSpace(line) == name {
			return true, nil
		}
	}
	return false, nil
}

func talosClusterExists(ctx context.Context, name string) bool {
	_, err := output(ctx, "", os.Environ(), "talosctl", "cluster", "show", "--name", name)
	return err == nil
}
