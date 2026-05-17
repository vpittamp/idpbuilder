package stacks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cnoe-io/idpbuilder/pkg/cmd/helpers"
	"github.com/spf13/cobra"
	"k8s.io/client-go/util/homedir"
)

const (
	providerKind        = "kind"
	providerTalosDocker = "talos-docker"

	defaultProfile                    = "ryzen"
	defaultOverlay                    = "packages/overlays/ryzen"
	defaultLegacyKindOverlay          = "packages/overlays/kind-ryzen"
	defaultClusterName                = "ryzen"
	defaultReadinessProfile           = "deployment/config/readiness/ryzen.yaml"
	defaultLegacyKindReadinessProfile = "deployment/config/readiness/kind-ryzen.yaml"
	defaultBranch                     = "main"
	defaultGiteaUser                  = "giteaadmin"
	defaultGiteaPass                  = "developer"
)

type options struct {
	Profile       string
	Provider      string
	StacksRepo    string
	Overlay       string
	Branch        string
	ClusterName   string
	GiteaOwner    string
	GiteaRepo     string
	GiteaUser     string
	GiteaPassword string
	Watch         bool
	WatchInterval time.Duration
	Recreate      bool

	SkipAzureCheck     bool
	SkipArgocdInit     bool
	SkipTektonBuild    bool
	SeedImages         bool
	SeedImagesMode     string
	SeedImageJobs      int
	RefreshKubeconfig  bool
	PushKubeconfigHost string
	EnforceSLO         bool
	StrictAccess       bool
	ReadinessProfile   string

	RewriteBootstrapImagePins bool

	KubeVersion string

	TalosImage         string
	TalosSubnet        string
	TalosWorkers       int
	TalosControlMemory string
	TalosWorkerMemory  string
	TalosControlCPUs   string
	TalosWorkerCPUs    string
	TalosOIDCIssuerURL string
	TalosConfigPatches []string
	TalosMounts        []string
	TalosExposedPorts  []string
}

// StacksCmd adds PittampalliOrg stacks-specific local cluster workflows without
// changing the upstream idpbuilder create/delete/get behavior.
var StacksCmd = newStacksCmd()

func newStacksCmd() *cobra.Command {
	opts := defaultOptions()
	cmd := &cobra.Command{
		Use:          "stacks",
		Short:        "Manage PittampalliOrg stacks local development clusters",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := helpers.SetLogger(); err != nil {
				return err
			}
			opts.applyProviderDefaults(cmd)
			return opts.validate()
		},
	}

	cmd.PersistentFlags().StringVar(&opts.Profile, "profile", opts.Profile, "stacks profile to use")
	cmd.PersistentFlags().StringVar(&opts.Provider, "provider", opts.Provider, "cluster provider: talos-docker or kind")
	cmd.PersistentFlags().StringVar(&opts.StacksRepo, "stacks-repo", opts.StacksRepo, "path to the PittampalliOrg/stacks repository")
	cmd.PersistentFlags().StringVar(&opts.Overlay, "overlay", opts.Overlay, "overlay path inside the stacks repository")
	cmd.PersistentFlags().StringVar(&opts.Branch, "branch", opts.Branch, "branch to publish into in-cluster Gitea")
	cmd.PersistentFlags().StringVar(&opts.ClusterName, "cluster-name", opts.ClusterName, "local cluster name")
	cmd.PersistentFlags().StringVar(&opts.GiteaOwner, "gitea-owner", opts.GiteaOwner, "Gitea owner for the stacks repository")
	cmd.PersistentFlags().StringVar(&opts.GiteaRepo, "gitea-repo", opts.GiteaRepo, "Gitea repository name for stacks")
	cmd.PersistentFlags().StringVar(&opts.GiteaUser, "gitea-user", opts.GiteaUser, "Gitea username")
	cmd.PersistentFlags().StringVar(&opts.GiteaPassword, "gitea-password", opts.GiteaPassword, "Gitea password")

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create or reconcile a stacks local cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := create(cmd.Context(), opts); err != nil {
				return err
			}
			if opts.Watch {
				return watchAndSync(cmd.Context(), opts)
			}
			return nil
		},
	}
	createCmd.Flags().BoolVar(&opts.Recreate, "recreate", false, "delete and recreate the local cluster")
	createCmd.Flags().BoolVar(&opts.Watch, "watch", false, "continue syncing local worktree changes")
	createCmd.Flags().DurationVar(&opts.WatchInterval, "watch-interval", 3*time.Second, "polling interval used with --watch")
	createCmd.Flags().BoolVar(&opts.SkipAzureCheck, "skip-azure-check", true, "skip stacks Azure authentication check during local create")
	createCmd.Flags().BoolVar(&opts.SkipArgocdInit, "skip-argocd-init", false, "skip ArgoCD CLI token initialization")
	createCmd.Flags().BoolVar(&opts.SkipTektonBuild, "skip-tekton-builds", true, "skip background Tekton image build triggers")
	createCmd.Flags().BoolVar(&opts.SeedImages, "seed-images", true, "seed ryzen bootstrap images from release pins into local Gitea")
	createCmd.Flags().StringVar(&opts.SeedImagesMode, "seed-images-mode", "release-pins", "bootstrap image seed mode; only release-pins is supported")
	createCmd.Flags().IntVar(&opts.SeedImageJobs, "seed-image-jobs", 4, "number of ryzen bootstrap images to seed concurrently")
	createCmd.Flags().BoolVar(&opts.RefreshKubeconfig, "refresh-kubeconfig", true, "refresh local kubeconfig after create")
	createCmd.Flags().StringVar(&opts.PushKubeconfigHost, "push-kubeconfig-host", "", "optional SSH host to receive refreshed Tailscale kubeconfig")
	createCmd.Flags().BoolVar(&opts.EnforceSLO, "enforce-slo", false, "fail when recreate timings regress beyond the readiness baseline")
	createCmd.Flags().BoolVar(&opts.StrictAccess, "strict-access", false, "fail create when the remote Tailscale access cohort is not ready")
	createCmd.Flags().StringVar(&opts.ReadinessProfile, "readiness-profile", "", "cluster readiness profile path")
	createCmd.Flags().StringVar(&opts.KubeVersion, "kube-version", "", "Kubernetes version for talos-docker; default is read from the dev Talos claim")
	createCmd.Flags().StringVar(&opts.TalosImage, "talos-image", "", "Talos image for talos-docker; default is read from the dev Talos claim")
	createCmd.Flags().StringVar(&opts.TalosSubnet, "talos-subnet", "10.6.0.0/24", "Docker subnet for talos-docker")
	createCmd.Flags().IntVar(&opts.TalosWorkers, "talos-workers", 2, "worker count for talos-docker")
	createCmd.Flags().StringVar(&opts.TalosControlMemory, "talos-controlplane-memory", opts.TalosControlMemory, "memory limit for each talos-docker control plane node")
	createCmd.Flags().StringVar(&opts.TalosWorkerMemory, "talos-worker-memory", opts.TalosWorkerMemory, "memory limit for each talos-docker worker node")
	createCmd.Flags().StringVar(&opts.TalosControlCPUs, "talos-controlplane-cpus", opts.TalosControlCPUs, "CPU share for each talos-docker control plane node")
	createCmd.Flags().StringVar(&opts.TalosWorkerCPUs, "talos-worker-cpus", opts.TalosWorkerCPUs, "CPU share for each talos-docker worker node")
	createCmd.Flags().StringVar(&opts.TalosOIDCIssuerURL, "talos-oidc-issuer-url", opts.TalosOIDCIssuerURL, "service-account issuer URL for talos-docker Azure Workload Identity")
	createCmd.Flags().StringSliceVar(&opts.TalosConfigPatches, "talos-config-patch", nil, "Talos machine config patch passed to talosctl cluster create docker")
	createCmd.Flags().StringSliceVar(&opts.TalosMounts, "talos-mount", nil, "Docker mount passed to talosctl cluster create docker")
	createCmd.Flags().StringSliceVarP(&opts.TalosExposedPorts, "talos-exposed-port", "p", opts.TalosExposedPorts, "port mapping passed to talosctl cluster create docker")

	syncCmd := &cobra.Command{
		Use:   "sync",
		Short: "Snapshot the local stacks worktree into in-cluster Gitea",
		RunE: func(cmd *cobra.Command, args []string) error {
			syncOptions := *opts
			syncOptions.RewriteBootstrapImagePins = opts.SeedImages && opts.SeedImagesMode == "release-pins"
			_, err := sync(cmd.Context(), &syncOptions)
			return err
		},
	}
	syncCmd.Flags().BoolVar(&opts.SeedImages, "seed-images", true, "rewrite ryzen bootstrap image references from release pins into the local Gitea snapshot")
	syncCmd.Flags().StringVar(&opts.SeedImagesMode, "seed-images-mode", "release-pins", "bootstrap image rewrite mode; only release-pins is supported")

	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show local stacks cluster and GitOps status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return status(cmd.Context(), opts)
		},
	}

	cmd.AddCommand(createCmd, syncCmd, statusCmd)
	return cmd
}

func defaultOptions() *options {
	stacksRepo := os.Getenv("STACKS_DIR")
	if stacksRepo == "" {
		if home := homedir.HomeDir(); home != "" {
			stacksRepo = filepath.Join(home, "repos", "PittampalliOrg", "stacks", "main")
		}
	}
	return &options{
		Profile:            defaultProfile,
		Provider:           providerTalosDocker,
		StacksRepo:         stacksRepo,
		Overlay:            defaultOverlay,
		Branch:             defaultBranch,
		ClusterName:        defaultClusterName,
		GiteaOwner:         defaultGiteaUser,
		GiteaRepo:          "stacks",
		GiteaUser:          defaultGiteaUser,
		GiteaPassword:      defaultGiteaPass,
		WatchInterval:      3 * time.Second,
		SkipAzureCheck:     true,
		SkipTektonBuild:    true,
		SeedImages:         true,
		SeedImagesMode:     "release-pins",
		SeedImageJobs:      4,
		RefreshKubeconfig:  true,
		ReadinessProfile:   defaultReadinessProfile,
		TalosSubnet:        "10.6.0.0/24",
		TalosWorkers:       2,
		TalosControlMemory: "6GiB",
		TalosWorkerMemory:  "6GiB",
		TalosControlCPUs:   "4.0",
		TalosWorkerCPUs:    "3.0",
		TalosOIDCIssuerURL: "https://oidcissuer65846b7df97b.z13.web.core.windows.net/",
		TalosExposedPorts: []string{
			"9443:443/tcp",
		},
	}
}

func (o *options) validate() error {
	if o.Profile != defaultProfile {
		return fmt.Errorf("unsupported stacks profile %q; only %q is implemented", o.Profile, defaultProfile)
	}
	if o.Provider != providerKind && o.Provider != providerTalosDocker {
		return fmt.Errorf("unsupported provider %q; expected %q or %q", o.Provider, providerKind, providerTalosDocker)
	}
	if o.StacksRepo == "" {
		return fmt.Errorf("--stacks-repo is required")
	}
	abs, err := filepath.Abs(o.StacksRepo)
	if err != nil {
		return fmt.Errorf("resolving stacks repo path: %w", err)
	}
	o.StacksRepo = abs
	if st, err := os.Stat(o.StacksRepo); err != nil {
		return fmt.Errorf("checking stacks repo %s: %w", o.StacksRepo, err)
	} else if !st.IsDir() {
		return fmt.Errorf("stacks repo %s is not a directory", o.StacksRepo)
	}
	if o.ReadinessProfile != "" && !filepath.IsAbs(o.ReadinessProfile) {
		o.ReadinessProfile = filepath.Join(o.StacksRepo, o.ReadinessProfile)
	}
	if o.Overlay == "" {
		return fmt.Errorf("--overlay is required")
	}
	if o.Branch == "" {
		return fmt.Errorf("--branch is required")
	}
	if o.ClusterName == "" {
		return fmt.Errorf("--cluster-name is required")
	}
	if o.GiteaOwner == "" || o.GiteaRepo == "" || o.GiteaUser == "" {
		return fmt.Errorf("gitea owner, repo, and user are required")
	}
	if o.GiteaPassword == "" {
		return fmt.Errorf("--gitea-password is required")
	}
	if o.SeedImagesMode != "" && o.SeedImagesMode != "release-pins" {
		return fmt.Errorf("unsupported --seed-images-mode %q; only release-pins is implemented", o.SeedImagesMode)
	}
	if o.SeedImageJobs < 1 {
		return fmt.Errorf("--seed-image-jobs must be at least 1")
	}
	return nil
}

func (o *options) applyProviderDefaults(cmd *cobra.Command) {
	if o.Provider == providerKind {
		if !flagChanged(cmd, "overlay") && o.Overlay == defaultOverlay {
			o.Overlay = defaultLegacyKindOverlay
		}
		if !flagChanged(cmd, "readiness-profile") && (o.ReadinessProfile == "" || o.ReadinessProfile == defaultReadinessProfile || o.ReadinessProfile == filepath.Join(o.StacksRepo, defaultReadinessProfile)) {
			o.ReadinessProfile = filepath.Join(o.StacksRepo, defaultLegacyKindReadinessProfile)
		}
		return
	}
	if !flagChanged(cmd, "readiness-profile") && (o.ReadinessProfile == "" || o.ReadinessProfile == defaultReadinessProfile) {
		o.ReadinessProfile = filepath.Join(o.StacksRepo, defaultReadinessProfile)
	}
}

func flagChanged(cmd *cobra.Command, name string) bool {
	if f := cmd.Flags().Lookup(name); f != nil {
		return f.Changed
	}
	if f := cmd.InheritedFlags().Lookup(name); f != nil {
		return f.Changed
	}
	if f := cmd.PersistentFlags().Lookup(name); f != nil {
		return f.Changed
	}
	return false
}

func withStacksEnv(o *options, extra ...string) []string {
	env := os.Environ()
	env = append(env,
		"STACKS_DIR="+o.StacksRepo,
		"CLUSTER_NAME="+o.ClusterName,
		"CLUSTER_HOST="+o.ClusterName,
		"GIT_BRANCH="+o.Branch,
		"GIT_REPO_URL=http://gitea-http.gitea.svc.cluster.local:3000/"+o.GiteaOwner+"/"+o.GiteaRepo+".git",
		"STACKS_GITEA_PRIMARY_BRANCH="+o.Branch,
		"STACKS_GITEA_OWNER="+o.GiteaOwner,
		"STACKS_GITEA_REPO="+o.GiteaRepo,
		"STACKS_GITEA_USER="+o.GiteaUser,
	)
	if o.SkipAzureCheck {
		env = append(env, "SKIP_AZURE_CHECK=true")
	}
	if o.SkipArgocdInit {
		env = append(env, "SKIP_ARGOCD_INIT=true")
	}
	if o.SkipTektonBuild {
		env = append(env, "SKIP_TEKTON_BUILDS=true")
	}
	if o.ReadinessProfile != "" {
		env = append(env, "READINESS_PROFILE="+o.ReadinessProfile)
	}
	env = append(env, extra...)
	return env
}

func watchAndSync(ctx context.Context, o *options) error {
	fmt.Printf("Watching %s for stacks changes every %s\n", o.StacksRepo, o.WatchInterval)
	last, err := snapshotHash(ctx, o.StacksRepo)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(o.WatchInterval)
	defer ticker.Stop()
	for {
		hash, err := snapshotHash(ctx, o.StacksRepo)
		if err != nil {
			return err
		}
		if hash != last {
			if _, err := sync(ctx, o); err != nil {
				return err
			}
			last = hash
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}
