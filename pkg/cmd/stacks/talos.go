package stacks

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var talosClaimValuePattern = regexp.MustCompile(`^\s*(version|kubernetesVersion):\s*"?([^"\s]+)"?\s*$`)

func talosDockerCreateArgs(o *options) ([]string, error) {
	kubeVersion := o.KubeVersion
	talosImage := o.TalosImage
	if kubeVersion == "" || talosImage == "" {
		talosVersion, detectedKubeVersion, err := detectTalosVersions(o.StacksRepo)
		if err != nil {
			return nil, err
		}
		if kubeVersion == "" {
			kubeVersion = detectedKubeVersion
		}
		if talosImage == "" {
			talosImage = "ghcr.io/siderolabs/talos:v" + talosVersion
		}
	}
	if kubeVersion == "" {
		return nil, fmt.Errorf("could not determine Kubernetes version for talos-docker; set --kube-version")
	}
	if talosImage == "" {
		return nil, fmt.Errorf("could not determine Talos image for talos-docker; set --talos-image")
	}
	args := []string{
		"cluster", "create", "docker",
		"--name", o.ClusterName,
		"--workers", fmt.Sprintf("%d", o.TalosWorkers),
		"--kubernetes-version", strings.TrimPrefix(kubeVersion, "v"),
		"--image", talosImage,
		"--subnet", o.TalosSubnet,
		"--memory-controlplanes", o.TalosControlMemory,
		"--memory-workers", o.TalosWorkerMemory,
		"--cpus-controlplanes", o.TalosControlCPUs,
		"--cpus-workers", o.TalosWorkerCPUs,
	}
	for _, port := range o.TalosExposedPorts {
		if strings.TrimSpace(port) != "" {
			args = append(args, "--exposed-ports", port)
		}
	}
	for _, patch := range o.TalosConfigPatches {
		if strings.TrimSpace(patch) != "" {
			args = append(args, "--config-patch", patch)
		}
	}
	for _, mount := range o.TalosMounts {
		if strings.TrimSpace(mount) != "" {
			args = append(args, "--mount", mount)
		}
	}
	return args, nil
}

func prepareTalosDockerOptions(o *options) (*options, func(), error) {
	prepared := *o
	cleanups := []func(){}
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	if !hasValues(prepared.TalosMounts) {
		mount, err := defaultTalosDockerLocalPathMount(&prepared)
		if err != nil {
			return nil, cleanup, err
		}
		prepared.TalosMounts = []string{mount}
	}

	patch, patchCleanup, err := renderTalosDockerBootstrapPatch(&prepared)
	if err != nil {
		return nil, cleanup, err
	}
	cleanups = append(cleanups, patchCleanup)
	prepared.TalosConfigPatches = append([]string{"@" + patch}, prepared.TalosConfigPatches...)

	return &prepared, cleanup, nil
}

func renderTalosDockerBootstrapPatch(o *options) (string, func(), error) {
	controlPlaneIP, err := talosDockerControlPlaneIP(o.TalosSubnet)
	if err != nil {
		return "", nil, err
	}
	gatewayIP, err := talosDockerGatewayIP(o.TalosSubnet)
	if err != nil {
		return "", nil, err
	}
	hostPort := talosDockerHTTPSHostPort(o)
	legacyPortMirror := ""
	if hostPort != "8443" {
		legacyPortMirror = `---
apiVersion: v1alpha1
kind: RegistryMirrorConfig
name: gitea.cnoe.localtest.me:8443
endpoints:
  - url: https://gitea.cnoe.localtest.me
skipFallback: true
`
	}
	serviceAccountKey, err := talosDockerServiceAccountKey(o)
	if err != nil {
		return "", nil, err
	}
	issuerURL := strings.TrimSpace(o.TalosOIDCIssuerURL)
	if issuerURL == "" {
		return "", nil, fmt.Errorf("--talos-oidc-issuer-url is required")
	}
	content := fmt.Sprintf(`machine:
  nodeLabels:
    stacks.io/swebench-pool: dev-benchmark
  network:
    nameservers:
      - %s
cluster:
  apiServer:
    extraArgs:
      service-account-issuer: %s
  serviceAccount:
    key: %s
---
apiVersion: v1alpha1
kind: RegistryMirrorConfig
name: gitea.cnoe.localtest.me:%s
endpoints:
  - url: https://gitea.cnoe.localtest.me
skipFallback: true
%s---
apiVersion: v1alpha1
kind: RegistryMirrorConfig
name: gitea.cnoe.localtest.me
endpoints:
  - url: https://gitea.cnoe.localtest.me
skipFallback: true
---
apiVersion: v1alpha1
kind: RegistryMirrorConfig
name: docker.io
endpoints:
  - url: https://mirror.gcr.io
---
apiVersion: v1alpha1
kind: RegistryTLSConfig
name: gitea.cnoe.localtest.me
insecureSkipVerify: true
---
apiVersion: v1alpha1
kind: RegistryAuthConfig
name: gitea.cnoe.localtest.me
username: %s
password: %s
---
apiVersion: v1alpha1
kind: StaticHostConfig
name: %s
hostnames:
  - gitea.cnoe.localtest.me
`, gatewayIP, strconv.Quote(issuerURL), strconv.Quote(serviceAccountKey), hostPort, legacyPortMirror, strconv.Quote(o.GiteaUser), strconv.Quote(o.GiteaPassword), controlPlaneIP)
	return writeTempYAML("idpbuilder-stacks-talos-docker-*.yaml", content)
}

func talosDockerServiceAccountKey(o *options) (string, error) {
	keyPath := filepath.Join(o.StacksRepo, "ref-implementation", "azure-workload-identity", "keys", "sa.key")
	raw, err := os.ReadFile(keyPath)
	if err != nil {
		return "", fmt.Errorf("reading Azure Workload Identity service-account signing key %s: %w", keyPath, err)
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

func defaultTalosDockerLocalPathMount(o *options) (string, error) {
	stateHome := os.Getenv("XDG_STATE_HOME")
	if stateHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("resolving home directory for Talos Docker mount: %w", err)
		}
		stateHome = filepath.Join(home, ".local", "state")
	}
	dir := filepath.Join(stateHome, "idpbuilder-stacks", o.ClusterName, "local-path-provisioner")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("creating Talos Docker local-path mount %s: %w", dir, err)
	}
	return "type=bind,source=" + dir + ",target=/var/local-path-provisioner", nil
}

func talosDockerHostRegistry(o *options) string {
	return fmt.Sprintf("gitea.cnoe.localtest.me:%s/%s", talosDockerHTTPSHostPort(o), o.GiteaOwner)
}

func talosDockerHTTPSHostPort(o *options) string {
	for _, port := range o.TalosExposedPorts {
		port = strings.TrimSpace(port)
		if port == "" {
			continue
		}
		hostPort, containerSpec, ok := strings.Cut(port, ":")
		if !ok || hostPort == "" {
			continue
		}
		containerPort, protocol, _ := strings.Cut(containerSpec, "/")
		if containerPort == "443" && (protocol == "" || protocol == "tcp") {
			return hostPort
		}
	}
	return "9443"
}

func talosDockerControlPlaneIP(subnet string) (string, error) {
	return talosDockerSubnetIP(subnet, 2, "control-plane")
}

func talosDockerGatewayIP(subnet string) (string, error) {
	return talosDockerSubnetIP(subnet, 1, "gateway")
}

func talosDockerSubnetIP(subnet string, offset uint32, name string) (string, error) {
	ip, network, err := net.ParseCIDR(subnet)
	if err != nil {
		return "", fmt.Errorf("parsing --talos-subnet %q: %w", subnet, err)
	}
	ip = ip.To4()
	if ip == nil {
		return "", fmt.Errorf("--talos-subnet %q must be an IPv4 CIDR", subnet)
	}
	ones, bits := network.Mask.Size()
	if bits != 32 || ones > 30 {
		return "", fmt.Errorf("--talos-subnet %q must have at least two usable IPv4 addresses", subnet)
	}
	value := binary.BigEndian.Uint32(ip)
	value += offset
	out := make(net.IP, net.IPv4len)
	binary.BigEndian.PutUint32(out, value)
	if !network.Contains(out) {
		return "", fmt.Errorf("derived Talos Docker %s IP %s is outside subnet %s", name, out, subnet)
	}
	return out.String(), nil
}

func hasValues(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func detectTalosVersions(stacksRepo string) (talosVersion, kubeVersion string, err error) {
	claim := filepath.Join(stacksRepo, "packages", "components", "crossplane-hetzner-talos", "manifests", "crossplane-hcloud-compositions", "TalosSpokeClusterClaim-dev.yaml")
	f, err := os.Open(claim)
	if err != nil {
		return "", "", fmt.Errorf("opening Talos dev claim: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		match := talosClaimValuePattern.FindStringSubmatch(scanner.Text())
		if len(match) != 3 {
			continue
		}
		switch match[1] {
		case "version":
			talosVersion = strings.TrimPrefix(match[2], "v")
		case "kubernetesVersion":
			kubeVersion = strings.TrimPrefix(match[2], "v")
		}
	}
	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("reading Talos dev claim: %w", err)
	}
	return talosVersion, kubeVersion, nil
}
