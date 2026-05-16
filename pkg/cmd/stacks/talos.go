package stacks

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
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
