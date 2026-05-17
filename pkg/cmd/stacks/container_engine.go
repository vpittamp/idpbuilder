package stacks

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

type commandInvocation struct {
	name string
	args []string
}

func resolveContainerEngine(o *options) (string, error) {
	switch o.ContainerEngine {
	case containerEngineDocker:
		if !commandAvailable(containerEngineDocker) {
			return "", fmt.Errorf("--container-engine=docker selected but docker was not found on PATH")
		}
		return containerEngineDocker, nil
	case containerEnginePodman:
		if !commandAvailable(containerEnginePodman) {
			return "", fmt.Errorf("--container-engine=podman selected but podman was not found on PATH")
		}
		return containerEnginePodman, nil
	case containerEngineAuto:
		if commandAvailable(containerEngineDocker) {
			return containerEngineDocker, nil
		}
		if commandAvailable(containerEnginePodman) {
			return containerEnginePodman, nil
		}
		return "", fmt.Errorf("--container-engine=auto could not find docker or podman on PATH")
	default:
		return "", fmt.Errorf("unsupported container engine %q", o.ContainerEngine)
	}
}

func commandAvailable(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func preflightTalosContainerEngine(ctx context.Context, o *options) error {
	engine, err := resolveContainerEngine(o)
	if err != nil {
		return err
	}
	if engine != containerEnginePodman {
		return nil
	}
	dockerHost := strings.TrimSpace(os.Getenv("DOCKER_HOST"))
	if err := validatePodmanDockerHost(dockerHost); err != nil {
		return err
	}
	rootless, err := podmanSocketRootless(ctx, o, dockerHost)
	if err != nil {
		return err
	}
	return validatePodmanRootful(rootless)
}

func validatePodmanDockerHost(dockerHost string) error {
	if dockerHost == "" {
		return fmt.Errorf("--container-engine=podman requires DOCKER_HOST to point at a rootful Podman Docker-compatible socket, for example unix:///run/podman/podman.sock")
	}
	if !strings.Contains(strings.ToLower(dockerHost), "podman") {
		return fmt.Errorf("--container-engine=podman requires DOCKER_HOST to point at a Podman socket; got %q", dockerHost)
	}
	return nil
}

func podmanSocketRootless(ctx context.Context, o *options, dockerHost string) (bool, error) {
	args := []string{"--remote", "--url", dockerHost, "info", "--format", "{{.Host.Security.Rootless}}"}
	out, err := output(ctx, o.StacksRepo, withStacksEnv(o), "podman", args...)
	if err != nil {
		return false, fmt.Errorf("checking Podman socket rootless mode: %w", err)
	}
	rootless, err := parsePodmanRootless(out)
	if err != nil {
		return false, err
	}
	return rootless, nil
}

func parsePodmanRootless(out string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(out)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("could not parse Podman rootless status from %q", strings.TrimSpace(out))
	}
}

func validatePodmanRootful(rootless bool) error {
	if rootless {
		return fmt.Errorf("Talos Docker parity requires rootful Podman; the Docker-compatible Podman socket in DOCKER_HOST is rootless. Use --provider kind for rootless kind experiments, or future talos-qemu work for Docker-free Talos without a container engine")
	}
	return nil
}

func resolveSeedImagePushEngine(o *options) (string, error) {
	switch o.SeedImagePushEngine {
	case seedImagePushEngineDocker, seedImagePushEngineSkopeo:
		return o.SeedImagePushEngine, nil
	case seedImagePushEngineAuto:
		containerEngine, err := resolveContainerEngine(o)
		if err != nil {
			return "", err
		}
		return resolveSeedImagePushEngineForContainerEngine(containerEngine), nil
	default:
		return "", fmt.Errorf("unsupported seed image push engine %q", o.SeedImagePushEngine)
	}
}

func resolveSeedImagePushEngineForContainerEngine(containerEngine string) string {
	if containerEngine == containerEnginePodman {
		return seedImagePushEngineSkopeo
	}
	return seedImagePushEngineDocker
}

func talosDockerCleanupCommands(o *options) ([]commandInvocation, error) {
	engine, err := resolveContainerEngine(o)
	if err != nil {
		return nil, err
	}
	return talosDockerCleanupCommandsForEngine(o, engine), nil
}

func talosDockerCleanupCommandsForEngine(o *options, engine string) []commandInvocation {
	commands := []commandInvocation{
		{name: engine, args: []string{"rm", "-f", o.ClusterName + "-controlplane-1"}},
	}
	for i := 1; i <= o.TalosWorkers; i++ {
		commands = append(commands, commandInvocation{name: engine, args: []string{"rm", "-f", fmt.Sprintf("%s-worker-%d", o.ClusterName, i)}})
	}
	commands = append(commands, commandInvocation{name: engine, args: []string{"network", "rm", o.ClusterName}})
	return commands
}
