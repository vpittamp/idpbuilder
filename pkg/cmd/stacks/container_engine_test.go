package stacks

import (
	"strings"
	"testing"
)

func TestTalosDockerCleanupCommandsUseSelectedEngine(t *testing.T) {
	o := defaultOptions()
	o.ClusterName = "ryzen"
	o.TalosWorkers = 2

	commands := talosDockerCleanupCommandsForEngine(o, containerEnginePodman)
	if len(commands) != 4 {
		t.Fatalf("cleanup command count = %d, want 4", len(commands))
	}
	for _, command := range commands {
		if command.name != containerEnginePodman {
			t.Fatalf("cleanup command used %q, want podman", command.name)
		}
		if strings.Contains(strings.Join(command.args, " "), "docker") {
			t.Fatalf("podman cleanup args should not contain docker: %#v", command.args)
		}
	}
	if got := strings.Join(commands[0].args, " "); got != "rm -f ryzen-controlplane-1" {
		t.Fatalf("controlplane cleanup args = %q", got)
	}
	if got := strings.Join(commands[3].args, " "); got != "network rm ryzen" {
		t.Fatalf("network cleanup args = %q", got)
	}
}

func TestSeedImagePushEngineAutoTracksContainerEngine(t *testing.T) {
	if got := resolveSeedImagePushEngineForContainerEngine(containerEngineDocker); got != seedImagePushEngineDocker {
		t.Fatalf("docker container engine resolved seed push engine %q, want docker", got)
	}
	if got := resolveSeedImagePushEngineForContainerEngine(containerEnginePodman); got != seedImagePushEngineSkopeo {
		t.Fatalf("podman container engine resolved seed push engine %q, want skopeo", got)
	}
}

func TestPodmanDockerHostValidation(t *testing.T) {
	for _, dockerHost := range []string{"", "unix:///var/run/docker.sock"} {
		if err := validatePodmanDockerHost(dockerHost); err == nil {
			t.Fatalf("validatePodmanDockerHost(%q) succeeded, want error", dockerHost)
		}
	}
	if err := validatePodmanDockerHost("unix:///run/podman/podman.sock"); err != nil {
		t.Fatalf("rootful Podman socket should validate: %v", err)
	}
}

func TestParsePodmanRootless(t *testing.T) {
	rootless, err := parsePodmanRootless("true\n")
	if err != nil {
		t.Fatal(err)
	}
	if !rootless {
		t.Fatalf("true parsed as rootful")
	}
	rootless, err = parsePodmanRootless("false\n")
	if err != nil {
		t.Fatal(err)
	}
	if rootless {
		t.Fatalf("false parsed as rootless")
	}
	if _, err := parsePodmanRootless("unknown"); err == nil {
		t.Fatalf("unknown rootless output parsed successfully, want error")
	}
}

func TestValidatePodmanRootfulRejectsRootless(t *testing.T) {
	err := validatePodmanRootful(true)
	if err == nil {
		t.Fatalf("rootless Podman validated successfully, want error")
	}
	if !strings.Contains(err.Error(), "requires rootful Podman") {
		t.Fatalf("rootless Podman error = %q, want rootful guidance", err)
	}
	if err := validatePodmanRootful(false); err != nil {
		t.Fatalf("rootful Podman validation failed: %v", err)
	}
}

func TestDefaultOptionsReadEngineEnv(t *testing.T) {
	t.Setenv("IDPBUILDER_STACKS_CONTAINER_ENGINE", containerEnginePodman)
	t.Setenv("IDPBUILDER_STACKS_SEED_IMAGE_PUSH_ENGINE", seedImagePushEngineSkopeo)

	o := defaultOptions()
	if o.ContainerEngine != containerEnginePodman {
		t.Fatalf("container engine = %q, want podman", o.ContainerEngine)
	}
	if o.SeedImagePushEngine != seedImagePushEngineSkopeo {
		t.Fatalf("seed image push engine = %q, want skopeo", o.SeedImagePushEngine)
	}
}

func TestValidateRejectsInvalidEngines(t *testing.T) {
	o := defaultOptions()
	o.StacksRepo = t.TempDir()
	o.ContainerEngine = "containerd"
	if err := o.validate(); err == nil || !strings.Contains(err.Error(), "--container-engine") {
		t.Fatalf("validate with invalid container engine returned %v, want --container-engine error", err)
	}

	o = defaultOptions()
	o.StacksRepo = t.TempDir()
	o.SeedImagePushEngine = "podman"
	if err := o.validate(); err == nil || !strings.Contains(err.Error(), "--seed-image-push-engine") {
		t.Fatalf("validate with invalid seed push engine returned %v, want --seed-image-push-engine error", err)
	}
}
