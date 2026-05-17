# Stacks Talos Podman CI

The `stacks` Talos provider keeps the existing Talos Docker workflow and can
run it against rootful Podman's Docker-compatible socket. This removes the
Docker daemon dependency for Talos-parity CI while preserving
`talosctl cluster create docker` behavior.

Rootless Podman is intentionally unsupported for `--provider talos-docker`.
Use `--provider kind` for rootless kind experiments. A future Talos QEMU path
would be the right place for Docker-free Talos without a container engine.

## Runner Setup

Install the tools used by the stacks bootstrap:

```bash
sudo apt-get update
sudo apt-get install -y jq podman skopeo yq
```

Install `talosctl` and `kubectl` using the versions required by the target
stacks branch.

Enable the rootful Podman socket and point Docker-compatible clients at it:

```bash
sudo systemctl enable --now podman.socket
export DOCKER_HOST=unix:///run/podman/podman.sock
```

The socket must be rootful. The stacks command fails fast when the Podman
socket reports rootless mode.

## Create

```bash
idpbuilder stacks create \
  --provider talos-docker \
  --container-engine podman \
  --seed-image-push-engine skopeo \
  --skip-tekton-builds \
  --refresh-kubeconfig=false \
  --strict-access=false
```

`--container-engine podman` controls the explicit stale-container cleanup
commands. `talosctl cluster create docker` still runs through `talosctl` and
inherits `DOCKER_HOST`, so it talks to the rootful Podman socket.

`--seed-image-push-engine skopeo` makes `seed-ryzen-images.sh` copy bootstrap
images without relying on Docker.

## Recreate And Cleanup

Use the same engine settings for recreate so stale Talos containers and the
cluster network are removed through Podman:

```bash
idpbuilder stacks create \
  --recreate \
  --provider talos-docker \
  --container-engine podman \
  --seed-image-push-engine skopeo \
  --skip-tekton-builds \
  --refresh-kubeconfig=false \
  --strict-access=false
```

Archive `talosctl cluster show --name ryzen`, idpbuilder telemetry, and
`deployment/scripts/cluster-readiness.sh summary` output from the run.
