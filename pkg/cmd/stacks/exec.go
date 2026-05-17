package stacks

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func run(ctx context.Context, dir string, env []string, name string, args ...string) error {
	ctx, span, start := stacksTelemetry.startCommand(ctx, dir, name, args, "run")
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		stacksTelemetry.endCommand(ctx, name, start, span, err)
		return fmt.Errorf("running %s %s: %w", name, strings.Join(redactArgs(args), " "), err)
	}
	stacksTelemetry.endCommand(ctx, name, start, span, nil)
	return nil
}

func runWithStdin(ctx context.Context, dir string, env []string, stdin *strings.Reader, name string, args ...string) error {
	ctx, span, start := stacksTelemetry.startCommand(ctx, dir, name, args, "stdin")
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = stdin
	if err := cmd.Run(); err != nil {
		stacksTelemetry.endCommand(ctx, name, start, span, err)
		return fmt.Errorf("running %s %s: %w", name, strings.Join(redactArgs(args), " "), err)
	}
	stacksTelemetry.endCommand(ctx, name, start, span, nil)
	return nil
}

func output(ctx context.Context, dir string, env []string, name string, args ...string) (string, error) {
	ctx, span, start := stacksTelemetry.startCommand(ctx, dir, name, args, "output")
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = env
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		stacksTelemetry.endCommand(ctx, name, start, span, err)
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = strings.TrimSpace(stdout.String())
		}
		if msg == "" {
			msg = err.Error()
		}
		return "", fmt.Errorf("running %s %s: %s", name, strings.Join(redactArgs(args), " "), msg)
	}
	stacksTelemetry.endCommand(ctx, name, start, span, nil)
	return stdout.String(), nil
}

func redactArgs(args []string) []string {
	out := make([]string, len(args))
	copy(out, args)
	redactNext := false
	for i, arg := range out {
		if redactNext {
			out[i] = "<redacted>"
			redactNext = false
			continue
		}
		if isSensitiveFlag(arg) {
			redactNext = true
			continue
		}
		if redacted, ok := redactFromLiteral(arg); ok {
			out[i] = redacted
			continue
		}
		if redacted, ok := redactKeyValue(arg); ok {
			out[i] = redacted
			continue
		}
		if strings.Contains(arg, "://") && strings.Contains(arg, "@") {
			out[i] = "<redacted-url>"
		}
	}
	return out
}

func isSensitiveFlag(arg string) bool {
	switch strings.ToLower(strings.TrimSpace(arg)) {
	case "--password", "--passwd", "--token", "--secret", "--client-secret", "--api-key", "--gitea-password":
		return true
	default:
		return false
	}
}

func redactFromLiteral(arg string) (string, bool) {
	const prefix = "--from-literal="
	if !strings.HasPrefix(arg, prefix) {
		return "", false
	}
	literal := strings.TrimPrefix(arg, prefix)
	key, _, ok := strings.Cut(literal, "=")
	if !ok || !isSensitiveKey(key) {
		return "", false
	}
	return prefix + key + "=<redacted>", true
}

func redactKeyValue(arg string) (string, bool) {
	key, _, ok := strings.Cut(arg, "=")
	if !ok || !isSensitiveKey(key) {
		return "", false
	}
	return key + "=<redacted>", true
}

func isSensitiveKey(key string) bool {
	lower := strings.ToLower(strings.TrimLeft(key, "-"))
	for _, token := range []string{"password", "passwd", "token", "secret", "credential", "api-key", "apikey"} {
		if strings.Contains(lower, token) {
			return true
		}
	}
	return false
}
