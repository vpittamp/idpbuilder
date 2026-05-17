package stacks

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func readinessScript(o *options) string {
	return filepath.Join(o.StacksRepo, "deployment", "scripts", "cluster-readiness.sh")
}

func runReadiness(ctx context.Context, o *options, args ...string) error {
	script := readinessScript(o)
	if _, err := os.Stat(script); err != nil {
		return fmt.Errorf("readiness script not found at %s", script)
	}
	return run(ctx, o.StacksRepo, withStacksEnv(o), script, args...)
}

func startReadinessRun(ctx context.Context, o *options) {
	if err := runReadiness(ctx, o, "start-run"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not start readiness run: %v\n", err)
	}
}

func markReadinessPhase(ctx context.Context, o *options, phase, edge string) {
	if err := runReadiness(ctx, o, "mark-phase", phase, edge); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not mark readiness phase %s %s: %v\n", phase, edge, err)
	}
}

func withReadinessPhase(ctx context.Context, o *options, phase string, fn func() error) error {
	ctx, span, start := stacksTelemetry.startPhase(ctx, o, phase)
	markReadinessPhase(ctx, o, phase, "start")
	err := fn()
	markReadinessPhase(ctx, o, phase, "end")
	stacksTelemetry.endPhase(ctx, o, phase, start, span, err)
	return err
}

func waitReadinessCohort(ctx context.Context, o *options, cohort string) error {
	return runReadiness(ctx, o, "wait", "--cohort", cohort)
}

func checkReadinessCohort(ctx context.Context, o *options, cohort string) error {
	return runReadiness(ctx, o, "check", "--cohort", cohort)
}

func finishReadiness(ctx context.Context, o *options) error {
	if err := runReadiness(ctx, o, "summary"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not print readiness summary: %v\n", err)
	}
	args := []string{"compare-baseline"}
	if o.EnforceSLO {
		args = append(args, "--enforce-slo")
	}
	if err := runReadiness(ctx, o, args...); err != nil {
		if o.EnforceSLO {
			return fmt.Errorf("readiness SLO comparison failed: %w", err)
		}
	}
	return nil
}
