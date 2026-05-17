package stacks

import (
	"context"
	"testing"
)

func TestInitStacksTelemetryNoopsWithoutEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "")

	telemetry := initStacksTelemetry(context.Background(), defaultOptions(), "status")
	if telemetry.enabled {
		t.Fatalf("telemetry enabled without an OTLP endpoint")
	}
	telemetry.shutdown(context.Background())
}

func TestInitStacksTelemetryHonorsSDKDisabled(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	t.Setenv("OTEL_SDK_DISABLED", "true")

	telemetry := initStacksTelemetry(context.Background(), defaultOptions(), "status")
	if telemetry.enabled {
		t.Fatalf("telemetry enabled while OTEL_SDK_DISABLED=true")
	}
	telemetry.shutdown(context.Background())
}
