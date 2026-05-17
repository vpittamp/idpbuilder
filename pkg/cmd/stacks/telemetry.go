package stacks

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	cmdversion "github.com/cnoe-io/idpbuilder/pkg/cmd/version"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const stacksInstrumentationName = "github.com/cnoe-io/idpbuilder/pkg/cmd/stacks"

var stacksTelemetry = newNoopStacksTelemetry()

type stacksTelemetryContextKey struct{}

type stacksCommandContext struct {
	Provider    string
	ClusterName string
}

type stacksTelemetryState struct {
	enabled bool

	tracer trace.Tracer

	phaseDuration       metric.Float64Histogram
	commandDuration     metric.Float64Histogram
	commandFailures     metric.Int64Counter
	recreateCompletions metric.Int64Counter

	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
}

func newNoopStacksTelemetry() *stacksTelemetryState {
	meter := otel.GetMeterProvider().Meter(stacksInstrumentationName)
	phaseDuration, _ := meter.Float64Histogram("idpbuilder.stacks.phase.duration", metric.WithUnit("s"))
	commandDuration, _ := meter.Float64Histogram("idpbuilder.stacks.command.duration", metric.WithUnit("s"))
	commandFailures, _ := meter.Int64Counter("idpbuilder.stacks.command.failures")
	recreateCompletions, _ := meter.Int64Counter("idpbuilder.stacks.recreate.completed")
	return &stacksTelemetryState{
		tracer:              otel.GetTracerProvider().Tracer(stacksInstrumentationName),
		phaseDuration:       phaseDuration,
		commandDuration:     commandDuration,
		commandFailures:     commandFailures,
		recreateCompletions: recreateCompletions,
	}
}

func runStacksCommand(ctx context.Context, o *options, command string, fn func(context.Context) error) error {
	telemetry := initStacksTelemetry(ctx, o, command)
	stacksTelemetry = telemetry
	defer func() {
		telemetry.shutdown(context.Background())
		stacksTelemetry = newNoopStacksTelemetry()
	}()

	ctx = context.WithValue(ctx, stacksTelemetryContextKey{}, stacksCommandContext{
		Provider:    o.Provider,
		ClusterName: o.ClusterName,
	})
	ctx, span := telemetry.tracer.Start(ctx, "idpbuilder.stacks."+command, trace.WithAttributes(telemetry.commandAttrs(o, command)...))
	start := time.Now()
	err := fn(ctx)
	duration := time.Since(start).Seconds()
	successValue := successAttr(err)
	span.SetAttributes(attribute.Bool("success", err == nil), attribute.Float64("duration.seconds", duration), successValue)
	if command == "create" && o.Recreate {
		telemetry.recreateCompletions.Add(ctx, 1, metric.WithAttributes(telemetry.commandMetricAttrs(o, command, successValue)...))
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
	return err
}

func initStacksTelemetry(ctx context.Context, o *options, command string) *stacksTelemetryState {
	if !otelEndpointConfigured() || sdkDisabled() {
		return newNoopStacksTelemetry()
	}

	// Export failures should not alter CLI behavior or add noisy stderr output.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(error) {}))

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(resourceAttrs(o, command)...),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return newNoopStacksTelemetry()
	}

	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return newNoopStacksTelemetry()
	}
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return newNoopStacksTelemetry()
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(traceExporter, sdktrace.WithExportTimeout(2*time.Second)),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithTimeout(2*time.Second))),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)

	meter := meterProvider.Meter(stacksInstrumentationName)
	phaseDuration, _ := meter.Float64Histogram("idpbuilder.stacks.phase.duration", metric.WithUnit("s"))
	commandDuration, _ := meter.Float64Histogram("idpbuilder.stacks.command.duration", metric.WithUnit("s"))
	commandFailures, _ := meter.Int64Counter("idpbuilder.stacks.command.failures")
	recreateCompletions, _ := meter.Int64Counter("idpbuilder.stacks.recreate.completed")

	return &stacksTelemetryState{
		enabled:             true,
		tracer:              tracerProvider.Tracer(stacksInstrumentationName),
		phaseDuration:       phaseDuration,
		commandDuration:     commandDuration,
		commandFailures:     commandFailures,
		recreateCompletions: recreateCompletions,
		tracerProvider:      tracerProvider,
		meterProvider:       meterProvider,
	}
}

func (t *stacksTelemetryState) shutdown(ctx context.Context) {
	if t == nil || !t.enabled {
		return
	}
	if t.tracerProvider != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = t.tracerProvider.Shutdown(shutdownCtx)
		cancel()
	}
	if t.meterProvider != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = t.meterProvider.Shutdown(shutdownCtx)
		cancel()
	}
	otel.SetTracerProvider(tracenoop.NewTracerProvider())
	otel.SetMeterProvider(metricnoop.NewMeterProvider())
}

func (t *stacksTelemetryState) startPhase(ctx context.Context, o *options, phase string) (context.Context, trace.Span, time.Time) {
	attrs := []attribute.KeyValue{
		attribute.String("phase", phase),
		attribute.String("provider", o.Provider),
		attribute.String("cluster.name", o.ClusterName),
	}
	ctx, span := t.tracer.Start(ctx, "idpbuilder.stacks.phase."+phase, trace.WithAttributes(attrs...))
	return ctx, span, time.Now()
}

func (t *stacksTelemetryState) endPhase(ctx context.Context, o *options, phase string, start time.Time, span trace.Span, err error) {
	duration := time.Since(start).Seconds()
	successValue := successAttr(err)
	attrs := []attribute.KeyValue{
		attribute.String("phase", phase),
		attribute.String("provider", o.Provider),
		attribute.String("cluster.name", o.ClusterName),
		successValue,
	}
	t.phaseDuration.Record(ctx, duration, metric.WithAttributes(attrs...))
	span.SetAttributes(attribute.Float64("duration.seconds", duration), successValue, attribute.Bool("success", err == nil))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func (t *stacksTelemetryState) startCommand(ctx context.Context, dir, name string, args []string, mode string) (context.Context, trace.Span, time.Time) {
	redacted := redactArgs(args)
	commandName := commandMetricName(name)
	attrs := []attribute.KeyValue{
		attribute.String("command.name", commandName),
		attribute.String("command.args", strings.Join(redacted, " ")),
		attribute.String("command.mode", mode),
	}
	if commandName != name {
		attrs = append(attrs, attribute.String("command.path", name))
	}
	if dir != "" {
		attrs = append(attrs, attribute.String("command.dir", dir))
	}
	if commandCtx, ok := ctx.Value(stacksTelemetryContextKey{}).(stacksCommandContext); ok {
		attrs = append(attrs,
			attribute.String("provider", commandCtx.Provider),
			attribute.String("cluster.name", commandCtx.ClusterName),
		)
	}
	ctx, span := t.tracer.Start(ctx, "idpbuilder.stacks.command."+commandName, trace.WithAttributes(attrs...))
	return ctx, span, time.Now()
}

func (t *stacksTelemetryState) endCommand(ctx context.Context, name string, start time.Time, span trace.Span, err error) {
	duration := time.Since(start).Seconds()
	successValue := successAttr(err)
	exitCode := commandExitCode(err)
	attrs := []attribute.KeyValue{
		attribute.String("command.name", commandMetricName(name)),
		successValue,
	}
	if commandCtx, ok := ctx.Value(stacksTelemetryContextKey{}).(stacksCommandContext); ok {
		attrs = append(attrs,
			attribute.String("provider", commandCtx.Provider),
			attribute.String("cluster.name", commandCtx.ClusterName),
		)
	}
	t.commandDuration.Record(ctx, duration, metric.WithAttributes(attrs...))
	if err != nil {
		t.commandFailures.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	span.SetAttributes(
		attribute.Float64("duration.seconds", duration),
		attribute.Int("command.exit_code", exitCode),
		successValue,
		attribute.Bool("success", err == nil),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End()
}

func commandMetricName(name string) string {
	if name == "" {
		return ""
	}
	base := filepath.Base(name)
	if base == "." || base == string(filepath.Separator) {
		return name
	}
	return base
}

func (t *stacksTelemetryState) commandAttrs(o *options, command string) []attribute.KeyValue {
	return resourceAttrs(o, command)
}

func (t *stacksTelemetryState) commandMetricAttrs(o *options, command string, success attribute.KeyValue) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("command", command),
		attribute.String("provider", o.Provider),
		attribute.String("cluster.name", o.ClusterName),
		success,
	}
}

func resourceAttrs(o *options, command string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		semconv.ServiceName("idpbuilder"),
		semconv.ServiceVersion(cmdversion.IDPBuilderVersion()),
		attribute.String("command", "stacks "+command),
		attribute.String("provider", o.Provider),
		attribute.String("profile", o.Profile),
		attribute.String("cluster.name", o.ClusterName),
		attribute.String("overlay", o.Overlay),
		attribute.String("stacks.repo", o.StacksRepo),
		attribute.String("idpbuilder.version", cmdversion.IDPBuilderVersion()),
		attribute.String("idpbuilder.git_commit", cmdversion.GitCommit()),
	}
	return attrs
}

func successAttr(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("result", "failure")
	}
	return attribute.String("result", "success")
}

func otelEndpointConfigured() bool {
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
	} {
		if strings.TrimSpace(os.Getenv(key)) != "" {
			return true
		}
	}
	return false
}

func sdkDisabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")))
	return value == "true" || value == "1"
}

func commandExitCode(err error) int {
	if err == nil {
		return 0
	}
	type exitCoder interface {
		ExitCode() int
	}
	var exitErr exitCoder
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}
