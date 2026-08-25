package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/AnanmayS/tape/internal/metrics"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// Off is the default, and "off" has to mean no AWS call of any kind — not even
// the credential lookup that building a client would do. This test runs with no
// credentials in the environment and must not care.
func TestMetricsOffByDefault(t *testing.T) {
	fs := flag.NewFlagSet("capture", flag.ContinueOnError)
	var m metricsFlags
	t.Setenv(envMetricsNamespace, "")
	t.Setenv(envMetricsInterval, "")
	m.register(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}

	if m.namespace != "" {
		t.Errorf("default namespace = %q, want empty", m.namespace)
	}
	if m.label() != "off" {
		t.Errorf("label = %q, want off", m.label())
	}

	rec, closeFn, err := m.recorder(context.Background(), "BTC-USD", quietLogger())
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}
	if _, ok := rec.(metrics.Nop); !ok {
		t.Errorf("recorder = %T, want metrics.Nop", rec)
	}
	if attrs := closeFn(); attrs != nil {
		t.Errorf("close returned %v, want no attributes when metrics are off", attrs)
	}
}

func TestMetricsFlagsReadTheEnvironment(t *testing.T) {
	t.Setenv(envMetricsNamespace, "TapeFromEnv")
	t.Setenv(envMetricsInterval, "30s")

	fs := flag.NewFlagSet("capture", flag.ContinueOnError)
	var m metricsFlags
	m.register(fs)
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	if m.namespace != "TapeFromEnv" || m.interval != 30*time.Second {
		t.Fatalf("from env: namespace %q interval %s", m.namespace, m.interval)
	}

	// An explicit flag beats the environment, which is what lets a task
	// definition set a default that an operator can still override.
	fs = flag.NewFlagSet("capture", flag.ContinueOnError)
	m = metricsFlags{}
	m.register(fs)
	if err := fs.Parse([]string{"-metrics-namespace", "TapeFromFlag"}); err != nil {
		t.Fatal(err)
	}
	if m.namespace != "TapeFromFlag" {
		t.Errorf("namespace = %q, want the flag to win", m.namespace)
	}
	if got := m.label(); got != "cloudwatch:TapeFromFlag every 30s" {
		t.Errorf("label = %q", got)
	}
}

func TestEnvDurationFallsBack(t *testing.T) {
	const key = "TAPE_TEST_DURATION"

	t.Setenv(key, "")
	if got := envDuration(key, time.Minute); got != time.Minute {
		t.Errorf("empty = %s, want 1m", got)
	}
	t.Setenv(key, "not a duration")
	if got := envDuration(key, time.Minute); got != time.Minute {
		t.Errorf("garbage = %s, want 1m", got)
	}
	t.Setenv(key, "-5s")
	if got := envDuration(key, time.Minute); got != time.Minute {
		t.Errorf("negative = %s, want 1m", got)
	}
	t.Setenv(key, "15s")
	if got := envDuration(key, time.Minute); got != 15*time.Second {
		t.Errorf("valid = %s, want 15s", got)
	}
}
