package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/AnanmayS/tape/internal/metrics"
	"github.com/AnanmayS/tape/internal/metrics/cwmetrics"
)

// metricsFlags turn CloudWatch publishing on. Off is the default and the whole
// point of the default: capture must work on a laptop with no AWS account, and
// a flag that has to be remembered to avoid a network call is a flag that will
// be forgotten.
//
// Both flags read an environment variable for their default, because the ECS
// task definition can pass either and there is no reason to make it pass one
// particular one. The namespace is the switch: empty publishes nothing.
type metricsFlags struct {
	namespace string
	interval  time.Duration
}

// Environment variables the flags fall back to.
const (
	envMetricsNamespace = "TAPE_METRICS_NAMESPACE"
	envMetricsInterval  = "TAPE_METRICS_INTERVAL"
)

func (m *metricsFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&m.namespace, "metrics-namespace", os.Getenv(envMetricsNamespace),
		"CloudWatch namespace to publish to; empty publishes nothing (env "+envMetricsNamespace+")")
	fs.DurationVar(&m.interval, "metrics-interval", envDuration(envMetricsInterval, time.Minute),
		"how often metrics are aggregated and published (env "+envMetricsInterval+")")
}

// label describes the configuration for the startup log line.
func (m *metricsFlags) label() string {
	if m.namespace == "" {
		return "off"
	}
	return fmt.Sprintf("cloudwatch:%s every %s", m.namespace, m.interval)
}

// recorder builds the Recorder capture should use and the function that closes
// it. The close function always exists, so the caller never has to ask whether
// metrics were on; with metrics off it returns no attributes and does nothing.
func (m *metricsFlags) recorder(ctx context.Context, product string, log *slog.Logger) (metrics.Recorder, func() []any, error) {
	if m.namespace == "" {
		return metrics.Nop{}, func() []any { return nil }, nil
	}
	if m.interval <= 0 {
		return nil, nil, fmt.Errorf("tape: -metrics-interval must be positive, got %s", m.interval)
	}

	sink, err := cwmetrics.New(ctx, m.namespace, product)
	if err != nil {
		return nil, nil, err
	}
	pub := metrics.NewPublisher(sink, metrics.PublisherConfig{
		Interval: m.interval,
		Log:      log,
	})
	return pub, func() []any {
		pub.Close()
		return pub.Stats().LogAttrs()
	}, nil
}

// envDuration reads a duration from the environment, falling back to def. A
// value that will not parse is a configuration mistake worth failing on, but
// this is a flag default and there is nowhere to report it from, so it falls
// back and the flag's own value is what gets logged at startup.
func envDuration(key string, def time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return def
	}
	return d
}
