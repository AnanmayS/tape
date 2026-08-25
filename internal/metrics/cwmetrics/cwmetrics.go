// Package cwmetrics publishes a metrics.Snapshot to CloudWatch.
//
// It is a translator and stays one. Everything decided — what is measured, how
// it is aggregated, what happens when a publish fails — is decided in the
// metrics package, which has no SDK in it and is tested without one. What is
// left here is turning a []metrics.Datum into a PutMetricDataInput, and that is
// all this file does.
//
// The split mirrors storage and s3store for the same reason: a package that
// needs a credential to test is a package that stops being tested.
package cwmetrics

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/AnanmayS/tape/internal/metrics"
)

// API is the one CloudWatch call this project makes. It is an interface so the
// tests can watch what gets sent without an account, a credential or a network.
type API interface {
	PutMetricData(ctx context.Context, in *cloudwatch.PutMetricDataInput, opts ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error)
}

// Sink publishes into one CloudWatch namespace under one Product dimension.
type Sink struct {
	api       API
	namespace string
	dims      []types.Dimension
}

var _ metrics.Sink = (*Sink)(nil)

// New builds a Sink from the ambient AWS configuration: environment, profile,
// or the task role an ECS Fargate task runs under. Nothing in this project ever
// takes a credential as an argument.
func New(ctx context.Context, namespace, product string) (*Sink, error) {
	if namespace == "" {
		return nil, errors.New("cwmetrics: namespace must not be empty")
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("cwmetrics: load aws config: %w", err)
	}
	return NewWithAPI(cloudwatch.NewFromConfig(cfg), namespace, product)
}

// NewWithAPI builds a Sink over an existing client, or a fake.
func NewWithAPI(api API, namespace, product string) (*Sink, error) {
	if api == nil {
		return nil, errors.New("cwmetrics: api must not be nil")
	}
	if namespace == "" {
		return nil, errors.New("cwmetrics: namespace must not be empty")
	}
	s := &Sink{api: api, namespace: namespace}
	if product != "" {
		s.dims = []types.Dimension{{
			Name:  aws.String(metrics.DimensionProduct),
			Value: aws.String(product),
		}}
	}
	return s, nil
}

func (s *Sink) String() string { return "cloudwatch:" + s.namespace }

// Publish sends one interval in one call.
//
// One call is not a batching strategy, it is a fact about the input:
// metrics.Snapshot.Data returns at most five datapoints, and PutMetricData
// accepts a thousand. If that ever stops being true the request will be
// rejected loudly rather than silently truncated, which is the right way round.
func (s *Sink) Publish(ctx context.Context, data []metrics.Datum) error {
	if len(data) == 0 {
		return nil
	}

	md := make([]types.MetricDatum, 0, len(data))
	for _, d := range data {
		datum := types.MetricDatum{
			MetricName: aws.String(d.Name),
			Dimensions: s.dims,
			Unit:       unitFor(d.Unit),
		}
		if !d.Time.IsZero() {
			datum.Timestamp = aws.Time(d.Time)
		}

		// A datum carries a value or a statistic set, never both; CloudWatch
		// rejects one that carries two. A statistic set with no samples is
		// rejected as well, and would be meaningless anyway.
		switch {
		case d.Statistics != nil && d.Statistics.Count > 0:
			datum.StatisticValues = &types.StatisticSet{
				SampleCount: aws.Float64(float64(d.Statistics.Count)),
				Sum:         aws.Float64(d.Statistics.Sum),
				Minimum:     aws.Float64(d.Statistics.Min),
				Maximum:     aws.Float64(d.Statistics.Max),
			}
		case d.Statistics != nil:
			continue
		default:
			datum.Value = aws.Float64(d.Value)
		}
		md = append(md, datum)
	}
	if len(md) == 0 {
		return nil
	}

	_, err := s.api.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(s.namespace),
		MetricData: md,
	})
	if err != nil {
		return fmt.Errorf("cwmetrics: put metric data: %w", err)
	}
	return nil
}

// unitFor maps this project's three units onto CloudWatch's enum. An unknown
// unit becomes None rather than an error: a datapoint with the wrong label on
// its axis is worth more than no datapoint.
func unitFor(u metrics.Unit) types.StandardUnit {
	switch u {
	case metrics.UnitCount:
		return types.StandardUnitCount
	case metrics.UnitCountPerSecond:
		return types.StandardUnitCountSecond
	case metrics.UnitSeconds:
		return types.StandardUnitSeconds
	default:
		return types.StandardUnitNone
	}
}
