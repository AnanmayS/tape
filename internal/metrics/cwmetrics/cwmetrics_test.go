package cwmetrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"

	"github.com/AnanmayS/tape/internal/metrics"
)

// fakeAPI stands in for CloudWatch. What is substituted is the far end of the
// call; the input this package builds is the real one, and it is the thing
// worth asserting on — a datum with the wrong dimension is a datum no alarm
// will ever see.
type fakeAPI struct {
	inputs []*cloudwatch.PutMetricDataInput
	err    error
}

func (f *fakeAPI) PutMetricData(_ context.Context, in *cloudwatch.PutMetricDataInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error) {
	f.inputs = append(f.inputs, in)
	if f.err != nil {
		return nil, f.err
	}
	return &cloudwatch.PutMetricDataOutput{}, nil
}

func newSink(t *testing.T, api API) *Sink {
	t.Helper()
	s, err := NewWithAPI(api, "TapeTest", "BTC-USD")
	if err != nil {
		t.Fatalf("NewWithAPI: %v", err)
	}
	return s
}

func TestPublishBuildsOneRequest(t *testing.T) {
	api := &fakeAPI{}
	sink := newSink(t, api)

	at := time.Date(2026, 8, 25, 14, 1, 0, 0, time.UTC)
	err := sink.Publish(context.Background(), []metrics.Datum{
		{Name: metrics.MetricMessages, Unit: metrics.UnitCount, Time: at, Value: 1860},
		{Name: metrics.MetricMessageRate, Unit: metrics.UnitCountPerSecond, Time: at, Value: 31},
		{Name: metrics.MetricGaps, Unit: metrics.UnitCount, Time: at, Value: 0},
		{Name: metrics.MetricIngestLag, Unit: metrics.UnitSeconds, Time: at,
			Statistics: &metrics.Stat{Count: 1860, Sum: 93, Min: 0.01, Max: 1.5}},
		{Name: metrics.MetricQueueDepth, Unit: metrics.UnitCount, Time: at, Value: 32},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// One interval is one call. Aggregation is the whole point: the alternative
	// is a network round trip per message.
	if len(api.inputs) != 1 {
		t.Fatalf("made %d PutMetricData calls, want 1", len(api.inputs))
	}
	in := api.inputs[0]
	if got := deref(in.Namespace); got != "TapeTest" {
		t.Errorf("namespace = %q, want TapeTest", got)
	}
	if len(in.MetricData) != 5 {
		t.Fatalf("sent %d datapoints, want 5", len(in.MetricData))
	}

	byName := map[string]types.MetricDatum{}
	for _, d := range in.MetricData {
		name := deref(d.MetricName)
		byName[name] = d

		// The alarms in terraform/alarms.tf match on exactly this dimension.
		// One that went out unlabelled would land on a different metric than
		// the one the alarm watches, and the alarm would sit at OK forever.
		if len(d.Dimensions) != 1 ||
			deref(d.Dimensions[0].Name) != metrics.DimensionProduct ||
			deref(d.Dimensions[0].Value) != "BTC-USD" {
			t.Errorf("%s dimensions = %+v, want Product=BTC-USD", name, d.Dimensions)
		}
		if d.Timestamp == nil || !d.Timestamp.Equal(at) {
			t.Errorf("%s timestamp = %v, want %v", name, d.Timestamp, at)
		}
	}

	if got := byName[metrics.MetricMessages]; got.Unit != types.StandardUnitCount || *got.Value != 1860 {
		t.Errorf("MessagesReceived = %v %v", got.Unit, got.Value)
	}
	if got := byName[metrics.MetricMessageRate]; got.Unit != types.StandardUnitCountSecond || *got.Value != 31 {
		t.Errorf("MessageRate = %v %v", got.Unit, got.Value)
	}
	if got := byName[metrics.MetricGaps]; got.Value == nil || *got.Value != 0 {
		t.Errorf("GapsDetected = %v, want an explicit 0", got.Value)
	}

	// The lag goes as a statistic set, so the maximum survives the trip. An
	// average of averages would bury the one message that arrived late, and
	// that message is the entire reason the metric exists.
	lag := byName[metrics.MetricIngestLag]
	if lag.Unit != types.StandardUnitSeconds {
		t.Errorf("IngestLag unit = %v, want Seconds", lag.Unit)
	}
	if lag.Value != nil {
		t.Error("IngestLag carries both a value and a statistic set; CloudWatch rejects that")
	}
	if lag.StatisticValues == nil {
		t.Fatal("IngestLag carries no statistic set")
	}
	if *lag.StatisticValues.SampleCount != 1860 ||
		*lag.StatisticValues.Sum != 93 ||
		*lag.StatisticValues.Minimum != 0.01 ||
		*lag.StatisticValues.Maximum != 1.5 {
		t.Errorf("IngestLag statistics = %+v", *lag.StatisticValues)
	}
}

func TestPublishDropsEmptyStatisticSet(t *testing.T) {
	api := &fakeAPI{}
	sink := newSink(t, api)

	err := sink.Publish(context.Background(), []metrics.Datum{
		{Name: metrics.MetricMessages, Unit: metrics.UnitCount, Value: 0},
		{Name: metrics.MetricIngestLag, Unit: metrics.UnitSeconds, Statistics: &metrics.Stat{}},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(api.inputs) != 1 {
		t.Fatalf("made %d calls, want 1", len(api.inputs))
	}
	if n := len(api.inputs[0].MetricData); n != 1 {
		t.Fatalf("sent %d datapoints, want 1: a statistic set with no samples is rejected by CloudWatch", n)
	}
	if got := deref(api.inputs[0].MetricData[0].MetricName); got != metrics.MetricMessages {
		t.Errorf("kept %q, want %q", got, metrics.MetricMessages)
	}
}

func TestPublishNothingMakesNoCall(t *testing.T) {
	api := &fakeAPI{}
	sink := newSink(t, api)

	if err := sink.Publish(context.Background(), nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(api.inputs) != 0 {
		t.Errorf("made %d calls for an empty publish", len(api.inputs))
	}
}

func TestPublishWrapsTheError(t *testing.T) {
	sentinel := errors.New("throttled")
	api := &fakeAPI{err: sentinel}
	sink := newSink(t, api)

	err := sink.Publish(context.Background(), []metrics.Datum{
		{Name: metrics.MetricMessages, Unit: metrics.UnitCount, Value: 1},
	})
	if !errors.Is(err, sentinel) {
		t.Errorf("Publish error = %v, want it to wrap %v", err, sentinel)
	}
}

func TestUnitMapping(t *testing.T) {
	cases := map[metrics.Unit]types.StandardUnit{
		metrics.UnitCount:          types.StandardUnitCount,
		metrics.UnitCountPerSecond: types.StandardUnitCountSecond,
		metrics.UnitSeconds:        types.StandardUnitSeconds,
		metrics.Unit("nonsense"):   types.StandardUnitNone,
	}
	for in, want := range cases {
		if got := unitFor(in); got != want {
			t.Errorf("unitFor(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestConstructorRejectsNonsense(t *testing.T) {
	if _, err := NewWithAPI(nil, "Tape", "BTC-USD"); err == nil {
		t.Error("NewWithAPI accepted a nil API")
	}
	if _, err := NewWithAPI(&fakeAPI{}, "", "BTC-USD"); err == nil {
		t.Error("NewWithAPI accepted an empty namespace")
	}
}

// A sink with no product publishes undimensioned metrics rather than a
// dimension with an empty value, which CloudWatch rejects outright.
func TestNoProductMeansNoDimension(t *testing.T) {
	api := &fakeAPI{}
	sink, err := NewWithAPI(api, "Tape", "")
	if err != nil {
		t.Fatalf("NewWithAPI: %v", err)
	}
	if err := sink.Publish(context.Background(), []metrics.Datum{
		{Name: metrics.MetricMessages, Unit: metrics.UnitCount, Value: 1},
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := api.inputs[0].MetricData[0].Dimensions; len(got) != 0 {
		t.Errorf("dimensions = %+v, want none", got)
	}
}

func TestSinkString(t *testing.T) {
	if got := newSink(t, &fakeAPI{}).String(); got != "cloudwatch:TapeTest" {
		t.Errorf("String() = %q", got)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
