package main

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"testing"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestNormalizeTopicConfigs(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{name: "unset flag", in: []string{""}, want: nil},
		{name: "nothing", in: nil, want: nil},
		{name: "repeated flag", in: []string{"retention.ms", "retention.bytes"}, want: []string{"retention.bytes", "retention.ms"}},
		{name: "comma separated with blanks", in: []string{" retention.ms, ,cleanup.policy "}, want: []string{"cleanup.policy", "retention.ms"}},
		{name: "duplicates", in: []string{"retention.ms,retention.ms", "retention.ms"}, want: []string{"retention.ms"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeTopicConfigs(tt.in); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %#v, want %#v", got, tt.want)
			}
		})
	}
}

func initTopicConfigDescs() {
	topicConfig = prometheus.NewDesc(prometheus.BuildFQName(namespace, "topic", "config"), "config",
		[]string{"topic", "config"}, nil)
	topicConfigInfo = prometheus.NewDesc(prometheus.BuildFQName(namespace, "topic", "config_info"), "config_info",
		[]string{"topic", "config", "value"}, nil)
}

// sample is a collected metric reduced to what the tests compare.
type sample struct {
	desc   *prometheus.Desc
	labels string
	value  float64
}

func collectSamples(t *testing.T, e *Exporter, topics []string) []sample {
	t.Helper()
	ch := make(chan prometheus.Metric, 1000)
	e.collectTopicConfigs(ch, topics)
	close(ch)

	var samples []sample
	for m := range ch {
		var pb dto.Metric
		if err := m.Write(&pb); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		labels := ""
		for _, lp := range pb.Label {
			labels += fmt.Sprintf("%s=%q,", lp.GetName(), lp.GetValue())
		}
		samples = append(samples, sample{desc: m.Desc(), labels: labels, value: pb.GetGauge().GetValue()})
	}
	return samples
}

// fakeAdmin answers DescribeConfigs from respond and records the requests.
type fakeAdmin struct {
	respond  func(resources []*sarama.ConfigResource) ([]*sarama.ConfigResourceResult, error)
	requests [][]*sarama.ConfigResource
}

func (f *fakeAdmin) DescribeConfigs(resources []*sarama.ConfigResource, _ sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error) {
	f.requests = append(f.requests, resources)
	return f.respond(resources)
}

func newConfigExporter(configs []string, admin *fakeAdmin) *Exporter {
	initTopicConfigDescs()
	return &Exporter{
		admin:        admin,
		topicFilter:  regexp.MustCompile(".*"),
		topicExclude: regexp.MustCompile("^$"),
		topicConfigs: configs,
	}
}

func entries(pairs ...string) []sarama.ConfigEntry {
	var out []sarama.ConfigEntry
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, sarama.ConfigEntry{Name: pairs[i], Value: pairs[i+1]})
	}
	return out
}

func TestCollectTopicConfigs(t *testing.T) {
	admin := &fakeAdmin{respond: func(resources []*sarama.ConfigResource) ([]*sarama.ConfigResourceResult, error) {
		secret := entries("retention.ms", "1")
		secret[0].Sensitive = true
		return []*sarama.ConfigResourceResult{
			{Type: sarama.TopicResource, Name: "broken", ErrorCode: sarama.ErrUnknownTopicOrPartition},
			{Type: sarama.TopicResource, Name: "secret", Configs: secret},
			{Type: sarama.TopicResource, Name: "events", Configs: entries(
				"retention.ms", "604800000",
				"retention.bytes", "-1",
				"cleanup.policy", "compact,delete",
				// not requested, must not be exported even if the broker returns it
				"segment.bytes", "1073741824",
				// no value: skipped
				"retention.empty", "",
			)},
		}, nil
	}}
	e := newConfigExporter([]string{"cleanup.policy", "retention.bytes", "retention.ms"}, admin)

	got := collectSamples(t, e, []string{"events", "broken", "secret"})

	want := []sample{
		{topicConfig, `config="retention.ms",topic="events",`, 604800000},
		{topicConfig, `config="retention.bytes",topic="events",`, -1},
		{topicConfigInfo, `config="cleanup.policy",topic="events",value="compact,delete",`, 1},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d metrics %+v, want %d", len(got), got, len(want))
	}
	for _, w := range want {
		if !slices.ContainsFunc(got, func(g sample) bool { return g == w }) {
			t.Errorf("missing %+v in %+v", w, got)
		}
	}
}

func TestCollectTopicConfigsOnlyRequestsFilteredTopicsInBatches(t *testing.T) {
	admin := &fakeAdmin{respond: func(resources []*sarama.ConfigResource) ([]*sarama.ConfigResourceResult, error) {
		var results []*sarama.ConfigResourceResult
		for _, r := range resources {
			results = append(results, &sarama.ConfigResourceResult{Type: r.Type, Name: r.Name, Configs: entries("retention.ms", "1000")})
		}
		return results, nil
	}}
	e := newConfigExporter([]string{"retention.ms"}, admin)
	e.topicFilter = regexp.MustCompile("^keep-")
	e.topicExclude = regexp.MustCompile("-skip$")

	var topics []string
	for i := range 250 {
		topics = append(topics, fmt.Sprintf("keep-%03d", i))
	}
	topics = append(topics, "other", "keep-skip")

	if got := collectSamples(t, e, topics); len(got) != 250 {
		t.Errorf("got %d metrics, want 250", len(got))
	}

	var described int
	for _, req := range admin.requests {
		described += len(req)
		if len(req) > topicConfigBatchSize {
			t.Errorf("request describes %d topics, want at most %d", len(req), topicConfigBatchSize)
		}
		for _, r := range req {
			if r.Type != sarama.TopicResource || !slices.Equal(r.ConfigNames, []string{"retention.ms"}) {
				t.Errorf("unexpected resource %+v, want a topic limited to retention.ms", r)
			}
		}
	}
	if len(admin.requests) != 3 || described != 250 {
		t.Errorf("got %d requests describing %d topics, want 3 requests describing 250", len(admin.requests), described)
	}
}

// A failing batch must not stop the other batches.
func TestCollectTopicConfigsSurvivesRequestError(t *testing.T) {
	calls := 0
	admin := &fakeAdmin{respond: func(resources []*sarama.ConfigResource) ([]*sarama.ConfigResourceResult, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("broker unavailable")
		}
		return []*sarama.ConfigResourceResult{{Type: sarama.TopicResource, Name: resources[0].Name, Configs: entries("retention.ms", "5")}}, nil
	}}
	e := newConfigExporter([]string{"retention.ms"}, admin)

	var topics []string
	for i := range topicConfigBatchSize + 1 {
		topics = append(topics, fmt.Sprintf("t%03d", i))
	}
	got := collectSamples(t, e, topics)
	if len(got) != 1 || calls != 2 {
		t.Errorf("got %d metrics after %d calls, want 1 metric from the second of 2 calls", len(got), calls)
	}
}
