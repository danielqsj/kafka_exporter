package main

import (
	"bytes"
	"flag"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"
)

// The broker list used for the consumer group fetch is debug output. At the
// default verbosity a scrape must not log it, or every scrape adds a line (#498).
func TestCollectDoesNotLogBrokersAtDefaultVerbosity(t *testing.T) {
	var buf bytes.Buffer
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	for name, value := range map[string]string{"logtostderr": "false", "alsologtostderr": "false", "stderrthreshold": "FATAL", "v": "0"} {
		if err := fs.Set(name, value); err != nil {
			t.Fatalf("set klog flag %s: %v", name, err)
		}
	}
	klog.SetOutput(&buf)
	t.Cleanup(func() {
		klog.Flush()
		_ = fs.Set("logtostderr", "true")
		_ = fs.Set("stderrthreshold", "ERROR")
		klog.SetOutput(nil)
	})

	initTopicMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(mock.Addr(), mock.BrokerID()),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})

	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	e := &Exporter{
		client:              client,
		topicFilter:         regexp.MustCompile(".*"),
		topicExclude:        regexp.MustCompile("^$"),
		groupFilter:         regexp.MustCompile(".*"),
		groupExclude:        regexp.MustCompile("^$"),
		nextMetadataRefresh: time.Now().Add(time.Hour),
		topicWorkers:        1,
		groupWorkers:        1,
		groupMetricsTimeout: time.Second,
	}
	ch := make(chan prometheus.Metric, 100)
	e.collect(ch)
	close(ch)
	for range ch {
	}
	klog.Flush()

	if strings.Contains(buf.String(), mock.Addr()) {
		t.Errorf("scrape logged the broker list at the default verbosity:\n%s", buf.String())
	}
}
