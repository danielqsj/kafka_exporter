package main

import (
	"regexp"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

// A consumer group task that is still running when collect stops waiting for
// it (group.metrics-timeout) must not send on the collect channel after
// collect returns: the caller closes that channel then, and the send panics.
func TestCollectGroupMetricsTimeoutDoesNotSendAfterReturn(t *testing.T) {
	initTopicMetricDescs()
	initGroupMetricDescs()

	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)

	metadata := sarama.NewMockMetadataResponse(t).
		SetBroker(mock.Addr(), mock.BrokerID()).
		SetLeader("kept", 0, mock.BrokerID())
	mock.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": metadata})

	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	offsets := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	offsets.AddTopicPartition("kept", 0, 10)
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":       metadata,
		"OffsetRequest":         sarama.NewMockWrapper(offsets),
		"ListGroupsRequest":     sarama.NewMockListGroupsResponse(t).AddGroup("g", "consumer"),
		"DescribeGroupsRequest": sarama.NewMockDescribeGroupsResponse(t).AddGroupDescription("g", &sarama.GroupDescription{GroupId: "g", State: "Empty"}),
		"OffsetFetchRequest":    sarama.NewMockOffsetFetchResponse(t).SetOffset("g", "kept", 0, 5, "", sarama.ErrNoError),
	})
	// Every response is slow, so the group task is still waiting for its offsets
	// when the (tiny) group metrics timeout expires.
	const latency = 50 * time.Millisecond
	mock.SetLatency(latency)

	e := &Exporter{
		client:              client,
		topicFilter:         regexp.MustCompile(".*"),
		topicExclude:        regexp.MustCompile("^$"),
		groupFilter:         regexp.MustCompile(".*"),
		groupExclude:        regexp.MustCompile("^$"),
		nextMetadataRefresh: time.Now().Add(time.Hour),
		offsetShowAll:       true,
		topicWorkers:        1,
		groupWorkers:        1,
		groupMetricsTimeout: time.Nanosecond,
	}
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	collected := len(ch)

	// Give the abandoned group task time to get its offsets. Anything it sends
	// now would go to a channel the registry has already closed.
	time.Sleep(10 * latency)

	if late := len(ch) - collected; late > 0 {
		t.Fatalf("%d metrics were sent after collect returned", late)
	}
}
