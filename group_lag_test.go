package main

import (
	"slices"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

// A group's offsets for a topic deleted after the topic walk must not cost a
// metadata refresh (with retries) for every partition of that topic. A failing
// partition of a topic that still exists must not hide the healthy ones.
func TestGroupLagStopsOnDeletedTopic(t *testing.T) {
	const partitions = 20
	tests := []struct {
		name string
		// leader of the target topic's partitions in the cached metadata
		cachedLeader int32
		// error the broker returns for the offsets of the first `failing` partitions
		offsetErr sarama.KError
		failing   int32
		// whether the target topic is still in the cluster's metadata
		exists bool
	}{
		// The broker answers the offset request with UNKNOWN_TOPIC_OR_PARTITION.
		{name: "offset lookup fails", cachedLeader: 0, offsetErr: sarama.ErrUnknownTopicOrPartition, failing: partitions},
		// The cached leader is unknown, so the leader lookup refreshes metadata.
		{name: "leader lookup fails", cachedLeader: 99, offsetErr: sarama.ErrUnknownTopicOrPartition, failing: partitions},
		// One partition is unknown but the refresh still lists the topic.
		{name: "unknown partition of a live topic", cachedLeader: 0, offsetErr: sarama.ErrUnknownTopicOrPartition, failing: 1, exists: true},
		// A different partition-level error on a topic that still exists.
		{name: "other error keeps going", cachedLeader: 0, offsetErr: sarama.ErrNotLeaderForPartition, failing: partitions, exists: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testGroupLagWithFailingTopic(t, partitions, tt.cachedLeader, tt.offsetErr, tt.failing, tt.exists)
		})
	}
}

func testGroupLagWithFailingTopic(t *testing.T, partitions, cachedLeader int32, offsetErr sarama.KError, failing int32, exists bool) {
	initGroupMetricDescs()

	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)

	metadata := func(withTarget bool, leader int32) *sarama.MockMetadataResponse {
		m := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID())
		for p := int32(0); withTarget && p < partitions; p++ {
			m.SetLeader("target", p, leader)
		}
		return m
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": metadata(true, cachedLeader)})

	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = time.Millisecond
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	offsets := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	offsets.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"target": {}}
	groupOffsets := sarama.NewMockOffsetFetchResponse(t)
	// The topic walk listed every partition before the topic was deleted.
	offsetMap := map[string]map[int32]int64{"target": {}}
	for p := int32(0); p < partitions; p++ {
		if p < failing {
			offsets.Blocks["target"][p] = &sarama.OffsetResponseBlock{Err: offsetErr}
		} else {
			offsets.AddTopicPartition("target", p, 10)
		}
		groupOffsets.SetOffset("g", "target", p, 5, "", sarama.ErrNoError)
		offsetMap["target"][p] = 10
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":    metadata(exists, mock.BrokerID()),
		"OffsetRequest":      sarama.NewMockWrapper(offsets),
		"OffsetFetchRequest": groupOffsets,
	})

	e := &Exporter{client: client, offsetShowAll: true}
	ch := make(chan prometheus.Metric, 1000)
	e.emitGroupMetrics(consumerGroupInfo{id: "g"}, sarama.NewBroker(mock.Addr()), offsetMap, ch)
	close(ch)

	var groupOffsetMetrics, lagMetrics, lagSums int32
	for m := range ch {
		switch m.Desc() {
		case consumergroupCurrentOffset:
			groupOffsetMetrics++
		case consumergroupLag:
			lagMetrics++
		case consumergroupLagSum:
			lagSums++
		}
	}
	if groupOffsetMetrics == 0 {
		t.Fatal("no consumer group offsets emitted: the lag loop did not run")
	}

	if exists {
		if want := partitions - failing; lagMetrics != want {
			t.Errorf("got %d lag metrics, want %d: lag loop stopped on an error that is not a deleted topic", lagMetrics, want)
		}
		if lagSums != 1 {
			t.Errorf("got %d lag sum metrics, want 1", lagSums)
		}
		return
	}
	if lagMetrics != 0 || lagSums != 0 {
		t.Errorf("got %d lag and %d lag sum metrics for a deleted topic, want none", lagMetrics, lagSums)
	}
	var targetLookups int
	for _, rr := range mock.History() {
		if req, ok := rr.Request.(*sarama.MetadataRequest); ok && slices.Contains(req.Topics, "target") {
			targetLookups++
		}
	}
	// The first partition only: GetOffset's leader lookup refreshes metadata, then
	// GetOffset refreshes once more itself, each with its retries.
	if limit := 2 * (config.Metadata.Retry.Max + 1); targetLookups > limit {
		t.Errorf("deleted topic cost %d metadata requests, want at most %d", targetLookups, limit)
	}
}

func initGroupMetricDescs() {
	desc := func(name string, variableLabels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(namespace, "", name), name, variableLabels, nil)
	}
	consumergroupCurrentOffset = desc("consumergroup_current_offset", "consumergroup", "topic", "partition")
	consumergroupCurrentOffsetSum = desc("consumergroup_current_offset_sum", "consumergroup", "topic")
	consumergroupLag = desc("consumergroup_lag", "consumergroup", "topic", "partition")
	consumergroupLagSum = desc("consumergroup_lag_sum", "consumergroup", "topic")
	consumergroupMembers = desc("consumergroup_members", "consumergroup")
}
