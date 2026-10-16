package main

import (
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

// A topic deleted after the exporter cached its partition list must not cost a
// metadata refresh (with retries) for every remaining partition. A failing
// partition of a topic that still exists must not hide the healthy ones.
func TestCollectStopsWalkingDeletedTopic(t *testing.T) {
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
		{name: "other error keeps walking", cachedLeader: 0, offsetErr: sarama.ErrNotLeaderForPartition, failing: partitions, exists: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCollectWithFailingTopic(t, partitions, tt.cachedLeader, tt.offsetErr, tt.failing, tt.exists)
		})
	}
}

func testCollectWithFailingTopic(t *testing.T, partitions, cachedLeader int32, offsetErr sarama.KError, failing int32, exists bool) {
	initTopicMetricDescs()

	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)

	metadata := func(withTarget bool, leader int32) *sarama.MockMetadataResponse {
		m := sarama.NewMockMetadataResponse(t).
			SetBroker(mock.Addr(), mock.BrokerID()).
			SetLeader("kept", 0, mock.BrokerID())
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
	offsets.AddTopicPartition("kept", 0, 10)
	offsets.Blocks["target"] = map[int32]*sarama.OffsetResponseBlock{}
	for p := int32(0); p < partitions; p++ {
		if p < failing {
			offsets.Blocks["target"][p] = &sarama.OffsetResponseBlock{Err: offsetErr}
		} else {
			offsets.AddTopicPartition("target", p, 10)
		}
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   metadata(exists, mock.BrokerID()),
		"OffsetRequest":     sarama.NewMockWrapper(offsets),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})

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
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	close(ch)

	var offsetMetrics, leaderMetrics int32
	for m := range ch {
		switch m.Desc() {
		case topicCurrentOffset:
			offsetMetrics++
		case topicPartitionLeader:
			leaderMetrics++
		}
	}
	// "kept" plus every healthy partition of "target"
	if want := partitions - failing + 1; offsetMetrics != want {
		t.Errorf("got %d current offset metrics, want %d", offsetMetrics, want)
	}

	if exists {
		if want := partitions + 1; leaderMetrics != want {
			t.Errorf("got %d leader metrics, want %d: walk stopped on an error that is not a deleted topic", leaderMetrics, want)
		}
		return
	}
	var targetLookups int
	for _, rr := range mock.History() {
		if req, ok := rr.Request.(*sarama.MetadataRequest); ok && slices.Contains(req.Topics, "target") {
			targetLookups++
		}
	}
	// One refresh of the first partition, with its retries.
	if limit := config.Metadata.Retry.Max + 1; targetLookups > limit {
		t.Errorf("deleted topic cost %d metadata requests, want at most %d", targetLookups, limit)
	}
}

func initTopicMetricDescs() {
	desc := func(name string, variableLabels ...string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(namespace, "", name), name, variableLabels, nil)
	}
	clusterBrokers = desc("brokers")
	clusterBrokerInfo = desc("broker_info", "id", "address")
	topicPartitions = desc("topic_partitions", "topic")
	topicCurrentOffset = desc("topic_partition_current_offset", "topic", "partition")
	topicOldestOffset = desc("topic_partition_oldest_offset", "topic", "partition")
	topicPartitionLeader = desc("topic_partition_leader", "topic", "partition")
	topicPartitionReplicas = desc("topic_partition_replicas", "topic", "partition")
	topicPartitionInSyncReplicas = desc("topic_partition_in_sync_replica", "topic", "partition")
	topicPartitionUsesPreferredReplica = desc("topic_partition_leader_is_preferred", "topic", "partition")
	topicUnderReplicatedPartition = desc("topic_partition_under_replicated_partition", "topic", "partition")
}
