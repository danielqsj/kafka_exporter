package main

import (
	"bytes"
	"errors"
	"flag"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"k8s.io/klog/v2"
)

const batchPartitions = 10

// Two brokers, each leading every other partition of topic "t". newest is
// 1000+p, oldest is 100+p; group "g" committed 500+p.
type twoBrokerCluster struct {
	brokers []*sarama.MockBroker
	client  sarama.Client
	config  *sarama.Config
}

func newTwoBrokerCluster(t *testing.T) *twoBrokerCluster {
	t.Helper()
	c := &twoBrokerCluster{
		brokers: []*sarama.MockBroker{sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2)},
	}
	metadata := sarama.NewMockMetadataResponse(t)
	for _, b := range c.brokers {
		t.Cleanup(b.Close)
		metadata.SetBroker(b.Addr(), b.BrokerID())
	}
	offsets := make([]*sarama.MockOffsetResponse, len(c.brokers))
	for i := range c.brokers {
		offsets[i] = sarama.NewMockOffsetResponse(t)
	}
	groupOffsets := sarama.NewMockOffsetFetchResponse(t)
	for p := int32(0); p < batchPartitions; p++ {
		leader := p % 2
		metadata.SetLeader("t", p, c.brokers[leader].BrokerID())
		offsets[leader].SetOffset("t", p, sarama.OffsetNewest, int64(1000+p))
		offsets[leader].SetOffset("t", p, sarama.OffsetOldest, int64(100+p))
		groupOffsets.SetOffset("g", "t", p, int64(500+p), "", sarama.ErrNoError)
	}
	for i, b := range c.brokers {
		b.SetHandlerByMap(map[string]sarama.MockResponse{
			"MetadataRequest":    metadata,
			"OffsetRequest":      offsets[i],
			"OffsetFetchRequest": groupOffsets,
			"ListGroupsRequest":  sarama.NewMockListGroupsResponse(t),
		})
	}

	c.config = sarama.NewConfig()
	c.config.ApiVersionsRequest = false
	// what Strimzi passes for our Kafka 4.0 brokers, which reject ListOffsets v0
	c.config.Version = sarama.V4_0_0_0
	client, err := sarama.NewClient([]string{c.brokers[0].Addr()}, c.config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	c.client = client
	return c
}

func (c *twoBrokerCluster) offsetRequests(t *testing.T) []*sarama.OffsetRequest {
	t.Helper()
	var requests []*sarama.OffsetRequest
	for _, b := range c.brokers {
		var count int
		for _, rr := range b.History() {
			if req, ok := rr.Request.(*sarama.OffsetRequest); ok {
				requests = append(requests, req)
				count++
			}
		}
		if count == 0 {
			t.Errorf("broker %d got no offset request", b.BrokerID())
		}
	}
	want := sarama.NewOffsetRequest(c.config.Version).Version
	for _, req := range requests {
		if req.Version != want {
			t.Errorf("offset request version %d, want %d (client.GetOffset's)", req.Version, want)
		}
	}
	return requests
}

func gaugesByPartition(t *testing.T, metrics []prometheus.Metric, desc *prometheus.Desc) map[string]float64 {
	t.Helper()
	values := make(map[string]float64)
	for _, m := range metrics {
		if m.Desc() != desc {
			continue
		}
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		var partition string
		for _, l := range out.GetLabel() {
			if l.GetName() == "partition" {
				partition = l.GetValue()
			}
		}
		values[partition] = out.GetGauge().GetValue()
	}
	return values
}

func drain(ch chan prometheus.Metric) []prometheus.Metric {
	close(ch)
	var metrics []prometheus.Metric
	for m := range ch {
		metrics = append(metrics, m)
	}
	return metrics
}

// The topic walk used to send two offset requests per partition; it now sends
// one newest and one oldest request to each partition's leader.
func TestCollectFetchesTopicOffsetsPerLeaderBroker(t *testing.T) {
	initTopicMetricDescs()
	c := newTwoBrokerCluster(t)
	e := &Exporter{
		client:              c.client,
		topicFilter:         regexp.MustCompile(".*"),
		topicExclude:        regexp.MustCompile("^$"),
		groupFilter:         regexp.MustCompile(".*"),
		groupExclude:        regexp.MustCompile("^$"),
		nextMetadataRefresh: time.Now().Add(time.Hour),
		topicWorkers:        4,
		groupWorkers:        1,
		groupMetricsTimeout: time.Second,
	}
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	metrics := drain(ch)

	if got := len(c.offsetRequests(t)); got != 2*len(c.brokers) {
		t.Errorf("got %d offset requests, want %d (newest and oldest per broker)", got, 2*len(c.brokers))
	}
	newest := gaugesByPartition(t, metrics, topicCurrentOffset)
	oldest := gaugesByPartition(t, metrics, topicOldestOffset)
	if len(newest) != batchPartitions || len(oldest) != batchPartitions {
		t.Fatalf("got %d current and %d oldest offsets, want %d each", len(newest), len(oldest), batchPartitions)
	}
	for p := 0; p < batchPartitions; p++ {
		key := strconv.Itoa(p)
		if newest[key] != float64(1000+p) || oldest[key] != float64(100+p) {
			t.Errorf("partition %d: current %v oldest %v, want %d and %d", p, newest[key], oldest[key], 1000+p, 100+p)
		}
	}
}

// Consumer group lag used to cost one offset request per partition the group
// committed; it now costs one per leader broker.
func TestGroupLagFetchesNewestOffsetsPerLeaderBroker(t *testing.T) {
	initGroupMetricDescs()
	c := newTwoBrokerCluster(t)
	e := &Exporter{client: c.client, offsetShowAll: true}
	offsetMap := map[string]map[int32]int64{"t": {}}
	for p := int32(0); p < batchPartitions; p++ {
		offsetMap["t"][p] = 0
	}
	ch := make(chan prometheus.Metric, 1000)
	e.emitGroupMetrics(consumerGroupInfo{id: "g"}, sarama.NewBroker(c.brokers[0].Addr()), offsetMap, ch)
	metrics := drain(ch)

	if got := len(c.offsetRequests(t)); got != len(c.brokers) {
		t.Errorf("got %d offset requests, want %d (one per broker)", got, len(c.brokers))
	}
	lag := gaugesByPartition(t, metrics, consumergroupLag)
	if len(lag) != batchPartitions {
		t.Fatalf("got %d lag metrics, want %d", len(lag), batchPartitions)
	}
	for p, value := range lag {
		if value != 500 {
			t.Errorf("partition %s: lag %v, want 500", p, value)
		}
	}
}

// client.GetOffset refreshed metadata after a failed lookup, so a moved leader
// was found again. The batch must still refresh the failing topic, once.
func TestCollectRefreshesMetadataOfTopicsWithFailedOffsets(t *testing.T) {
	initTopicMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	metadata := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).
		SetLeader("moved", 0, mock.BrokerID()).
		SetLeader("moved", 1, mock.BrokerID()).
		SetLeader("kept", 0, mock.BrokerID())
	offsets := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(sarama.NewConfig().Version).Version}
	offsets.AddTopicPartition("kept", 0, 10)
	offsets.Blocks["moved"] = map[int32]*sarama.OffsetResponseBlock{
		0: {Err: sarama.ErrNotLeaderForPartition},
		1: {Err: sarama.ErrNotLeaderForPartition},
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   metadata,
		"OffsetRequest":     sarama.NewMockWrapper(offsets),
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
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	drain(ch)

	var refreshes, keptRefreshes int
	for _, rr := range mock.History() {
		if req, ok := rr.Request.(*sarama.MetadataRequest); ok {
			if slices.Contains(req.Topics, "moved") {
				refreshes++
			}
			if slices.Contains(req.Topics, "kept") {
				keptRefreshes++
			}
		}
	}
	if refreshes != 1 {
		t.Errorf("got %d metadata refreshes of the failing topic, want 1", refreshes)
	}
	if keptRefreshes != 0 {
		t.Errorf("got %d metadata refreshes of a healthy topic, want 0", keptRefreshes)
	}
}

// A partition the broker leaves out of its answer has no offset and an error,
// as client.GetOffset reported ErrIncompleteResponse.
func TestOffsetBatchReportsPartitionsMissingFromTheResponse(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	offsets := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	offsets.AddTopicPartition("t", 0, 10)
	mock.SetHandlerByMap(map[string]sarama.MockResponse{"OffsetRequest": sarama.NewMockWrapper(offsets)})
	broker := sarama.NewBroker(mock.Addr())
	if err := broker.Open(config); err != nil {
		t.Fatalf("open broker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })

	batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
	batch.add(broker, "t", 0)
	batch.add(broker, "t", 1)
	result := batch.fetch()

	if got := result.offsets["t"]; len(got) != 1 || got[0] != 10 {
		t.Errorf("got offsets %v, want only partition 0 at 10", got)
	}
	if !errors.Is(result.errs["t"], sarama.ErrIncompleteResponse) {
		t.Errorf("got error %v, want %v", result.errs["t"], sarama.ErrIncompleteResponse)
	}
}

// A failed request leaves every topic it asked for with an error, so the
// caller refreshes them.
func TestOffsetBatchReportsEveryTopicOfAFailedRequest(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Net.ReadTimeout = 200 * time.Millisecond
	broker := sarama.NewBroker(mock.Addr())
	if err := broker.Open(config); err != nil {
		t.Fatalf("open broker: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	mock.Close()

	batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
	batch.add(broker, "a", 0)
	batch.add(broker, "b", 0)
	result := batch.fetch()

	if len(result.offsets) != 0 {
		t.Errorf("got offsets %v from a closed broker", result.offsets)
	}
	for _, topic := range []string{"a", "b"} {
		if result.errs[topic] == nil {
			t.Errorf("topic %s has no error", topic)
		}
	}
}

// Partition t/0 is cached as led by broker 1, which answers NOT_LEADER; the
// refreshed metadata names broker 2, which has newest 42 and oldest 7. The
// group "g" committed 40.
func newMovedLeaderCluster(t *testing.T) (sarama.Client, []*sarama.MockBroker) {
	t.Helper()
	brokers := []*sarama.MockBroker{sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2)}
	metadata := func(leader int32) *sarama.MockMetadataResponse {
		m := sarama.NewMockMetadataResponse(t)
		for _, b := range brokers {
			m.SetBroker(b.Addr(), b.BrokerID())
		}
		return m.SetLeader("t", 0, leader)
	}
	sequence := sarama.NewMockSequence(metadata(1), metadata(2))
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	stale := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
	groupOffsets := sarama.NewMockOffsetFetchResponse(t).SetOffset("g", "t", 0, 40, "", sarama.ErrNoError)
	brokers[0].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":    sequence,
		"OffsetRequest":      sarama.NewMockWrapper(stale),
		"OffsetFetchRequest": groupOffsets,
		"ListGroupsRequest":  sarama.NewMockListGroupsResponse(t),
	})
	brokers[1].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sequence,
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("t", 0, sarama.OffsetNewest, 42).
			SetOffset("t", 0, sarama.OffsetOldest, 7),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
	for _, b := range brokers {
		t.Cleanup(b.Close)
	}
	client, err := sarama.NewClient([]string{brokers[0].Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, brokers
}

// client.GetOffset retried on the refreshed leader within the same collect; a
// moved leader (a broker roll) must not drop the partition's offsets for a collect.
func TestCollectAsksTheRefreshedLeaderAgain(t *testing.T) {
	initTopicMetricDescs()
	client, _ := newMovedLeaderCluster(t)
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
	metrics := drain(ch)

	if got := gaugesByPartition(t, metrics, topicCurrentOffset); got["0"] != 42 {
		t.Errorf("current offsets %v, want partition 0 at 42 from the new leader", got)
	}
	if got := gaugesByPartition(t, metrics, topicOldestOffset); got["0"] != 7 {
		t.Errorf("oldest offsets %v, want partition 0 at 7 from the new leader", got)
	}
}

func TestGroupLagAsksTheRefreshedLeaderAgain(t *testing.T) {
	initGroupMetricDescs()
	client, brokers := newMovedLeaderCluster(t)
	e := &Exporter{client: client, offsetShowAll: true}
	ch := make(chan prometheus.Metric, 100)
	e.emitGroupMetrics(consumerGroupInfo{id: "g"}, sarama.NewBroker(brokers[0].Addr()), map[string]map[int32]int64{"t": {0: 0}}, ch)
	metrics := drain(ch)

	if got := gaugesByPartition(t, metrics, consumergroupLag); got["0"] != 2 {
		t.Errorf("lag %v, want partition 0 at 2 (42 from the new leader - 40)", got)
	}
}

// Topic "t" (partitions 0..n-1) is cached as led by broker 0, which answers
// NOT_LEADER for all of it; refreshed is what every later metadata request says.
func newStaleLeaderCluster(t *testing.T, partitions int32, refreshed func(*sarama.MockMetadataResponse)) (sarama.Client, *sarama.MockBroker, *sarama.Config) {
	t.Helper()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	cached := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID())
	later := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID())
	refreshed(later)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = time.Millisecond
	stale := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {}}
	groupOffsets := sarama.NewMockOffsetFetchResponse(t)
	for p := int32(0); p < partitions; p++ {
		cached.SetLeader("t", p, mock.BrokerID())
		stale.Blocks["t"][p] = &sarama.OffsetResponseBlock{Err: sarama.ErrNotLeaderForPartition}
		groupOffsets.SetOffset("g", "t", p, 5, "", sarama.ErrNoError)
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":    sarama.NewMockSequence(cached, later),
		"OffsetRequest":      sarama.NewMockWrapper(stale),
		"OffsetFetchRequest": groupOffsets,
		"ListGroupsRequest":  sarama.NewMockListGroupsResponse(t),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, mock, config
}

func metadataRequestsFor(mock *sarama.MockBroker, topic string) int {
	var n int
	for _, rr := range mock.History() {
		if req, ok := rr.Request.(*sarama.MetadataRequest); ok && slices.Contains(req.Topics, topic) {
			n++
		}
	}
	return n
}

// After the refresh every partition of "t" is leaderless. A leader lookup that
// misses the cache refreshes metadata itself, so retrying each partition would
// cost a refresh per partition and batch; the retry gives up on the topic instead.
func TestRetryGivesUpOnALeaderlessTopic(t *testing.T) {
	initTopicMetricDescs()
	const partitions = 20
	client, mock, config := newStaleLeaderCluster(t, partitions, func(m *sarama.MockMetadataResponse) {
		for p := int32(0); p < partitions; p++ {
			m.SetLeader("t", p, -1)
		}
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
	metrics := drain(ch)

	// the refresh of the failing topic, then one leader lookup's refresh, each with retries
	got, limit := metadataRequestsFor(mock, "t"), 2*(config.Metadata.Retry.Max+1)
	if got == 0 {
		t.Fatal("no metadata refresh of t: the offsets did not fail, so the retry was not reached")
	}
	if got > limit {
		t.Errorf("leaderless topic cost %d metadata requests, want at most %d", got, limit)
	}
	if offsets := gaugesByPartition(t, metrics, topicCurrentOffset); len(offsets) != 0 {
		t.Errorf("current offsets %v for a leaderless topic, want none", offsets)
	}
}

// The offset request fails with NOT_LEADER and the refresh shows the topic was
// deleted: lag and lag sum are skipped as for any deleted topic, not emitted as 0.
func TestGroupLagSkipsATopicDeletedAfterAnotherError(t *testing.T) {
	initGroupMetricDescs()
	client, mock, _ := newStaleLeaderCluster(t, 2, func(*sarama.MockMetadataResponse) {})
	e := &Exporter{client: client, offsetShowAll: true}
	ch := make(chan prometheus.Metric, 100)
	e.emitGroupMetrics(consumerGroupInfo{id: "g"}, sarama.NewBroker(mock.Addr()), map[string]map[int32]int64{"t": {0: 10, 1: 10}}, ch)

	var offsets, lag, lagSums int
	for _, m := range drain(ch) {
		switch m.Desc() {
		case consumergroupCurrentOffset:
			offsets++
		case consumergroupLag:
			lag++
		case consumergroupLagSum:
			lagSums++
		}
	}
	// the group was read, so the deleted-topic path was reached
	if offsets != 2 {
		t.Fatalf("got %d consumer group offsets, want 2", offsets)
	}
	if lag != 0 || lagSums != 0 {
		t.Errorf("got %d lag and %d lag sum metrics for a deleted topic, want none", lag, lagSums)
	}
}

// Topic "t" has 20 partitions that Kafka reports LEADER_NOT_AVAILABLE, as during
// an outage of every replica; "kept" has a leader.
func newLeaderlessCluster(t *testing.T) (sarama.Client, *sarama.MockBroker, *sarama.Config) {
	t.Helper()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = time.Millisecond
	metadata := &sarama.MetadataResponse{Version: sarama.NewMetadataRequest(config.Version, nil).Version}
	metadata.AddBroker(mock.Addr(), mock.BrokerID())
	replicas := []int32{mock.BrokerID()}
	metadata.AddTopicPartition("kept", 0, mock.BrokerID(), replicas, replicas, nil, sarama.ErrNoError)
	groupOffsets := sarama.NewMockOffsetFetchResponse(t)
	for p := int32(0); p < 20; p++ {
		metadata.AddTopicPartition("t", p, -1, replicas, nil, replicas, sarama.ErrLeaderNotAvailable)
		groupOffsets.SetOffset("g", "t", p, 5, "", sarama.ErrNoError)
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":    sarama.NewMockWrapper(metadata),
		"OffsetRequest":      sarama.NewMockOffsetResponse(t).SetOffset("kept", 0, sarama.OffsetNewest, 10).SetOffset("kept", 0, sarama.OffsetOldest, 0),
		"OffsetFetchRequest": groupOffsets,
		"ListGroupsRequest":  sarama.NewMockListGroupsResponse(t),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, mock, config
}

// A leader lookup for a leaderless partition refreshes metadata every time, so
// walking a leaderless topic cost a refresh per partition; the walk now reads
// which partitions have a leader from the cache.
func TestCollectSkipsLeaderlessPartitionsWithoutRefreshing(t *testing.T) {
	initTopicMetricDescs()
	client, mock, config := newLeaderlessCluster(t)
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
	metrics := drain(ch)

	// WritablePartitions refreshes when a topic has no partition with a leader, and
	// once more to catch partitions that recovered in that refresh; not per partition
	if got, limit := metadataRequestsFor(mock, "t"), 2*(config.Metadata.Retry.Max+1); got > limit {
		t.Errorf("leaderless topic cost %d metadata requests, want at most %d", got, limit)
	}
	if got := gaugesByPartition(t, metrics, topicCurrentOffset); len(got) != 1 {
		t.Errorf("current offsets %v, want only kept's partition", got)
	}
}

func TestGroupLagSkipsLeaderlessPartitionsWithoutRefreshing(t *testing.T) {
	initGroupMetricDescs()
	client, mock, config := newLeaderlessCluster(t)
	e := &Exporter{client: client, offsetShowAll: true}
	offsetMap := map[string]map[int32]int64{"t": {}}
	for p := int32(0); p < 20; p++ {
		offsetMap["t"][p] = 10
	}
	ch := make(chan prometheus.Metric, 1000)
	e.emitGroupMetrics(consumerGroupInfo{id: "g"}, sarama.NewBroker(mock.Addr()), offsetMap, ch)
	metrics := drain(ch)

	// the group was read, so its partitions reached the leader check
	if got := len(gaugesByPartition(t, metrics, consumergroupCurrentOffset)); got != 20 {
		t.Fatalf("got %d consumer group offsets, want 20", got)
	}
	if got := len(gaugesByPartition(t, metrics, consumergroupLag)); got != 0 {
		t.Errorf("got %d lag metrics for leaderless partitions, want none", got)
	}
	if got, limit := metadataRequestsFor(mock, "t"), 2*(config.Metadata.Retry.Max+1); got > limit {
		t.Errorf("leaderless topic cost %d metadata requests in group lag, want at most %d", got, limit)
	}
}

// The topic walk reads "t" fine, then every group's newest-offset request for it
// fails while metadata still names the broker. Within sarama's metadata retry
// backoff the collect refreshes "t" once, not once per group.
func TestCollectRefreshesAFailingTopicOncePerCollect(t *testing.T) {
	initTopicMetricDescs()
	initGroupMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	// metadata never fails here, so sarama never waits it; the collect's bound
	// must not depend on how fast the groups run
	config.Metadata.Retry.Backoff = time.Hour
	ok := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	ok.AddTopicPartition("t", 0, 10)
	failing := &sarama.OffsetResponse{Version: ok.Version}
	failing.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
	groups := []string{"g1", "g2", "g3"}
	list := sarama.NewMockListGroupsResponse(t)
	describe := sarama.NewMockDescribeGroupsResponse(t)
	groupOffsets := sarama.NewMockOffsetFetchResponse(t)
	for _, g := range groups {
		list.AddGroup(g, "consumer")
		describe.AddGroupDescription(g, &sarama.GroupDescription{GroupId: g, State: "Stable", ProtocolType: "consumer"})
		groupOffsets.SetOffset(g, "t", 0, 5, "", sarama.ErrNoError)
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).SetLeader("t", 0, mock.BrokerID()),
		// the walk's newest and oldest requests succeed, every later one fails
		"OffsetRequest":          sarama.NewMockSequence(sarama.NewMockWrapper(ok), sarama.NewMockWrapper(ok), sarama.NewMockWrapper(failing)),
		"OffsetFetchRequest":     groupOffsets,
		"ListGroupsRequest":      list,
		"DescribeGroupsRequest":  describe,
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t),
	})
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
		offsetShowAll:       true,
		nextMetadataRefresh: time.Now().Add(time.Hour),
		topicWorkers:        1,
		// one at a time: sarama already merges refreshes that run concurrently
		groupWorkers:        1,
		groupMetricsTimeout: 5 * time.Second,
	}
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	metrics := drain(ch)

	seen := map[string]bool{}
	for _, m := range metrics {
		if m.Desc() != consumergroupCurrentOffset {
			continue
		}
		var out dto.Metric
		_ = m.Write(&out)
		for _, l := range out.GetLabel() {
			if l.GetName() == "consumergroup" {
				seen[l.GetValue()] = true
			}
		}
	}
	if len(seen) != len(groups) {
		t.Fatalf("read %d groups, want %d", len(seen), len(groups))
	}
	if got := metadataRequestsFor(mock, "t"); got != 1 {
		t.Errorf("got %d metadata refreshes of the failing topic across %d groups, want 1", got, len(groups))
	}
}

// The cache has no leader for any partition of "t"; the refresh that check
// triggers brings partition 0 back while the rest stay leaderless, so sarama
// reports LEADER_NOT_AVAILABLE. Partition 0's offsets are still read.
func TestLeaderCheckKeepsPartitionsThatRecoveredInItsRefresh(t *testing.T) {
	initTopicMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	// one request per refresh, so the second metadata response is the refresh's
	config.Metadata.Retry.Max = 0
	version := sarama.NewMetadataRequest(config.Version, nil).Version
	replicas := []int32{mock.BrokerID()}
	metadata := func(recovered int32) *sarama.MetadataResponse {
		m := &sarama.MetadataResponse{Version: version}
		m.AddBroker(mock.Addr(), mock.BrokerID())
		for p := int32(0); p < 20; p++ {
			if p < recovered {
				m.AddTopicPartition("t", p, mock.BrokerID(), replicas, replicas, nil, sarama.ErrNoError)
			} else {
				m.AddTopicPartition("t", p, -1, replicas, nil, replicas, sarama.ErrLeaderNotAvailable)
			}
		}
		return m
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   sarama.NewMockSequence(sarama.NewMockWrapper(metadata(0)), sarama.NewMockWrapper(metadata(1))),
		"OffsetRequest":     sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 42).SetOffset("t", 0, sarama.OffsetOldest, 7),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
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
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)

	if got := gaugesByPartition(t, drain(ch), topicCurrentOffset); len(got) != 1 || got["0"] != 42 {
		t.Errorf("current offsets %v, want only partition 0 at 42", got)
	}
}

// The topic walk reads "t", then it is deleted; three groups committed offsets
// for it. The first group's failed request confirms the deletion, and the
// others skip "t" with no further metadata refresh, and no lag sum.
func TestCollectSkipsATopicDeletedAfterTheWalkForLaterGroups(t *testing.T) {
	initTopicMetricDescs()
	initGroupMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = time.Millisecond
	ok := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	ok.AddTopicPartition("t", 0, 10)
	gone := &sarama.OffsetResponse{Version: ok.Version}
	gone.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrUnknownTopicOrPartition}}}
	groups := []string{"g1", "g2", "g3"}
	list := sarama.NewMockListGroupsResponse(t)
	describe := sarama.NewMockDescribeGroupsResponse(t)
	groupOffsets := sarama.NewMockOffsetFetchResponse(t)
	for _, g := range groups {
		list.AddGroup(g, "consumer")
		describe.AddGroupDescription(g, &sarama.GroupDescription{GroupId: g, State: "Stable", ProtocolType: "consumer"})
		groupOffsets.SetOffset(g, "t", 0, 5, "", sarama.ErrNoError)
	}
	withTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).SetLeader("t", 0, mock.BrokerID())
	withoutTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID())
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":        sarama.NewMockSequence(withTopic, withoutTopic),
		"OffsetRequest":          sarama.NewMockSequence(sarama.NewMockWrapper(ok), sarama.NewMockWrapper(ok), sarama.NewMockWrapper(gone)),
		"OffsetFetchRequest":     groupOffsets,
		"ListGroupsRequest":      list,
		"DescribeGroupsRequest":  describe,
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t),
	})
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
		offsetShowAll:       true,
		nextMetadataRefresh: time.Now().Add(time.Hour),
		topicWorkers:        1,
		groupWorkers:        1,
		groupMetricsTimeout: 5 * time.Second,
	}
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	metrics := drain(ch)

	var offsets, lagSums int
	for _, m := range metrics {
		switch m.Desc() {
		case consumergroupCurrentOffset:
			offsets++
		case consumergroupLagSum:
			lagSums++
		}
	}
	if offsets != len(groups) {
		t.Fatalf("got %d consumer group offsets, want %d: the groups were not read", offsets, len(groups))
	}
	if lagSums != 0 {
		t.Errorf("got %d lag sums for a deleted topic, want none", lagSums)
	}
	// the first group's one refresh, with its retries
	if got, limit := metadataRequestsFor(mock, "t"), config.Metadata.Retry.Max+1; got == 0 || got > limit {
		t.Errorf("deleted topic cost %d metadata requests across %d groups, want 1 to %d", got, len(groups), limit)
	}
}

// Partitions 0-4 of "t" are leaderless in the cache and 5-9 have a leader. The
// collect refreshes "t" once before skipping 0-4: if the refresh shows their new
// leader, their offsets and group lag are read in the same collect.
func TestCollectRefreshesAPartlyLeaderlessTopicOnce(t *testing.T) {
	tests := []struct {
		name      string
		recovered bool
		want      int
	}{
		{name: "leaders elected since", recovered: true, want: 10},
		{name: "still leaderless", recovered: false, want: 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initTopicMetricDescs()
			initGroupMetricDescs()
			mock := sarama.NewMockBroker(t, 0)
			t.Cleanup(mock.Close)
			config := sarama.NewConfig()
			config.ApiVersionsRequest = false
			// one request per refresh, so the second metadata response is the refresh's
			config.Metadata.Retry.Max = 0
			version := sarama.NewMetadataRequest(config.Version, nil).Version
			replicas := []int32{mock.BrokerID()}
			metadata := func(leaderless int32) *sarama.MetadataResponse {
				m := &sarama.MetadataResponse{Version: version}
				m.AddBroker(mock.Addr(), mock.BrokerID())
				for p := int32(0); p < 10; p++ {
					if p < leaderless {
						m.AddTopicPartition("t", p, -1, replicas, nil, replicas, sarama.ErrLeaderNotAvailable)
					} else {
						m.AddTopicPartition("t", p, mock.BrokerID(), replicas, replicas, nil, sarama.ErrNoError)
					}
				}
				return m
			}
			refreshed := metadata(5)
			if tt.recovered {
				refreshed = metadata(0)
			}
			offsets := sarama.NewMockOffsetResponse(t)
			groupOffsets := sarama.NewMockOffsetFetchResponse(t)
			for p := int32(0); p < 10; p++ {
				offsets.SetOffset("t", p, sarama.OffsetNewest, 10).SetOffset("t", p, sarama.OffsetOldest, 0)
				groupOffsets.SetOffset("g", "t", p, 5, "", sarama.ErrNoError)
			}
			mock.SetHandlerByMap(map[string]sarama.MockResponse{
				"MetadataRequest":        sarama.NewMockSequence(sarama.NewMockWrapper(metadata(5)), sarama.NewMockWrapper(refreshed)),
				"OffsetRequest":          offsets,
				"OffsetFetchRequest":     groupOffsets,
				"ListGroupsRequest":      sarama.NewMockListGroupsResponse(t).AddGroup("g", "consumer"),
				"DescribeGroupsRequest":  sarama.NewMockDescribeGroupsResponse(t).AddGroupDescription("g", &sarama.GroupDescription{GroupId: "g", State: "Stable", ProtocolType: "consumer"}),
				"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t),
			})
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
				offsetShowAll:       true,
				nextMetadataRefresh: time.Now().Add(time.Hour),
				topicWorkers:        1,
				groupWorkers:        1,
				groupMetricsTimeout: 5 * time.Second,
			}
			ch := make(chan prometheus.Metric, 1000)
			e.collect(ch)
			metrics := drain(ch)

			if got := len(gaugesByPartition(t, metrics, topicCurrentOffset)); got != tt.want {
				t.Errorf("got %d current offsets, want %d", got, tt.want)
			}
			if got := len(gaugesByPartition(t, metrics, consumergroupLag)); got != tt.want {
				t.Errorf("got %d lag metrics, want %d", got, tt.want)
			}
			if got := metadataRequestsFor(mock, "t"); got != 1 {
				t.Errorf("got %d metadata refreshes of t in one collect, want 1", got)
			}
		})
	}
}

// Partitions 0-4 of "t" are leaderless in the cache and 5-9 are led by broker
// 1. The leader check refreshes "t": all ten are led by broker 1. Broker 1 then
// answers NOT_LEADER, and the refresh for the failed fetch moves them to broker
// 2. The leader check's refresh must not use up the failed fetch's refresh.
func TestFailedFetchRefreshesAfterTheLeaderCheckRefreshed(t *testing.T) {
	initTopicMetricDescs()
	brokers := []*sarama.MockBroker{sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2)}
	for _, b := range brokers {
		t.Cleanup(b.Close)
	}
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	// one request per refresh, so each metadata response is one refresh's
	config.Metadata.Retry.Max = 0
	version := sarama.NewMetadataRequest(config.Version, nil).Version
	metadata := func(leaderless int32, leader *sarama.MockBroker) *sarama.MetadataResponse {
		m := &sarama.MetadataResponse{Version: version}
		for _, b := range brokers {
			m.AddBroker(b.Addr(), b.BrokerID())
		}
		replicas := []int32{leader.BrokerID()}
		for p := int32(0); p < 10; p++ {
			if p < leaderless {
				m.AddTopicPartition("t", p, -1, replicas, nil, replicas, sarama.ErrLeaderNotAvailable)
			} else {
				m.AddTopicPartition("t", p, leader.BrokerID(), replicas, replicas, nil, sarama.ErrNoError)
			}
		}
		return m
	}
	sequence := sarama.NewMockSequence(
		sarama.NewMockWrapper(metadata(5, brokers[0])),
		sarama.NewMockWrapper(metadata(0, brokers[0])),
		sarama.NewMockWrapper(metadata(0, brokers[1])),
	)
	stale := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {}}
	offsets := sarama.NewMockOffsetResponse(t)
	for p := int32(0); p < 10; p++ {
		stale.Blocks["t"][p] = &sarama.OffsetResponseBlock{Err: sarama.ErrNotLeaderForPartition}
		offsets.SetOffset("t", p, sarama.OffsetNewest, 10).SetOffset("t", p, sarama.OffsetOldest, 0)
	}
	brokers[0].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   sequence,
		"OffsetRequest":     sarama.NewMockWrapper(stale),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
	brokers[1].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   sequence,
		"OffsetRequest":     offsets,
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
	client, err := sarama.NewClient([]string{brokers[0].Addr()}, config)
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
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)

	if got := len(gaugesByPartition(t, drain(ch), topicCurrentOffset)); got != 10 {
		t.Errorf("got %d current offsets, want 10 from broker 2", got)
	}
}

// Every partition of "t" is leaderless in the cache and then no broker answers:
// the leader check's refresh fails with something other than an unknown topic.
// The collect must not fall back to a leader lookup, and so a refresh attempt,
// per partition.
func TestCollectSkipsALeaderlessTopicWhenMetadataIsUnreachable(t *testing.T) {
	initTopicMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Net.DialTimeout = 200 * time.Millisecond
	config.Metadata.Retry.Max = 1
	var attempts atomic.Int32
	config.Metadata.Retry.BackoffFunc = func(int, int) time.Duration {
		attempts.Add(1)
		return 0
	}
	metadata := &sarama.MetadataResponse{Version: sarama.NewMetadataRequest(config.Version, nil).Version}
	metadata.AddBroker(mock.Addr(), mock.BrokerID())
	replicas := []int32{mock.BrokerID()}
	for p := int32(0); p < 20; p++ {
		metadata.AddTopicPartition("t", p, -1, replicas, nil, replicas, sarama.ErrLeaderNotAvailable)
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": sarama.NewMockWrapper(metadata)})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	mock.Close()
	attempts.Store(0)

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
	drain(ch)

	// the leader check's one refresh, which retries once
	if got := attempts.Load(); got != 1 {
		t.Errorf("unreachable metadata cost %d refresh retries, want 1, not one per partition", got)
	}
}

// leaderHookClient calls onLeader after each successful leader lookup. The
// group path looks a partition's leader up after its deletion checks and
// before it sends the offset request.
type leaderHookClient struct {
	sarama.Client
	onLeader func()
}

func (c *leaderHookClient) Leader(topic string, partition int32) (*sarama.Broker, error) {
	leader, err := c.Client.Leader(topic, partition)
	if err == nil {
		c.onLeader()
	}
	return leader, err
}

// Another group confirms "t" deleted after this group's own deletion checks,
// before this group's offsets are emitted. This group must then skip t's lag
// and lag sum too, not report them.
func TestGroupLagSkipsATopicAnotherGroupFoundDeleted(t *testing.T) {
	initGroupMetricDescs()
	coordinator, leader := sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2)
	t.Cleanup(coordinator.Close)
	t.Cleanup(leader.Close)
	metadata := sarama.NewMockMetadataResponse(t).
		SetBroker(coordinator.Addr(), coordinator.BrokerID()).
		SetBroker(leader.Addr(), leader.BrokerID()).
		SetLeader("t", 0, leader.BrokerID())
	coordinator.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":    metadata,
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset("g", "t", 0, 5, "", sarama.ErrNoError),
	})
	leader.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": metadata,
		"OffsetRequest":   sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 10),
	})
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	client, err := sarama.NewClient([]string{coordinator.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	meta := newCollectMetadata(client)
	e := &Exporter{
		client:        &leaderHookClient{Client: client, onLeader: func() { meta.markDeleted("t") }},
		offsetShowAll: true,
	}
	ch := make(chan prometheus.Metric, 100)
	e.emitGroupMetricsIn(meta, consumerGroupInfo{id: "g"}, sarama.NewBroker(coordinator.Addr()), map[string]map[int32]int64{"t": {0: 10}}, ch)

	var offsets, lag, lagSums int
	for _, m := range drain(ch) {
		switch m.Desc() {
		case consumergroupCurrentOffset:
			offsets++
		case consumergroupLag:
			lag++
		case consumergroupLagSum:
			lagSums++
		}
	}
	if offsets != 1 {
		t.Fatalf("got %d consumer group offsets, want 1: the group was not read", offsets)
	}
	// the deletion must land after the group's own checks, or the emit-time check is untested
	var requests int
	for _, rr := range leader.History() {
		if _, ok := rr.Request.(*sarama.OffsetRequest); ok {
			requests++
		}
	}
	if requests != 1 {
		t.Fatalf("leader got %d offset requests, want 1: the group skipped t before the deletion was marked", requests)
	}
	if lag != 0 || lagSums != 0 {
		t.Errorf("got %d lag and %d lag sum metrics for a topic another group found deleted, want none", lag, lagSums)
	}
}

// Partition t/0 moves twice in one collect: the walk's fetch from broker 1 gets
// NOT_LEADER and its refresh finds broker 2; broker 2 then answers the group's
// fetch with NOT_LEADER and the next refresh finds broker 3. The walk's refresh
// must not stop the group's own refresh, or the group's lag goes missing.
func TestGroupRefreshesAfterTheWalkRefreshed(t *testing.T) {
	initTopicMetricDescs()
	initGroupMetricDescs()
	brokers := []*sarama.MockBroker{sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2), sarama.NewMockBroker(t, 3)}
	for _, b := range brokers {
		t.Cleanup(b.Close)
	}
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	// one request per refresh, so each metadata response is one refresh's
	config.Metadata.Retry.Max = 0
	metadata := func(leader *sarama.MockBroker) *sarama.MockMetadataResponse {
		m := sarama.NewMockMetadataResponse(t)
		for _, b := range brokers {
			m.SetBroker(b.Addr(), b.BrokerID())
		}
		return m.SetLeader("t", 0, leader.BrokerID())
	}
	sequence := sarama.NewMockSequence(metadata(brokers[0]), metadata(brokers[1]), metadata(brokers[2]))
	ok := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	ok.AddTopicPartition("t", 0, 10)
	stale := &sarama.OffsetResponse{Version: ok.Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
	brokers[0].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":        sequence,
		"OffsetRequest":          sarama.NewMockWrapper(stale),
		"OffsetFetchRequest":     sarama.NewMockOffsetFetchResponse(t).SetOffset("g", "t", 0, 40, "", sarama.ErrNoError),
		"ListGroupsRequest":      sarama.NewMockListGroupsResponse(t).AddGroup("g", "consumer"),
		"DescribeGroupsRequest":  sarama.NewMockDescribeGroupsResponse(t).AddGroupDescription("g", &sarama.GroupDescription{GroupId: "g", State: "Stable", ProtocolType: "consumer"}),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t),
	})
	brokers[1].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sequence,
		// the walk's newest and oldest retries succeed, the group's fetch does not
		"OffsetRequest":     sarama.NewMockSequence(sarama.NewMockWrapper(ok), sarama.NewMockWrapper(ok), sarama.NewMockWrapper(stale)),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
	brokers[2].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   sequence,
		"OffsetRequest":     sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 42),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
	client, err := sarama.NewClient([]string{brokers[0].Addr()}, config)
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
		offsetShowAll:       true,
		nextMetadataRefresh: time.Now().Add(time.Hour),
		topicWorkers:        1,
		groupWorkers:        1,
		groupMetricsTimeout: 5 * time.Second,
	}
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	metrics := drain(ch)

	if got := gaugesByPartition(t, metrics, topicCurrentOffset); got["0"] != 10 {
		t.Fatalf("current offsets %v, want partition 0 at 10 from broker 2: the walk's retry failed", got)
	}
	if got := gaugesByPartition(t, metrics, consumergroupLag); got["0"] != 2 {
		t.Errorf("lag %v, want partition 0 at 2 (42 from broker 3 - 40)", got)
	}
}

// t/0 moves twice between consumer groups: the first group's fetch from broker 1
// gets NOT_LEADER and its refresh finds broker 2; broker 2 then answers the
// second group's fetch with NOT_LEADER, and that group's refresh finds broker 3.
// The first group's refresh must not stop the second group's.
func TestLaterGroupRefreshesAfterAnEarlierGroupRefreshed(t *testing.T) {
	initTopicMetricDescs()
	initGroupMetricDescs()
	brokers := []*sarama.MockBroker{sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2), sarama.NewMockBroker(t, 3)}
	for _, b := range brokers {
		t.Cleanup(b.Close)
	}
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	// one request per refresh, so each metadata response is one refresh's
	config.Metadata.Retry.Max = 0
	metadata := func(leader *sarama.MockBroker) *sarama.MockMetadataResponse {
		m := sarama.NewMockMetadataResponse(t)
		for _, b := range brokers {
			m.SetBroker(b.Addr(), b.BrokerID())
		}
		return m.SetLeader("t", 0, leader.BrokerID())
	}
	sequence := sarama.NewMockSequence(metadata(brokers[0]), metadata(brokers[1]), metadata(brokers[2]))
	ok := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	ok.AddTopicPartition("t", 0, 10)
	stale := &sarama.OffsetResponse{Version: ok.Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
	groups := []string{"g1", "g2"}
	list := sarama.NewMockListGroupsResponse(t)
	describe := sarama.NewMockDescribeGroupsResponse(t)
	groupOffsets := sarama.NewMockOffsetFetchResponse(t)
	for _, g := range groups {
		list.AddGroup(g, "consumer")
		describe.AddGroupDescription(g, &sarama.GroupDescription{GroupId: g, State: "Stable", ProtocolType: "consumer"})
		groupOffsets.SetOffset(g, "t", 0, 5, "", sarama.ErrNoError)
	}
	brokers[0].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sequence,
		// the walk's newest and oldest succeed, the first group's fetch does not
		"OffsetRequest":          sarama.NewMockSequence(sarama.NewMockWrapper(ok), sarama.NewMockWrapper(ok), sarama.NewMockWrapper(stale)),
		"OffsetFetchRequest":     groupOffsets,
		"ListGroupsRequest":      list,
		"DescribeGroupsRequest":  describe,
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t),
	})
	brokers[1].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sequence,
		// the first group's retry succeeds, the second group's fetch does not
		"OffsetRequest":     sarama.NewMockSequence(sarama.NewMockWrapper(ok), sarama.NewMockWrapper(stale)),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
	brokers[2].SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   sequence,
		"OffsetRequest":     sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 42),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
	client, err := sarama.NewClient([]string{brokers[0].Addr()}, config)
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
		offsetShowAll:       true,
		nextMetadataRefresh: time.Now().Add(time.Hour),
		topicWorkers:        1,
		// one at a time, so the second group fetches after the first refreshed
		groupWorkers:        1,
		groupMetricsTimeout: 5 * time.Second,
	}
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)

	lag := make(map[string]float64)
	for _, m := range drain(ch) {
		if m.Desc() != consumergroupLag {
			continue
		}
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		for _, l := range out.GetLabel() {
			if l.GetName() == "consumergroup" {
				lag[l.GetValue()] = out.GetGauge().GetValue()
			}
		}
	}
	// whichever group runs first reads 10 from broker 2, the other 42 from broker 3
	values := []float64{}
	for _, g := range groups {
		values = append(values, lag[g])
	}
	slices.Sort(values)
	if len(lag) != 2 || values[0] != 5 || values[1] != 37 {
		t.Errorf("lag by group %v, want one group at 5 (10 from broker 2) and one at 37 (42 from broker 3)", lag)
	}
}

// A failed fetch must not refresh a topic that was refreshed after the fetch
// began, whether by another failed fetch or by the leader check; it must
// refresh one that was not.
func TestRefreshSkipsATopicRefreshedSinceTheFetchBegan(t *testing.T) {
	tests := []struct {
		name       string
		leaderless int32
		// refreshes t between the fetch's start and its refresh
		between func(meta *collectMetadata, fetchStarted map[string]int)
	}{
		{name: "by another failed fetch", between: func(meta *collectMetadata, fetchStarted map[string]int) {
			meta.refresh([]string{"t"}, fetchStarted)
		}},
		{name: "by the leader check of a leaderless topic", leaderless: 10, between: func(meta *collectMetadata, _ map[string]int) {
			meta.hasLeader("t", 0)
		}},
		{name: "by the leader check of a partly leaderless topic", leaderless: 5, between: func(meta *collectMetadata, _ map[string]int) {
			meta.hasLeader("t", 5)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := sarama.NewMockBroker(t, 0)
			t.Cleanup(mock.Close)
			config := sarama.NewConfig()
			config.ApiVersionsRequest = false
			// one request per refresh
			config.Metadata.Retry.Max = 0
			metadata := &sarama.MetadataResponse{Version: sarama.NewMetadataRequest(config.Version, nil).Version}
			metadata.AddBroker(mock.Addr(), mock.BrokerID())
			replicas := []int32{mock.BrokerID()}
			for p := int32(0); p < 10; p++ {
				if p < tt.leaderless {
					metadata.AddTopicPartition("t", p, -1, replicas, nil, replicas, sarama.ErrLeaderNotAvailable)
				} else {
					metadata.AddTopicPartition("t", p, mock.BrokerID(), replicas, replicas, nil, sarama.ErrNoError)
				}
			}
			mock.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": sarama.NewMockWrapper(metadata)})
			client, err := sarama.NewClient([]string{mock.Addr()}, config)
			if err != nil {
				t.Fatalf("create client: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })
			meta := newCollectMetadata(client)
			batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
			batch.add(client.Brokers()[0], "t", 9)

			fetchStarted := meta.generations([]*offsetBatch{batch})
			tt.between(meta, fetchStarted)
			before := metadataRequestsFor(mock, "t")
			if before == 0 {
				t.Fatalf("t was not refreshed between the fetch's start and its refresh")
			}
			meta.refresh([]string{"t"}, fetchStarted)
			if got := metadataRequestsFor(mock, "t") - before; got != 0 {
				t.Errorf("the fetch's refresh sent %d metadata requests for t, want 0: t was refreshed after the fetch began", got)
			}
			meta.refresh([]string{"t"}, meta.generations([]*offsetBatch{batch}))
			if got := metadataRequestsFor(mock, "t") - before; got != 1 {
				t.Errorf("a fetch that began after the last refresh sent %d metadata requests for t, want 1", got)
			}
		})
	}
}

// t/0 moves from b1 to b2, and the first refresh still names b1. Fetches within
// sarama's metadata retry backoff of that refresh must not refresh again, even
// though each fails; the first fetch after it must, and finds b2.
func TestDisagreeingTopicIsRefreshedAgainAfterTheBackoff(t *testing.T) {
	b1, b2 := sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2)
	t.Cleanup(b1.Close)
	t.Cleanup(b2.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	// one request per refresh, so each metadata response is one refresh's
	config.Metadata.Retry.Max = 0
	// far from sarama's 250ms default, so a backoff not read from the config fails
	config.Metadata.Retry.Backoff = 10 * time.Second
	metadata := func(leader *sarama.MockBroker) *sarama.MockMetadataResponse {
		return sarama.NewMockMetadataResponse(t).
			SetBroker(b1.Addr(), b1.BrokerID()).
			SetBroker(b2.Addr(), b2.BrokerID()).
			SetLeader("t", 0, leader.BrokerID())
	}
	// client start, the first refresh (still b1), the refresh after the backoff
	sequence := sarama.NewMockSequence(metadata(b1), metadata(b1), metadata(b2))
	stale := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
	b1.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": sequence, "OffsetRequest": sarama.NewMockWrapper(stale)})
	b2.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": sequence, "OffsetRequest": sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 42)})
	client, err := sarama.NewClient([]string{b1.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	e := &Exporter{client: client}
	meta := newCollectMetadata(client)
	// far from the real time, so a real clock anywhere in the check shows up
	clock := time.Unix(1_000_000, 0)
	meta.now = func() time.Time { return clock }
	refreshes := func() int { return metadataRequestsFor(b1, "t") + metadataRequestsFor(b2, "t") }
	fetch := func() (int64, bool) {
		leader, err := client.Leader("t", 0)
		if err != nil {
			t.Fatalf("leader of t/0: %v", err)
		}
		batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
		batch.add(leader, "t", 0)
		offset, ok := e.fetchOffsets(meta, batch)[0].offsets["t"][0]
		return offset, ok
	}

	// the walk: refreshed once, and the refresh still names b1
	if _, ok := fetch(); ok {
		t.Fatalf("first fetch got an offset from b1")
	}
	if got := refreshes(); got != 1 {
		t.Fatalf("got %d refreshes after the first fetch, want 1", got)
	}
	// a group within the backoff: no refresh, and its failure doesn't restart the backoff
	clock = clock.Add(config.Metadata.Retry.Backoff * 3 / 5)
	if _, ok := fetch(); ok {
		t.Fatalf("second fetch got an offset without a refresh")
	}
	if got := refreshes(); got != 1 {
		t.Fatalf("got %d refreshes within the backoff, want 1", got)
	}
	// a group after the backoff: refreshes and reads b2
	clock = clock.Add(config.Metadata.Retry.Backoff * 3 / 5)
	if got, ok := fetch(); !ok || got != 42 {
		t.Errorf("fetch after the backoff got %d (found %v), want 42 from b2", got, ok)
	}
	if got := refreshes(); got != 2 {
		t.Errorf("got %d refreshes in all, want 2", got)
	}
}

// Two fetches began before the same refresh, and both retries found the broker
// that failed. The later one, finishing after a slow request, must not restart
// the backoff that the first one started.
func TestFetchesBeforeOneRefreshStartOneBackoff(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	config.Metadata.Retry.Backoff = 10 * time.Second
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).SetLeader("t", 0, mock.BrokerID()),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	meta := newCollectMetadata(client)
	// far from the real time, so a real clock anywhere in the check shows up
	clock := time.Unix(1_000_000, 0)
	meta.now = func() time.Time { return clock }
	batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
	batch.add(client.Brokers()[0], "t", 0)

	began := meta.generations([]*offsetBatch{batch})
	meta.refresh([]string{"t"}, began)
	lookedUp := meta.generationsOf([]string{"t"})
	meta.markDisagrees("t", began["t"], lookedUp["t"])
	clock = clock.Add(config.Metadata.Retry.Backoff * 3 / 5)
	meta.markDisagrees("t", began["t"], lookedUp["t"])
	clock = clock.Add(config.Metadata.Retry.Backoff * 3 / 5)

	before := metadataRequestsFor(mock, "t")
	meta.refresh([]string{"t"}, meta.generations([]*offsetBatch{batch}))
	if got := metadataRequestsFor(mock, "t") - before; got != 1 {
		t.Errorf("got %d refreshes once the backoff from the refresh had passed, want 1", got)
	}
}

// A fetch's retry looked its leaders up after its refresh, then another fetch
// refreshed "t" again before the first one recorded that metadata disagrees.
// The newer metadata may name another leader, so the first fetch must not start
// a backoff that blocks the next failing fetch's refresh.
func TestRetryLookedUpBeforeAnotherRefreshStartsNoBackoff(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	config.Metadata.Retry.Backoff = 10 * time.Second
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).SetLeader("t", 0, mock.BrokerID()),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	meta := newCollectMetadata(client)
	clock := time.Unix(1_000_000, 0)
	meta.now = func() time.Time { return clock }
	topics := []string{"t"}

	began := meta.generationsOf(topics)
	meta.refresh(topics, began)
	lookedUp := meta.generationsOf(topics)
	// another fetch refreshes between the lookups and the mark
	meta.refresh(topics, meta.generationsOf(topics))
	meta.markDisagrees("t", began["t"], lookedUp["t"])

	before := metadataRequestsFor(mock, "t")
	meta.refresh(topics, meta.generationsOf(topics))
	if got := metadataRequestsFor(mock, "t") - before; got != 1 {
		t.Errorf("got %d refreshes for a fetch failing on the newer metadata, want 1: a backoff was started from older metadata", got)
	}
}

// One fetch's refresh of "t" and "kept" finds t deleted. Another group's leader
// check that runs after the refresh, before the first fetch's retry marks t
// deleted, must not pay for its own refresh (sarama retries an unknown topic's
// metadata); "kept", refreshed in the same request, is not deleted.
func TestRefreshThatFindsATopicDeletedMarksIt(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 3
	config.Metadata.Retry.Backoff = time.Millisecond
	withTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).
		SetLeader("t", 0, mock.BrokerID()).
		SetLeader("kept", 0, mock.BrokerID())
	withoutTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).
		SetLeader("kept", 0, mock.BrokerID())
	mock.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": sarama.NewMockSequence(withTopic, withoutTopic)})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	meta := newCollectMetadata(client)
	if !meta.hasLeader("t", 0) {
		t.Fatalf("t/0 has no leader before the deletion")
	}

	topics := []string{"t", "kept"}
	meta.refresh(topics, meta.generationsOf(topics))
	if !meta.isDeleted("t") {
		t.Errorf("the refresh found t gone but did not mark it deleted")
	}
	if meta.isDeleted("kept") || !meta.hasLeader("kept", 0) {
		t.Errorf("kept, still in metadata, was marked deleted or lost its leader")
	}
	before := metadataRequestsFor(mock, "t")
	if meta.hasLeader("t", 0) {
		t.Errorf("leader check reported a leader for a deleted topic")
	}
	if got := metadataRequestsFor(mock, "t") - before; got != 0 {
		t.Errorf("leader check after the refresh sent %d metadata requests for a deleted topic, want 0", got)
	}
}

// "t" is deleted after the walk lists it and before the walk's offset fetch.
// No group runs, so only the fetch's refresh learns of the deletion; it must
// log it once, or the runbook's "no longer exists" check finds nothing.
func TestTopicDeletedBeforeTheWalkFetchIsLoggedOnce(t *testing.T) {
	var buf bytes.Buffer
	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	for name, value := range map[string]string{"logtostderr": "false", "alsologtostderr": "false", "stderrthreshold": "FATAL", "v": "0"} {
		if err := fs.Set(name, value); err != nil {
			t.Fatalf("set klog flag %s: %v", name, err)
		}
	}
	// every line once: the INFO stream gets every severity
	klog.SetOutput(io.Discard)
	klog.SetOutputBySeverity("INFO", &buf)
	t.Cleanup(func() {
		klog.Flush()
		_ = fs.Set("logtostderr", "true")
		_ = fs.Set("stderrthreshold", "ERROR")
		klog.SetOutput(nil)
	})

	initTopicMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	withTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).
		SetLeader("t", 0, mock.BrokerID()).
		SetLeader("t", 1, mock.BrokerID())
	withoutTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID())
	gone := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	gone.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {
		0: {Err: sarama.ErrUnknownTopicOrPartition},
		1: {Err: sarama.ErrUnknownTopicOrPartition},
	}}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":   sarama.NewMockSequence(withTopic, withoutTopic),
		"OffsetRequest":     sarama.NewMockWrapper(gone),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t),
	})
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
	ch := make(chan prometheus.Metric, 1000)
	e.collect(ch)
	drain(ch)
	klog.Flush()

	if got := strings.Count(buf.String(), "Topic t no longer exists"); got != 1 {
		t.Errorf("got %d \"Topic t no longer exists\" lines, want 1; log:\n%s", got, buf.String())
	}
}

// "t" grew a partition after the exporter cached its metadata, and the group's
// assignment (offset.show-all=false) already includes it. Partition 1 is not
// leaderless, just unknown to the cache: its lag must still be reported, as
// client.GetOffset's refresh on a cache miss did.
func TestGroupLagReadsAPartitionTheCacheDoesNotKnowYet(t *testing.T) {
	initGroupMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	onePartition := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).
		SetLeader("t", 0, mock.BrokerID())
	twoPartitions := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).
		SetLeader("t", 0, mock.BrokerID()).
		SetLeader("t", 1, mock.BrokerID())
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockSequence(onePartition, twoPartitions),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("g", "t", 0, 5, "", sarama.ErrNoError).
			SetOffset("g", "t", 1, 5, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("t", 0, sarama.OffsetNewest, 10).
			SetOffset("t", 1, sarama.OffsetNewest, 10),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if partitions, _ := client.Partitions("t"); len(partitions) != 1 {
		t.Fatalf("cache has partitions %v, want only 0", partitions)
	}

	e := &Exporter{client: client, offsetShowAll: false}
	group := consumerGroupInfo{id: "g", memberCount: 1, currentAssignments: map[string]map[int32]struct{}{"t": {0: {}, 1: {}}}}
	ch := make(chan prometheus.Metric, 100)
	e.emitGroupMetricsIn(newCollectMetadata(client), group, sarama.NewBroker(mock.Addr()), nil, ch)

	metrics := drain(ch)
	if got := gaugesByPartition(t, metrics, consumergroupLag); len(got) != 2 || got["1"] != 5 {
		t.Errorf("lag by partition %v, want 5 for both 0 and 1", got)
	}
	for _, m := range metrics {
		if m.Desc() != consumergroupLagSum {
			continue
		}
		var out dto.Metric
		if err := m.Write(&out); err != nil {
			t.Fatalf("write metric: %v", err)
		}
		if got := out.GetGauge().GetValue(); got != 10 {
			t.Errorf("lag sum %v, want 10", got)
		}
	}
}

// The group is assigned partitions 2 and 3 of "t", which Kafka's metadata never
// lists. The leader check refreshes "t" once for them, not once per partition,
// and skips them.
func TestGroupLagRefreshesOnceForPartitionsMetadataNeverLists(t *testing.T) {
	initGroupMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).
			SetLeader("t", 0, mock.BrokerID()).
			SetLeader("t", 1, mock.BrokerID()),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("g", "t", 0, 5, "", sarama.ErrNoError).
			SetOffset("g", "t", 1, 5, "", sarama.ErrNoError).
			SetOffset("g", "t", 2, 5, "", sarama.ErrNoError).
			SetOffset("g", "t", 3, 5, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("t", 0, sarama.OffsetNewest, 10).
			SetOffset("t", 1, sarama.OffsetNewest, 10),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	before := metadataRequestsFor(mock, "t")

	e := &Exporter{client: client, offsetShowAll: false}
	assigned := map[int32]struct{}{0: {}, 1: {}, 2: {}, 3: {}}
	group := consumerGroupInfo{id: "g", memberCount: 1, currentAssignments: map[string]map[int32]struct{}{"t": assigned}}
	ch := make(chan prometheus.Metric, 100)
	e.emitGroupMetricsIn(newCollectMetadata(client), group, sarama.NewBroker(mock.Addr()), nil, ch)

	if got := gaugesByPartition(t, drain(ch), consumergroupLag); len(got) != 2 {
		t.Errorf("lag by partition %v, want partitions 0 and 1 only", got)
	}
	if got := metadataRequestsFor(mock, "t") - before; got != 1 {
		t.Errorf("got %d metadata refreshes of t for 2 unlisted partitions, want 1", got)
	}
}

// The group has offsets for "gone", which Kafka's metadata does not list. The
// leader check must not read the partitions of a topic it could not find (a
// sarama cache miss refreshes): one refresh for the leader check and one for
// the leader lookup that confirms the deletion.
func TestGroupLagDoesNotReadPartitionsOfAnUnknownTopic(t *testing.T) {
	initGroupMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":    sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset("g", "gone", 0, 5, "", sarama.ErrNoError),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	e := &Exporter{client: client, offsetShowAll: false}
	group := consumerGroupInfo{id: "g", memberCount: 1, currentAssignments: map[string]map[int32]struct{}{"gone": {0: {}}}}
	ch := make(chan prometheus.Metric, 100)
	e.emitGroupMetricsIn(newCollectMetadata(client), group, sarama.NewBroker(mock.Addr()), nil, ch)
	drain(ch)

	if got := metadataRequestsFor(mock, "gone"); got != 2 {
		t.Errorf("got %d metadata requests for an unknown topic, want 2", got)
	}
}

// Partition 1 of "t" is leaderless in the cache. The leader check's refresh for
// it also brings in a new, leaderless partition 2. Partition 2 is now in the
// cache, so it is skipped as leaderless, without a refresh for an unknown one.
func TestLeaderCheckRefreshUpdatesTheKnownPartitions(t *testing.T) {
	initGroupMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	version := sarama.NewMetadataRequest(config.Version, nil).Version
	replicas := []int32{mock.BrokerID()}
	metadata := func(partitions int32) *sarama.MetadataResponse {
		m := &sarama.MetadataResponse{Version: version}
		m.AddBroker(mock.Addr(), mock.BrokerID())
		m.AddTopicPartition("t", 0, mock.BrokerID(), replicas, replicas, nil, sarama.ErrNoError)
		for p := int32(1); p < partitions; p++ {
			m.AddTopicPartition("t", p, -1, replicas, nil, replicas, sarama.ErrLeaderNotAvailable)
		}
		return m
	}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockSequence(sarama.NewMockWrapper(metadata(2)), sarama.NewMockWrapper(metadata(3))),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("g", "t", 0, 5, "", sarama.ErrNoError).
			SetOffset("g", "t", 1, 5, "", sarama.ErrNoError).
			SetOffset("g", "t", 2, 5, "", sarama.ErrNoError),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 10),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	before := metadataRequestsFor(mock, "t")

	e := &Exporter{client: client, offsetShowAll: false}
	group := consumerGroupInfo{id: "g", memberCount: 1, currentAssignments: map[string]map[int32]struct{}{"t": {0: {}, 1: {}, 2: {}}}}
	ch := make(chan prometheus.Metric, 100)
	e.emitGroupMetricsIn(newCollectMetadata(client), group, sarama.NewBroker(mock.Addr()), nil, ch)

	if got := gaugesByPartition(t, drain(ch), consumergroupLag); len(got) != 1 || got["0"] != 5 {
		t.Errorf("lag by partition %v, want 5 for partition 0 only", got)
	}
	if got := metadataRequestsFor(mock, "t") - before; got != 1 {
		t.Errorf("got %d metadata refreshes of t, want 1 (the leader check's)", got)
	}
}

// t/0 is led by b1. After a fetch from b1 begins, another group refreshes "t"
// (still b1); leadership then moves to b2, and b1 answers the fetch NOT_LEADER.
// The other refresh predates the move, so the fetch must refresh again rather
// than retry the cached b1 or start a backoff from that metadata.
func TestRefreshFromBeforeAFailureWasSeenDoesNotStopItsRefresh(t *testing.T) {
	tests := []struct {
		name string
		// the other group's refresh, after the fetch began
		between func(meta *collectMetadata)
	}{
		{name: "a failed fetch's refresh", between: func(meta *collectMetadata) {
			meta.refresh([]string{"t"}, meta.generationsOf([]string{"t"}))
		}},
		{name: "a leader check's refresh", between: func(meta *collectMetadata) {
			// a partition the cache doesn't list refreshes the topic
			meta.hasLeader("t", 5)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b1, b2 := sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2)
			t.Cleanup(b1.Close)
			t.Cleanup(b2.Close)
			config := sarama.NewConfig()
			config.ApiVersionsRequest = false
			// one request per refresh, so each metadata response is one refresh's
			config.Metadata.Retry.Max = 0
			config.Metadata.Retry.Backoff = 10 * time.Second
			metadata := func(leader *sarama.MockBroker) *sarama.MockMetadataResponse {
				return sarama.NewMockMetadataResponse(t).
					SetBroker(b1.Addr(), b1.BrokerID()).
					SetBroker(b2.Addr(), b2.BrokerID()).
					SetLeader("t", 0, leader.BrokerID())
			}
			// client start, the other group's refresh (before the move), any later one
			sequence := sarama.NewMockSequence(metadata(b1), metadata(b1), metadata(b2))
			stale := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
			stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
			b1.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": sequence, "OffsetRequest": sarama.NewMockWrapper(stale)})
			b2.SetHandlerByMap(map[string]sarama.MockResponse{
				"MetadataRequest": sequence,
				"OffsetRequest":   sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 42),
			})
			client, err := sarama.NewClient([]string{b1.Addr()}, config)
			if err != nil {
				t.Fatalf("create client: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })
			e := &Exporter{client: client}
			meta := newCollectMetadata(client)
			meta.now = func() time.Time { return time.Unix(1_000_000, 0) }
			meta.hasLeader("t", 0)
			leader, err := client.Leader("t", 0)
			if err != nil {
				t.Fatalf("leader of t/0: %v", err)
			}
			batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
			batch.add(leader, "t", 0)
			batches := []*offsetBatch{batch}

			// fetchOffsets, with the other refresh landing while the fetch is in flight
			began := meta.generations(batches)
			tt.between(meta)
			results := e.retryFailed(meta, batches, fetchAll(batches), began)

			if got, ok := results[0].offsets["t"][0]; !ok || got != 42 {
				t.Errorf("got offset %d (found %v), want 42 from b2", got, ok)
			}
			if !meta.topic("t").disagreedAt.IsZero() {
				t.Errorf("a backoff was started from metadata read before the failure")
			}
			if got := metadataRequestsFor(b1, "t") + metadataRequestsFor(b2, "t"); got != 2 {
				t.Errorf("got %d metadata refreshes of t, want 2 (the other group's and the fetch's)", got)
			}
		})
	}
}

// "t"'s broker b1 is down and metadata keeps naming it, so a first fetch starts
// a backoff. Within it, another group's leader check refreshes "t" while a
// second fetch is in flight. That refresh predates the second fetch's failure,
// so it must not restart the backoff: the first fetch after the original
// backoff refreshes.
func TestRefreshFromBeforeAFailureWasSeenDoesNotExtendTheBackoff(t *testing.T) {
	b1 := sarama.NewMockBroker(t, 1)
	t.Cleanup(b1.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	config.Metadata.Retry.Backoff = 10 * time.Second
	stale := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
	b1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).SetBroker(b1.Addr(), b1.BrokerID()).SetLeader("t", 0, b1.BrokerID()),
		"OffsetRequest":   sarama.NewMockWrapper(stale),
	})
	client, err := sarama.NewClient([]string{b1.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	e := &Exporter{client: client}
	meta := newCollectMetadata(client)
	clock := time.Unix(1_000_000, 0)
	meta.now = func() time.Time { return clock }
	newBatch := func() []*offsetBatch {
		leader, err := client.Leader("t", 0)
		if err != nil {
			t.Fatalf("leader of t/0: %v", err)
		}
		batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
		batch.add(leader, "t", 0)
		return []*offsetBatch{batch}
	}

	// refreshes, still names b1: the backoff starts
	e.fetchOffsets(meta, newBatch()...)
	if meta.topic("t").disagreedAt.IsZero() {
		t.Fatalf("the first fetch started no backoff")
	}
	clock = clock.Add(config.Metadata.Retry.Backoff * 3 / 5)
	batches := newBatch()
	began := meta.generations(batches)
	// another group's leader check, while the second fetch is in flight
	meta.hasLeader("t", 5)
	e.retryFailed(meta, batches, fetchAll(batches), began)

	clock = clock.Add(config.Metadata.Retry.Backoff * 3 / 5)
	before := metadataRequestsFor(b1, "t")
	e.fetchOffsets(meta, newBatch()...)
	if got := metadataRequestsFor(b1, "t") - before; got != 1 {
		t.Errorf("got %d refreshes once the first backoff had passed, want 1: the second fetch restarted the backoff from metadata read before its failure", got)
	}
}

// As above, but the other group's refresh already found the new leader b2: it
// postdates the move, so the fetch shares it and retries b2 without refreshing.
func TestRefreshThatFoundTheNewLeaderIsSharedWithAFetchInFlight(t *testing.T) {
	b1, b2 := sarama.NewMockBroker(t, 1), sarama.NewMockBroker(t, 2)
	t.Cleanup(b1.Close)
	t.Cleanup(b2.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	metadata := func(leader *sarama.MockBroker) *sarama.MockMetadataResponse {
		return sarama.NewMockMetadataResponse(t).
			SetBroker(b1.Addr(), b1.BrokerID()).
			SetBroker(b2.Addr(), b2.BrokerID()).
			SetLeader("t", 0, leader.BrokerID())
	}
	sequence := sarama.NewMockSequence(metadata(b1), metadata(b2))
	stale := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	stale.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrNotLeaderForPartition}}}
	b1.SetHandlerByMap(map[string]sarama.MockResponse{"MetadataRequest": sequence, "OffsetRequest": sarama.NewMockWrapper(stale)})
	b2.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sequence,
		"OffsetRequest":   sarama.NewMockOffsetResponse(t).SetOffset("t", 0, sarama.OffsetNewest, 42),
	})
	client, err := sarama.NewClient([]string{b1.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	e := &Exporter{client: client}
	meta := newCollectMetadata(client)
	leader, err := client.Leader("t", 0)
	if err != nil {
		t.Fatalf("leader of t/0: %v", err)
	}
	batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
	batch.add(leader, "t", 0)
	batches := []*offsetBatch{batch}

	began := meta.generations(batches)
	meta.refresh([]string{"t"}, meta.generationsOf([]string{"t"}))
	results := e.retryFailed(meta, batches, fetchAll(batches), began)

	if got, ok := results[0].offsets["t"][0]; !ok || got != 42 {
		t.Errorf("got offset %d (found %v), want 42 from b2", got, ok)
	}
	if got := metadataRequestsFor(b1, "t") + metadataRequestsFor(b2, "t"); got != 1 {
		t.Errorf("got %d metadata refreshes of t, want 1 (the other group's)", got)
	}
}

// "t" is deleted while a fetch is in flight, and another group's leader check
// drops it from the cache without marking it deleted. The fetch's retry must
// skip it from the cache, without a leader lookup that refreshes again.
func TestRetrySkipsATopicALeaderCheckDroppedFromTheCache(t *testing.T) {
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Metadata.Retry.Max = 0
	withTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID()).SetLeader("t", 0, mock.BrokerID())
	withoutTopic := sarama.NewMockMetadataResponse(t).SetBroker(mock.Addr(), mock.BrokerID())
	gone := &sarama.OffsetResponse{Version: sarama.NewOffsetRequest(config.Version).Version}
	gone.Blocks = map[string]map[int32]*sarama.OffsetResponseBlock{"t": {0: {Err: sarama.ErrUnknownTopicOrPartition}}}
	mock.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockSequence(withTopic, withoutTopic),
		"OffsetRequest":   sarama.NewMockWrapper(gone),
	})
	client, err := sarama.NewClient([]string{mock.Addr()}, config)
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	e := &Exporter{client: client}
	meta := newCollectMetadata(client)
	meta.hasLeader("t", 0)
	leader, err := client.Leader("t", 0)
	if err != nil {
		t.Fatalf("leader of t/0: %v", err)
	}
	batch := newOffsetBatch(config.Version, sarama.OffsetNewest)
	batch.add(leader, "t", 0)
	batches := []*offsetBatch{batch}

	began := meta.generations(batches)
	// a partition the cache doesn't list: the leader check refreshes "t" and finds it gone
	meta.hasLeader("t", 5)
	if meta.isDeleted("t") {
		t.Fatalf("the leader check marked t deleted; the test needs it unmarked")
	}
	before := metadataRequestsFor(mock, "t")
	results := e.retryFailed(meta, batches, fetchAll(batches), began)

	if got := metadataRequestsFor(mock, "t") - before; got != 0 {
		t.Errorf("retry sent %d metadata requests for a topic the cache no longer lists, want 0", got)
	}
	if !errors.Is(results[0].errs["t"], sarama.ErrUnknownTopicOrPartition) || !meta.isDeleted("t") {
		t.Errorf("t not reported deleted: err %v, marked %v", results[0].errs["t"], meta.isDeleted("t"))
	}
}
