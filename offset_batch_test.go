package main

import (
	"errors"
	"regexp"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

// metadataRequestsForT counts the metadata requests that name topic "t".
func metadataRequestsForT(mock *sarama.MockBroker) int {
	var n int
	for _, rr := range mock.History() {
		if req, ok := rr.Request.(*sarama.MetadataRequest); ok && slices.Contains(req.Topics, "t") {
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
	got, limit := metadataRequestsForT(mock), 2*(config.Metadata.Retry.Max+1)
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
	if got, limit := metadataRequestsForT(mock), 2*(config.Metadata.Retry.Max+1); got > limit {
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
	if got, limit := metadataRequestsForT(mock), 2*(config.Metadata.Retry.Max+1); got > limit {
		t.Errorf("leaderless topic cost %d metadata requests in group lag, want at most %d", got, limit)
	}
}

// The topic walk reads "t" fine, then every group's newest-offset request for it
// fails. The collect refreshes "t" once, not once per group.
func TestCollectRefreshesAFailingTopicOncePerCollect(t *testing.T) {
	initTopicMetricDescs()
	initGroupMetricDescs()
	mock := sarama.NewMockBroker(t, 0)
	t.Cleanup(mock.Close)
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
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
	if got := metadataRequestsForT(mock); got != 1 {
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
	if got, limit := metadataRequestsForT(mock), config.Metadata.Retry.Max+1; got == 0 || got > limit {
		t.Errorf("deleted topic cost %d metadata requests across %d groups, want 1 to %d", got, len(groups), limit)
	}
}
