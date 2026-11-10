package main

import (
	"errors"
	"slices"
	"sync"

	"github.com/IBM/sarama"
	"k8s.io/klog/v2"
)

// offsetBatch collects partitions by leader broker and fetches their offsets
// with one ListOffsets request per broker, instead of one per partition.
type offsetBatch struct {
	version   sarama.KafkaVersion
	timestamp int64

	mu        sync.Mutex
	brokers   map[int32]*sarama.Broker
	requests  map[int32]*sarama.OffsetRequest
	requested map[int32]map[string][]int32
}

type offsetBatchResult struct {
	offsets map[string]map[int32]int64
	// The first error seen per topic. sarama.ErrUnknownTopicOrPartition wins,
	// since it is the one that can mean the topic was deleted.
	errs map[string]error
}

func newOffsetBatch(version sarama.KafkaVersion, timestamp int64) *offsetBatch {
	return &offsetBatch{
		version:   version,
		timestamp: timestamp,
		brokers:   make(map[int32]*sarama.Broker),
		requests:  make(map[int32]*sarama.OffsetRequest),
		requested: make(map[int32]map[string][]int32),
	}
}

func (b *offsetBatch) add(leader *sarama.Broker, topic string, partition int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	id := leader.ID()
	request, ok := b.requests[id]
	if !ok {
		// Same version client.GetOffset uses; Kafka 4.0 rejects ListOffsets v0.
		request = sarama.NewOffsetRequest(b.version)
		b.requests[id] = request
		b.brokers[id] = leader
		b.requested[id] = make(map[string][]int32)
	}
	request.AddBlock(topic, partition, b.timestamp, 1)
	b.requested[id][topic] = append(b.requested[id][topic], partition)
}

// fetch sends the requests to all brokers in parallel. A partition without an
// offset in the result has its topic's error in errs.
func (b *offsetBatch) fetch() offsetBatchResult {
	result := offsetBatchResult{
		offsets: make(map[string]map[int32]int64),
		errs:    make(map[string]error),
	}
	var mu sync.Mutex
	setErr := func(topic string, err error) {
		if prev, ok := result.errs[topic]; !ok || (!errors.Is(prev, sarama.ErrUnknownTopicOrPartition) && errors.Is(err, sarama.ErrUnknownTopicOrPartition)) {
			result.errs[topic] = err
		}
	}

	var wg sync.WaitGroup
	for id, request := range b.requests {
		broker := b.brokers[id]
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := broker.GetAvailableOffsets(request)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				klog.Errorf("Cannot get offsets from broker %d: %v", id, err)
				// as client.GetOffset does, so the next request reconnects
				_ = broker.Close()
				for topic := range b.requested[id] {
					setErr(topic, err)
				}
				return
			}
			type failure struct {
				count int
				err   error
			}
			failed := make(map[string]*failure)
			for topic, partitions := range b.requested[id] {
				for _, partition := range partitions {
					block := response.GetBlock(topic, partition)
					switch {
					case block == nil:
						err = sarama.ErrIncompleteResponse
					case !errors.Is(block.Err, sarama.ErrNoError):
						err = block.Err
					case len(block.Offsets) == 0:
						err = sarama.ErrIncompleteResponse
					default:
						if result.offsets[topic] == nil {
							result.offsets[topic] = make(map[int32]int64)
						}
						result.offsets[topic][partition] = block.Offsets[0]
						continue
					}
					klog.V(DEBUG).Infof("Cannot get offset of topic %s partition %d from broker %d: %v", topic, partition, id, err)
					if failed[topic] == nil {
						failed[topic] = &failure{err: err}
					}
					failed[topic].count++
					setErr(topic, err)
				}
			}
			for topic, f := range failed {
				klog.Errorf("Cannot get offsets of %d partitions of topic %s from broker %d: %v", f.count, topic, id, f.err)
			}
		}()
	}
	wg.Wait()
	return result
}

// fetchOffsets fetches the batches, refreshes the metadata of every topic with
// a failed partition (once per collect), then asks the refreshed leaders again for the failed
// partitions of topics that still exist, as client.GetOffset retried after its
// refresh. A topic left in errs still has partitions without an offset.
func (e *Exporter) fetchOffsets(meta *collectMetadata, batches ...*offsetBatch) []offsetBatchResult {
	results := fetchAll(batches)

	var failing []string
	seen := make(map[string]bool)
	for _, result := range results {
		for topic := range result.errs {
			if !seen[topic] {
				seen[topic] = true
				failing = append(failing, topic)
			}
		}
	}
	if len(failing) == 0 {
		return results
	}
	meta.refresh(failing)

	// A leader lookup that misses the cache refreshes metadata itself, so each
	// partition is looked up once for all batches, and a topic is given up at its
	// first failed lookup: a leaderless topic costs one refresh, not one per partition.
	type topicPartition struct {
		topic     string
		partition int32
	}
	leaders := make(map[topicPartition]*sarama.Broker)
	unresolved := make(map[string]bool)
	leaderOf := func(topic string, partition int32) *sarama.Broker {
		key := topicPartition{topic, partition}
		if leader, ok := leaders[key]; ok {
			return leader
		}
		if unresolved[topic] || !meta.hasLeader(topic, partition) {
			return nil
		}
		leader, err := e.client.Leader(topic, partition)
		if err != nil {
			klog.V(DEBUG).Infof("Cannot get leader of topic %s partition %d after refresh, not retrying the topic: %v", topic, partition, err)
			unresolved[topic] = true
			return nil
		}
		leaders[key] = leader
		return leader
	}

	retries := make([]*offsetBatch, len(batches))
	for i, batch := range batches {
		retries[i] = newOffsetBatch(batch.version, batch.timestamp)
		for topic, partitions := range batch.missing(results[i]) {
			if e.topicGone(topic, sarama.ErrUnknownTopicOrPartition) {
				// The failure may have said NOT_LEADER or EOF; callers recognise a
				// deleted topic by this error.
				results[i].errs[topic] = sarama.ErrUnknownTopicOrPartition
				meta.markDeleted(topic)
				continue
			}
			// cached partitions only, so an unknown one costs no further refresh
			known, err := e.client.Partitions(topic)
			if err != nil {
				continue
			}
			for _, partition := range partitions {
				if !slices.Contains(known, partition) {
					continue
				}
				if leader := leaderOf(topic, partition); leader != nil {
					retries[i].add(leader, topic, partition)
				}
			}
		}
	}
	for i, retried := range fetchAll(retries) {
		results[i].merge(batches[i], retried)
	}
	return results
}

func fetchAll(batches []*offsetBatch) []offsetBatchResult {
	results := make([]offsetBatchResult, len(batches))
	var wg sync.WaitGroup
	for i, batch := range batches {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = batch.fetch()
		}()
	}
	wg.Wait()
	return results
}

// missing lists the requested partitions of each failing topic that have no offset.
func (b *offsetBatch) missing(result offsetBatchResult) map[string][]int32 {
	missing := make(map[string][]int32)
	for _, topics := range b.requested {
		for topic, partitions := range topics {
			if _, failed := result.errs[topic]; !failed {
				continue
			}
			for _, partition := range partitions {
				if _, ok := result.offsets[topic][partition]; !ok {
					missing[topic] = append(missing[topic], partition)
				}
			}
		}
	}
	return missing
}

// merge adds a retry's offsets and keeps an error only for topics that still
// have a partition without an offset; the retry's error is the newer one.
func (r *offsetBatchResult) merge(original *offsetBatch, retried offsetBatchResult) {
	for topic, partitions := range retried.offsets {
		if r.offsets[topic] == nil {
			r.offsets[topic] = make(map[int32]int64)
		}
		for partition, offset := range partitions {
			r.offsets[topic][partition] = offset
		}
	}
	stillMissing := original.missing(*r)
	for topic := range r.errs {
		if _, ok := stillMissing[topic]; !ok {
			delete(r.errs, topic)
		} else if err, ok := retried.errs[topic]; ok {
			r.errs[topic] = err
		}
	}
}

// collectMetadata is one collect's view of metadata, shared by the topic walk
// and every consumer group: a topic is refreshed at most once per collect, and
// which of its partitions have a leader is read once from the cache, because a
// leader lookup for a leaderless partition refreshes metadata every time. Locks
// are per topic, so a slow refresh of one topic does not hold up the others.
type collectMetadata struct {
	client sarama.Client

	mu     sync.Mutex
	topics map[string]*topicMetadata
}

type topicMetadata struct {
	mu        sync.Mutex
	refreshed bool
	// confirmed deleted in this collect: later groups skip it without lookups
	deleted bool
	read    bool
	// nil when the partitions could not be read: callers look the leader up as before
	withLeader map[int32]bool
}

func newCollectMetadata(client sarama.Client) *collectMetadata {
	return &collectMetadata{client: client, topics: make(map[string]*topicMetadata)}
}

func (m *collectMetadata) topic(name string) *topicMetadata {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.topics[name]
	if !ok {
		t = &topicMetadata{}
		m.topics[name] = t
	}
	return t
}

// refresh refreshes the topics not yet refreshed in this collect, in one
// request. Callers for the same topic wait for it, so the cache they read
// afterwards is fresh.
func (m *collectMetadata) refresh(topics []string) {
	names := slices.Clone(topics)
	slices.Sort(names)
	names = slices.Compact(names)
	var stale []string
	for _, name := range names {
		// sorted, so concurrent callers lock in the same order
		t := m.topic(name)
		t.mu.Lock()
		defer t.mu.Unlock()
		if !t.refreshed {
			t.refreshed = true
			t.read = false
			stale = append(stale, name)
		}
	}
	if len(stale) == 0 {
		return
	}
	if err := m.client.RefreshMetadata(stale...); err != nil {
		klog.Errorf("Cannot refresh metadata of topics %v: %v", stale, err)
	}
}

// hasLeader reports whether the cached metadata gives the partition a leader.
// Kafka marks a leaderless partition LEADER_NOT_AVAILABLE, and
// WritablePartitions leaves those out from the cache; it refreshes only when
// the topic has no partition with a leader at all.
func (m *collectMetadata) hasLeader(topic string, partition int32) bool {
	t := m.topic(topic)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.read {
		t.read = true
		t.withLeader = nil
		writable, err := m.client.WritablePartitions(topic)
		if errors.Is(err, sarama.ErrLeaderNotAvailable) {
			// A topic with no leader in the cache is refreshed, and that refresh fails
			// while any partition is still leaderless, even if others recovered: read
			// once more, from the cache unless none recovered.
			writable, err = m.client.WritablePartitions(topic)
		}
		if err == nil || errors.Is(err, sarama.ErrLeaderNotAvailable) {
			t.withLeader = make(map[int32]bool, len(writable))
			for _, p := range writable {
				t.withLeader[p] = true
			}
		}
	}
	return !t.deleted && (t.withLeader == nil || t.withLeader[partition])
}

func (m *collectMetadata) markDeleted(topic string) {
	t := m.topic(topic)
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleted = true
}

func (m *collectMetadata) isDeleted(topic string) bool {
	t := m.topic(topic)
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.deleted
}
