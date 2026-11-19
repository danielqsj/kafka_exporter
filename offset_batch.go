package main

import (
	"errors"
	"slices"
	"sync"
	"time"

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
	// the broker each failed partition was asked
	failedOn map[string]map[int32]int32
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
		offsets:  make(map[string]map[int32]int64),
		errs:     make(map[string]error),
		failedOn: make(map[string]map[int32]int32),
	}
	var mu sync.Mutex
	setErr := func(topic string, partition, broker int32, err error) {
		if prev, ok := result.errs[topic]; !ok || (!errors.Is(prev, sarama.ErrUnknownTopicOrPartition) && errors.Is(err, sarama.ErrUnknownTopicOrPartition)) {
			result.errs[topic] = err
		}
		if result.failedOn[topic] == nil {
			result.failedOn[topic] = make(map[int32]int32)
		}
		result.failedOn[topic][partition] = broker
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
				for topic, partitions := range b.requested[id] {
					for _, partition := range partitions {
						setErr(topic, partition, id, err)
					}
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
					setErr(topic, partition, id, err)
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
// a failed partition unless it was refreshed since this fetch began (or since
// its failure was seen, if the cache still names the broker that failed), then
// asks the refreshed leaders again for the failed partitions of topics that
// still exist, as client.GetOffset retried after its refresh. A topic left in
// errs still has partitions without an offset.
func (e *Exporter) fetchOffsets(meta *collectMetadata, batches ...*offsetBatch) []offsetBatchResult {
	began := meta.generations(batches)
	return e.retryFailed(meta, batches, fetchAll(batches), began)
}

// retryFailed refreshes and retries the failed partitions of fetched batches;
// began holds each topic's generation from before the batches were sent.
func (e *Exporter) retryFailed(meta *collectMetadata, batches []*offsetBatch, results []offsetBatchResult, began map[string]int) []offsetBatchResult {
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
	// when the failures were seen: a refresh started before then may predate their cause
	observed := meta.generationsOf(failing)
	meta.refresh(failing, began)

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
	missing := make([]map[string][]int32, len(batches))
	for i, batch := range batches {
		missing[i] = batch.missing(results[i])
	}

	// The refresh above is skipped when another started after these batches were
	// sent, but that one may have read metadata from before the leader moved. If
	// the cache still names the broker that failed, refresh again, unless a
	// refresh has started since the failure was seen.
	var recheck []string
	for _, topic := range failing {
		if e.topicGone(topic, sarama.ErrUnknownTopicOrPartition) {
			continue
		}
		sameBroker := 0
		for i := range batches {
			for _, partition := range missing[i][topic] {
				failed, ok := results[i].failedOn[topic][partition]
				if !ok {
					continue
				}
				if leader := leaderOf(topic, partition); leader != nil {
					if leader.ID() == failed {
						sameBroker++
					}
				}
			}
		}
		// one stale leader is enough: the refresh may predate that partition's move
		if sameBroker > 0 {
			recheck = append(recheck, topic)
		}
	}
	if len(recheck) > 0 {
		meta.refresh(recheck, observed)
		for key := range leaders {
			if slices.Contains(recheck, key.topic) {
				delete(leaders, key)
			}
		}
		for _, topic := range recheck {
			delete(unresolved, topic)
		}
	}
	// the metadata the retry's leaders are looked up in
	lookedUp := meta.generationsOf(failing)

	retries := make([]*offsetBatch, len(batches))
	for i, batch := range batches {
		retries[i] = newOffsetBatch(batch.version, batch.timestamp)
		for topic, partitions := range missing[i] {
			if e.topicGone(topic, sarama.ErrUnknownTopicOrPartition) {
				// The failure may have said NOT_LEADER or EOF; callers recognise a
				// deleted topic by this error.
				results[i].errs[topic] = sarama.ErrUnknownTopicOrPartition
				meta.markDeleted(topic)
				continue
			}
			// cached partitions only, so an unknown one costs no further refresh
			cached, err := e.client.Partitions(topic)
			if err != nil {
				continue
			}
			known := make(map[int32]bool, len(cached))
			for _, partition := range cached {
				known[partition] = true
			}
			// still asking the broker that failed: metadata disagrees with it
			sameBroker := 0
			for _, partition := range partitions {
				if !known[partition] {
					continue
				}
				if leader := leaderOf(topic, partition); leader != nil {
					retries[i].add(leader, topic, partition)
					if failed, ok := results[i].failedOn[topic][partition]; ok && failed == leader.ID() {
						sameBroker++
					}
				}
			}
			if sameBroker > 0 {
				meta.markDisagrees(topic, observed[topic], lookedUp[topic])
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
// and every consumer group: a failing topic is refreshed only if no refresh
// happened since the failed fetch began and, once a refresh still named the
// broker that failed (metadata disagrees with the broker), not again until
// sarama's metadata retry backoff has passed, and
// which of its partitions have a leader is read once from the cache, because a
// leader lookup for a leaderless partition refreshes metadata every time. Locks
// are per topic, so a slow refresh of one topic does not hold up the others.
type collectMetadata struct {
	backoff time.Duration
	now     func() time.Time
	client  sarama.Client

	mu     sync.Mutex
	topics map[string]*topicMetadata
}

type topicMetadata struct {
	mu sync.Mutex
	// refreshes of this topic so far in the collect
	generation int
	// when a refresh, of generation disagreedGen, still named the broker that
	// failed: no refresh until the backoff has passed
	disagreedAt  time.Time
	disagreedGen int
	// the leader check refreshed: it does so at most once per collect
	leaderRefreshed bool
	// confirmed deleted in this collect: later groups skip it without lookups
	deleted bool
	read    bool
	// nil when the partitions could not be read: callers look the leader up as before
	withLeader map[int32]bool
	// every partition in the cache when withLeader was read
	known map[int32]bool
	// a partition missing from the cache refreshed the topic: at most once per collect
	unknownRefreshed bool
}

func newCollectMetadata(client sarama.Client) *collectMetadata {
	return &collectMetadata{
		backoff: client.Config().Metadata.Retry.Backoff,
		now:     time.Now,
		client:  client,
		topics:  make(map[string]*topicMetadata),
	}
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

// generations returns the refresh generation of every topic in the batches.
func (m *collectMetadata) generations(batches []*offsetBatch) map[string]int {
	var topics []string
	for _, batch := range batches {
		for _, requested := range batch.requested {
			for topic := range requested {
				topics = append(topics, topic)
			}
		}
	}
	return m.generationsOf(topics)
}

// generationsOf returns the refresh generation of each topic.
func (m *collectMetadata) generationsOf(topics []string) map[string]int {
	seen := make(map[string]int, len(topics))
	for _, topic := range topics {
		if _, ok := seen[topic]; !ok {
			t := m.topic(topic)
			t.mu.Lock()
			seen[topic] = t.generation
			t.mu.Unlock()
		}
	}
	return seen
}

// refresh refreshes, in one request, the topics not refreshed since the
// generation in seen and not found, within the backoff, to disagree with their
// brokers. Callers for
// the same topic wait for it, so the cache they read afterwards is fresh.
func (m *collectMetadata) refresh(topics []string, seen map[string]int) {
	names := slices.Clone(topics)
	slices.Sort(names)
	names = slices.Compact(names)
	var stale []string
	refreshed := make(map[string]*topicMetadata)
	for _, name := range names {
		// sorted, so concurrent callers lock in the same order
		t := m.topic(name)
		t.mu.Lock()
		defer t.mu.Unlock()
		if t.generation == seen[name] && (t.disagreedAt.IsZero() || m.now().Sub(t.disagreedAt) >= m.backoff) {
			t.generation++
			t.read = false
			stale = append(stale, name)
			refreshed[name] = t
		}
	}
	if len(stale) == 0 {
		return
	}
	err := m.client.RefreshMetadata(stale...)
	if err == nil {
		return
	}
	klog.Errorf("Cannot refresh metadata of topics %v: %v", stale, err)
	// The retry treats a topic the refresh left out of the cache as deleted
	// (topicGone); mark it here, while the locks are held, so a leader check
	// waiting on one skips the topic instead of refreshing it again.
	if known, err := m.client.Topics(); err == nil {
		for name, t := range refreshed {
			if !t.deleted && !slices.Contains(known, name) {
				klog.Warningf("Topic %s no longer exists, skipping it for the rest of this scrape", name)
				t.deleted = true
			}
		}
	}
}

// markDisagrees records that metadata refreshed after the failure was seen (at
// generation seen) still names the broker that failed, as looked up at
// generation lookedUp. A retry that used only metadata from before the failure
// is no new evidence, and does not restart the backoff; nor is one whose
// metadata was refreshed again after its lookups, since the newer metadata may
// name another leader.
func (m *collectMetadata) markDisagrees(topic string, seen, lookedUp int) {
	t := m.topic(topic)
	t.mu.Lock()
	defer t.mu.Unlock()
	if lookedUp != seen && t.generation == lookedUp && t.disagreedGen != lookedUp {
		t.disagreedGen = t.generation
		t.disagreedAt = m.now()
	}
}

// hasLeader reports whether the cached metadata gives the partition a leader.
// Kafka marks a leaderless partition LEADER_NOT_AVAILABLE, and
// WritablePartitions leaves those out from the cache. A topic with some
// leaderless partitions is refreshed once per collect before they are skipped,
// so a leader elected since is used, as client.GetOffset's refresh did; so is a
// topic asked about a partition the cache doesn't list at all. Neither refresh
// uses up refresh's, which a failed fetch still needs.
func (m *collectMetadata) hasLeader(topic string, partition int32) bool {
	t := m.topic(topic)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.deleted {
		return false
	}
	if !t.read {
		t.read = true
		var refreshed bool
		t.withLeader, refreshed = m.readWithLeader(topic)
		t.leaderRefreshed = t.leaderRefreshed || refreshed
		if refreshed {
			t.generation++
		}
		t.known = m.cachedPartitions(topic, t.withLeader)
		if t.withLeader != nil && !t.leaderRefreshed && t.known != nil && len(t.withLeader) < len(t.known) {
			t.leaderRefreshed = true
			m.refreshForLeaderCheck(t, topic)
		}
	}
	// Not leaderless, just missing from the cache: the topic may have gained
	// partitions since it was cached, and a consumer group can already be
	// assigned them. Refresh once, as client.GetOffset's cache miss did.
	if t.known != nil && !t.withLeader[partition] && !t.known[partition] && !t.unknownRefreshed {
		t.unknownRefreshed = true
		m.refreshForLeaderCheck(t, topic)
	}
	return !t.deleted && (t.withLeader == nil || t.withLeader[partition])
}

// refreshForLeaderCheck refreshes topic and re-reads its partitions; the
// caller holds t.mu.
func (m *collectMetadata) refreshForLeaderCheck(t *topicMetadata, topic string) {
	t.generation++
	if err := m.client.RefreshMetadata(topic); err != nil && !errors.Is(err, sarama.ErrLeaderNotAvailable) {
		klog.Errorf("Cannot refresh metadata of topic %s: %v", topic, err)
	}
	t.withLeader, _ = m.readWithLeader(topic)
	t.known = m.cachedPartitions(topic, t.withLeader)
}

// cachedPartitions returns the partitions of topic in the cache, or nil if
// they cannot be read or withLeader is nil (the topic is unknown, and callers
// look leaders up instead).
func (m *collectMetadata) cachedPartitions(topic string, withLeader map[int32]bool) map[int32]bool {
	if withLeader == nil {
		return nil
	}
	all, err := m.client.Partitions(topic)
	if err != nil {
		return nil
	}
	known := make(map[int32]bool, len(all))
	for _, partition := range all {
		known[partition] = true
	}
	return known
}

// readWithLeader returns the partitions of topic that have a leader in the
// cache, nil if the topic is unknown, and whether reading refreshed metadata.
func (m *collectMetadata) readWithLeader(topic string) (map[int32]bool, bool) {
	writable, err := m.client.WritablePartitions(topic)
	// WritablePartitions refreshes when the cache has no leader for the topic,
	// and that refresh fails while any partition is still leaderless, even if
	// others recovered: read once more, from the cache unless none recovered.
	refreshed := errors.Is(err, sarama.ErrLeaderNotAvailable)
	if refreshed {
		writable, err = m.client.WritablePartitions(topic)
	}
	if errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
		// possibly deleted: callers look the leader up, which detects that
		return nil, refreshed
	}
	if err != nil && !errors.Is(err, sarama.ErrLeaderNotAvailable) {
		// Metadata is unreachable (no broker answered): WritablePartitions already
		// tried to refresh. A leader lookup per partition would retry that refresh
		// each time; skip the topic instead.
		klog.Errorf("Cannot read the leaders of topic %s: %v", topic, err)
		refreshed = true
	}
	withLeader := make(map[int32]bool, len(writable))
	for _, p := range writable {
		withLeader[p] = true
	}
	return withLeader, refreshed
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
