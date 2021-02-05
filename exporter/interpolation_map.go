package exporter

import (
	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"sort"
	"sync"
	"time"
)

type interpolationMap struct {
	iMap map[string]map[string]map[int32]map[int64]time.Time
	mu   sync.Mutex
}

// Prune removes any entries from the Interpolation map that are not returned by the
// ClusterAdmin. An example would be when a consumer group or topic has been deleted
// from the cluster, the Interpolation map may still have cached offsets. Any partition
// that contains more offset entries than maxNumberOfOffsets will have the oldest
// offsets pruned
func (i *interpolationMap) Prune(logger log.Logger, client sarama.Client, maxOffsets int) {
	level.Debug(logger).Log("msg", "pruning iMap data", "maxOffsets", maxOffsets)
	if i.iMap == nil {
		level.Info(logger).Log("msg", "Interpolation map is nil, nothing to prune")
		return
	}
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		level.Error(logger).Log("msg", "Error creating cluster admin", "err", err.Error())
	}
	if admin == nil {
		level.Error(logger).Log("msg", "Failed to create cluster admin")
		return
	}

	defer admin.Close()

	groupsMap, err := admin.ListConsumerGroups()
	groupKeys := make([]string, len(groupsMap))
	for group, _ := range groupsMap {
		groupKeys = append(groupKeys, group)
	}

	topicsMap, err := admin.ListTopics()
	topicKeys := make([]string, len(topicsMap))
	for topic, _ := range topicsMap {
		topicKeys = append(topicKeys, topic)
	}

	i.mu.Lock()
	level.Debug(logger).Log("msg", "iMap locked for pruning")
	start := time.Now()

	for group, _ := range i.iMap {
		if !contains(groupKeys, group) {
			delete(i.iMap, group)
			continue
		}
		for topic, partitions := range i.iMap[group] {
			if !contains(topicKeys, topic) {
				delete(i.iMap[group], topic)
				continue
			}
			for partition, offsets := range partitions {
				if len(offsets) > maxOffsets {
					offsetKeys := make([]int64, len(offsets))
					for offset, _ := range offsets {
						offsetKeys = append(offsetKeys, offset)
					}
					sort.Slice(offsetKeys, func(i, j int) bool { return offsetKeys[i] < offsetKeys[j] })
					offsetKeys = offsetKeys[0 : len(offsetKeys)-maxOffsets]
					level.Debug(logger).Log("msg", "pruning offsets", "count", len(offsetKeys), "group", group, "topic", topic, "partition", partition)
					for _, offsetToRemove := range offsetKeys {
						delete(i.iMap[group][topic][partition], offsetToRemove)
					}
				}
			}
		}
	}
	level.Debug(logger).Log("msg", "pruning complete", "duration", time.Since(start).String())
	i.mu.Unlock()
}

// Lazily create the interpolation map as we see new group/topic/partition/offset
func (i *interpolationMap) createOrUpdate(group, topic string, partition int32, offset int64) {
	i.mu.Lock()
	if i.iMap == nil {
		i.iMap = make(map[string]map[string]map[int32]map[int64]time.Time)
	}
	if fetchedGroup, ok := i.iMap[group]; ok {
		if fetchedTopic, ok := fetchedGroup[topic]; ok {
			if fetchedPartition, ok := fetchedTopic[partition]; ok {
				fetchedPartition[offset] = time.Now()
			} else {
				fetchedTopic[partition] = make(map[int64]time.Time)
			}
		} else {
			fetchedGroup[topic] = make(map[int32]map[int64]time.Time)
		}
	} else {
		i.iMap[group] = make(map[string]map[int32]map[int64]time.Time)
	}
	i.mu.Unlock()
}

func contains(keys []string, v string) bool {
	for _, k := range keys {
		if k == v {
			return true
		}
	}
	return false
}
