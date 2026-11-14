package main

import (
	"slices"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog/v2"
)

// topicConfigBatchSize is the number of topics described in one DescribeConfigs request.
const topicConfigBatchSize = 100

// topicConfigDescriber is the part of sarama.ClusterAdmin used to read topic configs.
type topicConfigDescriber interface {
	DescribeConfigs(resources []*sarama.ConfigResource, options sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error)
}

// normalizeTopicConfigs flattens repeated and comma-separated --topic.config
// values into a sorted list without blanks and duplicates.
func normalizeTopicConfigs(values []string) []string {
	var names []string
	for _, value := range values {
		for _, name := range strings.Split(value, ",") {
			if name = strings.TrimSpace(name); name != "" {
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// collectTopicConfigs exports the configured topic configurations of the topics
// that pass the topic filters. Numeric values become kafka_topic_config, other
// values (cleanup.policy, compression.type, ...) become kafka_topic_config_info.
// A failure is logged and skipped, it never fails the whole scrape.
func (e *Exporter) collectTopicConfigs(ch chan<- prometheus.Metric, topics []string) {
	var selected []string
	for _, topic := range topics {
		if e.topicFilter.MatchString(topic) && !e.topicExclude.MatchString(topic) {
			selected = append(selected, topic)
		}
	}

	for batch := range slices.Chunk(selected, topicConfigBatchSize) {
		resources := make([]*sarama.ConfigResource, 0, len(batch))
		for _, topic := range batch {
			resources = append(resources, &sarama.ConfigResource{
				Type:        sarama.TopicResource,
				Name:        topic,
				ConfigNames: e.topicConfigs,
			})
		}

		results, err := e.admin.DescribeConfigs(resources, sarama.DescribeConfigsOptions{})
		if err != nil {
			klog.Errorf("Cannot describe configs of %d topics: %v", len(resources), err)
			continue
		}

		for _, result := range results {
			if result.ErrorCode != sarama.ErrNoError {
				klog.Errorf("Cannot describe configs of topic %s: %v %s", result.Name, result.ErrorCode, result.ErrorMsg)
				continue
			}
			for _, entry := range result.Configs {
				e.emitTopicConfig(ch, result.Name, entry)
			}
		}
	}
}

func (e *Exporter) emitTopicConfig(ch chan<- prometheus.Metric, topic string, entry sarama.ConfigEntry) {
	// Sensitive values are never returned by the broker, an empty value is "unset".
	if entry.Sensitive || entry.Value == "" || !slices.Contains(e.topicConfigs, entry.Name) {
		return
	}
	if value, err := strconv.ParseFloat(entry.Value, 64); err == nil {
		ch <- prometheus.MustNewConstMetric(topicConfig, prometheus.GaugeValue, value, topic, entry.Name)
		return
	}
	ch <- prometheus.MustNewConstMetric(topicConfigInfo, prometheus.GaugeValue, 1, topic, entry.Name, entry.Value)
}
