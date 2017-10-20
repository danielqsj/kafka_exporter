package main

import (
	"fmt"
	"net/http"

	"strconv"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "kafka"
)

var (
	clusterBrokers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "brokers"),
		"Number of Brokers in the Kafka Cluster.",
		nil, nil,
	)

	topicPartitions = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partitions"),
		"Number of partitions for this Topic",
		[]string{"topic"}, nil,
	)

	topicCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_current_offset"),
		"Current Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, nil,
	)

	topicOldestOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_oldest_offset"),
		"Oldest Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, nil,
	)

	topicPartitionLeader = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader"),
		"Leader Broker ID of this Topic/Partition",
		[]string{"topic", "partition"}, nil,
	)

	topicPartitionReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_replicas"),
		"Number of Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, nil,
	)

	topicPartitionInSyncReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_in_sync_replica"),
		"Number of In-Sync Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, nil,
	)

	topicPartitionUsesPreferredReplica = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader_is_preferred"),
		"1 if Topic/Partition is using the Preferred Broker",
		[]string{"topic", "partition"}, nil,
	)

	topicUnderReplicatedPartition = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_under_replicated_partition"),
		"1 if Topic/Partition is under Replicated",
		[]string{"topic", "partition"}, nil,
	)

	consumergroupCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset"),
		"Current Offset of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)

	consumergroupLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag"),
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client sarama.Client
	offset map[string]map[int32]int64
}

type kafkaOpts struct {
	uri []string
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts kafkaOpts) (*Exporter, error) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient(opts.uri, config)

	if err != nil {
		fmt.Println("Error Init Kafka Client")
		panic(err)
	}
	fmt.Println("Done Init Clients")

	// Init our exporter.
	return &Exporter{
		client: client,
		offset: make(map[string]map[int32]int64),
	}, nil
}

// Describe describes all the metrics ever exported by the Kafka exporter. It
// implements prometheus.Collector.
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- clusterBrokers
	ch <- topicCurrentOffset
	ch <- topicOldestOffset
	ch <- topicPartitions
	ch <- topicPartitionLeader
	ch <- topicPartitionReplicas
	ch <- topicPartitionInSyncReplicas
	ch <- topicPartitionUsesPreferredReplica
	ch <- topicUnderReplicatedPartition
	ch <- consumergroupCurrentOffset
	ch <- consumergroupLag
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		clusterBrokers, prometheus.GaugeValue, float64(len(e.client.Brokers())),
	)

	topics, err := e.client.Topics()
	if err != nil {
		log.Errorf("Can't get topics: %v", err)
	} else {
		for _, topic := range topics {
			partitions, err := e.client.Partitions(topic)
			if err != nil {
				log.Errorf("Can't get partitions of topic %s: %v", topic, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitions, prometheus.GaugeValue, float64(len(partitions)), topic,
				)
				e.offset[topic] = make(map[int32]int64, len(partitions))
				for _, partition := range partitions {
					broker, err := e.client.Leader(topic, partition)
					if err != nil {
						log.Errorf("Can't get leader of topic %s partition %s: %v", topic, partition, err)
					} else {
						ch <- prometheus.MustNewConstMetric(
							topicPartitionLeader, prometheus.GaugeValue, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
						)
					}

					currentOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
					if err != nil {
						log.Errorf("Can't get current offset of topic %s partition %s: %v", topic, partition, err)
					} else {
						e.offset[topic][partition] = currentOffset
						ch <- prometheus.MustNewConstMetric(
							topicCurrentOffset, prometheus.GaugeValue, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10),
						)
					}

					oldestOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetOldest)
					if err != nil {
						log.Errorf("Can't get oldest offset of topic %s partition %s: %v", topic, partition, err)
					} else {
						ch <- prometheus.MustNewConstMetric(
							topicOldestOffset, prometheus.GaugeValue, float64(oldestOffset), topic, strconv.FormatInt(int64(partition), 10),
						)
					}

					replicas, err := e.client.Replicas(topic, partition)
					if err != nil {
						log.Errorf("Can't get replicas of topic %s partition %s: %v", topic, partition, err)
					} else {
						ch <- prometheus.MustNewConstMetric(
							topicPartitionReplicas, prometheus.GaugeValue, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
						)
					}

					inSyncReplicas, err := e.client.InSyncReplicas(topic, partition)
					if err != nil {
						log.Errorf("Can't get in-sync replicas of topic %s partition %s: %v", topic, partition, err)
					} else {
						ch <- prometheus.MustNewConstMetric(
							topicPartitionInSyncReplicas, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
						)
					}

					if broker != nil && replicas != nil && broker.ID() == replicas[0] {
						ch <- prometheus.MustNewConstMetric(
							topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
						)
					} else {
						ch <- prometheus.MustNewConstMetric(
							topicPartitionUsesPreferredReplica, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
						)
					}

					if replicas != nil && inSyncReplicas != nil && len(inSyncReplicas) < len(replicas) {
						ch <- prometheus.MustNewConstMetric(
							topicUnderReplicatedPartition, prometheus.GaugeValue, float64(1), topic, strconv.FormatInt(int64(partition), 10),
						)
					} else {
						ch <- prometheus.MustNewConstMetric(
							topicUnderReplicatedPartition, prometheus.GaugeValue, float64(0), topic, strconv.FormatInt(int64(partition), 10),
						)
					}
				}
			}
		}
	}
	if len(e.client.Brokers()) > 0 {
		for _, broker := range e.client.Brokers() {
			conf := sarama.NewConfig()
			conf.Version = sarama.V0_10_1_0
			if err := broker.Open(conf); err != nil {
				if err == sarama.ErrAlreadyConnected {
					broker.Close()
					if err := broker.Open(conf); err != nil {
						log.Errorf("Can't connect to broker %v: %v", broker.ID(), err)
						break
					}
				} else {
					log.Errorf("Can't connect to broker %v: %v", broker.ID(), err)
					break
				}
			}

			groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
			if err != nil {
				log.Errorf("Can't get consumer group: %v", err)
				broker.Close()
				break
			}

			groupIds := make([]string, 0)
			for groupId := range groups.Groups {
				groupIds = append(groupIds, groupId)
			}

			describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
			if err != nil {
				log.Errorf("Can't get describe groups: %v", err)
				broker.Close()
				break
			}

			for _, group := range describeGroups.Groups {
				offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: 1}
				for _, member := range group.Members {
					assignment, err := member.GetMemberAssignment()
					if err != nil {
						log.Errorf("Can't get member assignment of group: %s: %v", group.GroupId, err)
					} else {
						for topic, partitions := range assignment.Topics {
							for _, partition := range partitions {
								offsetFetchRequest.AddPartition(topic, partition)
							}
						}
					}
				}
				if offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest); err != nil {
					log.Errorf("Cant't get offset of group %s: %v", group.GroupId, err)
				} else {
					for topic, partitions := range offsetFetchResponse.Blocks {
						for partition, offsetFetchResponseBlock := range partitions {
							ch <- prometheus.MustNewConstMetric(
								consumergroupCurrentOffset, prometheus.GaugeValue, float64(offsetFetchResponseBlock.Offset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
							)
							if offset, ok := e.offset[topic][partition]; ok {
								ch <- prometheus.MustNewConstMetric(
									consumergroupLag, prometheus.GaugeValue, float64(offset-offsetFetchResponseBlock.Offset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
								)
							}
						}
					}
				}
			}
			broker.Close()
		}
	}
}

func init() {
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9308").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()

		opts = kafkaOpts{}
	)
	kingpin.Flag("kafka.server", "Address (host:port) of Kafka server.").Default("kafka:9092").StringsVar(&opts.uri)

	log.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	log.Infoln("Starting kafka_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	exporter, err := NewExporter(opts)
	if err != nil {
		log.Fatalln(err)
	}
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + *metricsPath + `'>Metrics</a></p>
	        </body>
	        </html>`))
	})

	log.Infoln("Listening on", *listenAddress)
	log.Fatal(http.ListenAndServe(*listenAddress, nil))
}
