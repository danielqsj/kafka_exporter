package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	kazoo "github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	namespace = "kafka"
	clientID  = "kafka_exporter"
)

var (
	clusterBrokers                           *prometheus.Desc
	topicPartitions                          *prometheus.Desc
	topicCurrentOffset                       *prometheus.Desc
	topicOldestOffset                        *prometheus.Desc
	topicPartitionLeader                     *prometheus.Desc
	topicPartitionReplicas                   *prometheus.Desc
	topicPartitionInSyncReplicas             *prometheus.Desc
	topicPartitionUsesPreferredReplica       *prometheus.Desc
	topicUnderReplicatedPartition            *prometheus.Desc
	consumergroupCurrentOffset               *prometheus.Desc
	consumergroupCurrentOffsetSum            *prometheus.Desc
	consumergroupUncomittedOffsets           *prometheus.Desc
	consumergroupUncommittedOffsetsSum       *prometheus.Desc
	consumergroupUncommittedOffsetsZookeeper *prometheus.Desc
	consumergroupMembers                     *prometheus.Desc
	//topicPartitionLagMillis                  *prometheus.Desc
	someMetricName *prometheus.Desc
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	groupFilter             *regexp.Regexp
	mu                      sync.Mutex
	useZooKeeperLag         bool
	zookeeperClient         *kazoo.Kazoo
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
	allowConcurrent         bool
	sgMutex                 sync.Mutex
	sgWaitCh                chan struct{}
	sgChans                 []chan<- prometheus.Metric
	consumerGroupFetchAll   bool
	consumerGroupLagTable   interpolationMap
}

type kafkaOpts struct {
	uri                      []string
	useSASL                  bool
	useSASLHandshake         bool
	saslUsername             string
	saslPassword             string
	saslMechanism            string
	useTLS                   bool
	tlsCAFile                string
	tlsCertFile              string
	tlsKeyFile               string
	tlsInsecureSkipTLSVerify bool
	kafkaVersion             string
	useZooKeeperLag          bool
	uriZookeeper             []string
	labels                   string
	metadataRefreshInterval  string
	allowConcurrent          bool
}

type interpolationMap struct {
	iMap map[string]map[string]map[int32]map[int64]time.Time
}

type Consumer struct {
	ready chan bool
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if certReadable == false && keyReadable == false {
		return false, nil
	}

	if certReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if keyReadable == false {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", keyPath)
	}

	return true, nil
}

// If the file represented by path exists and
// readable, returns true otherwise returns false.
func canReadFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}

	defer f.Close()

	return true
}

// NewExporter returns an initialized Exporter.
func NewExporter(opts kafkaOpts, topicFilter string, groupFilter string) (*Exporter, error) {
	var zookeeperClient *kazoo.Kazoo
	config := sarama.NewConfig()
	config.ClientID = clientID
	kafkaVersion, err := sarama.ParseKafkaVersion(opts.kafkaVersion)
	if err != nil {
		return nil, err
	}
	config.Version = kafkaVersion

	if opts.useSASL {
		// Convert to lowercase so that SHA512 and SHA256 is still valid
		opts.saslMechanism = strings.ToLower(opts.saslMechanism)
		switch opts.saslMechanism {
		case "scram-sha512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		case "scram-sha256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)

		case "plain":
		default:
			plog.Fatalf("invalid sasl mechanism \"%s\": can only be \"scram-sha256\", \"scram-sha512\" or \"plain\"", opts.saslMechanism)
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if opts.saslPassword != "" {
			config.Net.SASL.Password = opts.saslPassword
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			RootCAs:            x509.NewCertPool(),
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := ioutil.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				plog.Fatalln(err)
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			plog.Fatalln(err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				plog.Fatalln(err)
			}
		}
	}

	if opts.useZooKeeperLag {
		zookeeperClient, err = kazoo.NewKazoo(opts.uriZookeeper, nil)
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		plog.Errorln("Cannot parse metadata refresh interval")
		panic(err)
	}

	config.Metadata.RefreshFrequency = interval

	client, err := sarama.NewClient(opts.uri, config)

	if err != nil {
		plog.Errorln("Error Init Kafka Client")
		panic(err)
	}
	plog.Infoln("Done Init Clients")

	// Init our exporter.
	return &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile(topicFilter),
		groupFilter:             regexp.MustCompile(groupFilter),
		useZooKeeperLag:         opts.useZooKeeperLag,
		zookeeperClient:         zookeeperClient,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
		allowConcurrent:         opts.allowConcurrent,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   config.Version.IsAtLeast(sarama.V2_0_0_0),
		consumerGroupLagTable:   interpolationMap{},
	}, nil
}

func (e *Exporter) fetchOffsetVersion() int16 {
	version := e.client.Config().Version
	if e.client.Config().Version.IsAtLeast(sarama.V2_0_0_0) {
		return 4
	} else if version.IsAtLeast(sarama.V0_10_2_0) {
		return 2
	} else if version.IsAtLeast(sarama.V0_8_2_2) {
		return 1
	}
	return 0
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
	ch <- consumergroupCurrentOffsetSum
	ch <- consumergroupUncomittedOffsets
	ch <- consumergroupUncommittedOffsetsZookeeper
	ch <- consumergroupUncommittedOffsetsSum
	//ch <- topicPartitionLagMillis
	ch <- someMetricName
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	if e.allowConcurrent {
		e.collect(ch)
		return
	}
	// Locking to avoid race add
	e.sgMutex.Lock()
	e.sgChans = append(e.sgChans, ch)
	// Safe to compare length since we own the Lock
	if len(e.sgChans) == 1 {
		e.sgWaitCh = make(chan struct{})
		go e.collectChans(e.sgWaitCh)
	} else {
		plog.Info("concurrent calls detected, waiting for first to finish")
	}
	// Put in another variable to ensure not overwriting it in another Collect once we wait
	waiter := e.sgWaitCh
	e.sgMutex.Unlock()
	// Released lock, we have insurance that our chan will be part of the collectChan slice
	<-waiter
	// collectChan finished
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
func (e *Exporter) collectChans(quit chan struct{}) {
	original := make(chan prometheus.Metric)
	container := make([]prometheus.Metric, 0, 100)
	go func() {
		for metric := range original {
			container = append(container, metric)
		}
	}()
	e.collect(original)
	close(original)
	// Lock to avoid modification on the channel slice
	e.sgMutex.Lock()
	for _, ch := range e.sgChans {
		for _, metric := range container {
			ch <- metric
		}
	}
	// Reset the slice
	e.sgChans = e.sgChans[:0]
	// Notify remaining waiting Collect they can return
	close(quit)
	// Release the lock so Collect can append to the slice again
	e.sgMutex.Unlock()
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) {
	var wg = sync.WaitGroup{}
	ch <- prometheus.MustNewConstMetric(
		clusterBrokers, prometheus.GaugeValue, float64(len(e.client.Brokers())),
	)

	offset := make(map[string]map[int32]int64)

	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		plog.Info("Refreshing client metadata")

		if err := e.client.RefreshMetadata(); err != nil {
			plog.Errorf("Cannot refresh topics, using cached data: %v", err)
		}

		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	topics, err := e.client.Topics()
	if err != nil {
		plog.Errorf("Cannot get topics: %v", err)
		return
	}

	getTopicMetrics := func(topic string) {
		defer wg.Done()
		plog.Debugf("Fetching metrics for \"%s\"", topic)
		if e.topicFilter.MatchString(topic) {
			partitions, err := e.client.Partitions(topic)
			if err != nil {
				plog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
				return
			}
			ch <- prometheus.MustNewConstMetric(
				topicPartitions, prometheus.GaugeValue, float64(len(partitions)), topic,
			)
			e.mu.Lock()
			offset[topic] = make(map[int32]int64, len(partitions))
			e.mu.Unlock()
			for _, partition := range partitions {
				broker, err := e.client.Leader(topic, partition)
				if err != nil {
					plog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionLeader, prometheus.GaugeValue, float64(broker.ID()), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				currentOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					plog.Errorf("Cannot get current offset of topic %s partition %d: %v", topic, partition, err)
				} else {
					e.mu.Lock()
					offset[topic][partition] = currentOffset
					e.mu.Unlock()
					ch <- prometheus.MustNewConstMetric(
						topicCurrentOffset, prometheus.GaugeValue, float64(currentOffset), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				oldestOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetOldest)
				if err != nil {
					plog.Errorf("Cannot get oldest offset of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicOldestOffset, prometheus.GaugeValue, float64(oldestOffset), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				replicas, err := e.client.Replicas(topic, partition)
				if err != nil {
					plog.Errorf("Cannot get replicas of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionReplicas, prometheus.GaugeValue, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				inSyncReplicas, err := e.client.InSyncReplicas(topic, partition)
				if err != nil {
					plog.Errorf("Cannot get in-sync replicas of topic %s partition %d: %v", topic, partition, err)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicPartitionInSyncReplicas, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
					)
				}

				if broker != nil && replicas != nil && len(replicas) > 0 && broker.ID() == replicas[0] {
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

				if e.useZooKeeperLag {
					ConsumerGroups, err := e.zookeeperClient.Consumergroups()

					if err != nil {
						plog.Errorf("Cannot get consumer group %v", err)
					}

					for _, group := range ConsumerGroups {
						offset, _ := group.FetchOffset(topic, partition)
						if offset > 0 {

							consumerGroupLag := currentOffset - offset
							ch <- prometheus.MustNewConstMetric(
								consumergroupUncommittedOffsetsZookeeper, prometheus.GaugeValue, float64(consumerGroupLag), group.Name, topic, strconv.FormatInt(int64(partition), 10),
							)
						}
					}
				}
			}
		}
	}

	plog.Info("Fetching topic metrics")
	for _, topic := range topics {
		wg.Add(1)
		go getTopicMetrics(topic)
	}

	wg.Wait()

	getConsumerGroupMetrics := func(broker *sarama.Broker) {
		defer wg.Done()
		plog.Debugf("[%d] Fetching consumer group metrics", broker.ID())
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			plog.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}
		defer broker.Close()

		plog.Debugf("[%d]> listing groups", broker.ID())
		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			plog.Errorf("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}
		plog.Debugf("[%d]> describing groups", broker.ID())
		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			plog.Errorf("Cannot get describe groups: %v", err)
			return
		}
		for _, group := range describeGroups.Groups {
			offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: e.fetchOffsetVersion()}
			if !e.consumerGroupFetchAll {
				for topic, partitions := range offset {
					for partition := range partitions {
						offsetFetchRequest.AddPartition(topic, partition)
					}
				}
			}
			ch <- prometheus.MustNewConstMetric(
				consumergroupMembers, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
			)
			start := time.Now()
			plog.Debugf("[%d][%s]> fetching group offsets", broker.ID(), group.GroupId)
			if offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest); err != nil {
				plog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
			} else {
				plog.Debugf("[%d][%s] done fetching group offset in %s", broker.ID(), group.GroupId, time.Since(start).String())
				for topic, partitions := range offsetFetchResponse.Blocks {
					if !e.topicFilter.MatchString(topic) {
						continue
					}
					// If the topic is not consumed by that consumer group, skip it
					topicConsumed := false
					for _, offsetFetchResponseBlock := range partitions {
						// Kafka will return -1 if there is no offset associated with a topic-partition under that consumer group
						if offsetFetchResponseBlock.Offset != -1 {
							topicConsumed = true
							break
						}
					}
					if topicConsumed {
						var currentOffsetSum int64
						var lagSum int64
						for partition, offsetFetchResponseBlock := range partitions {
							err := offsetFetchResponseBlock.Err
							if err != sarama.ErrNoError {
								plog.Errorf("Error for  partition %d :%v", partition, err.Error())
								continue
							}
							currentOffset := offsetFetchResponseBlock.Offset
							currentOffsetSum += currentOffset
							ch <- prometheus.MustNewConstMetric(
								consumergroupCurrentOffset, prometheus.GaugeValue, float64(currentOffset), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
							)
							e.mu.Lock()
							if offset, ok := offset[topic][partition]; ok {
								nextOffset, err := e.client.GetOffset(topic, partition, sarama.OffsetNewest)
								if err != nil {
								}
								e.consumerGroupLagTable.createOrUpdate(group.GroupId, topic, partition, nextOffset)

								// If the topic is consumed by that consumer group, but no offset associated with the partition
								// forcing lag to -1 to be able to alert on that
								var lag int64
								if offsetFetchResponseBlock.Offset == -1 {
									lag = -1
								} else {
									lag = offset - offsetFetchResponseBlock.Offset
									lagSum += lag
								}
								ch <- prometheus.MustNewConstMetric(
									consumergroupUncomittedOffsets, prometheus.GaugeValue, float64(lag), group.GroupId, topic, strconv.FormatInt(int64(partition), 10),
								)
							} else {
								plog.Errorf("No offset of topic %s partition %d, cannot get consumer group lag", topic, partition)
							}
							e.mu.Unlock()
						}
						ch <- prometheus.MustNewConstMetric(
							consumergroupCurrentOffsetSum, prometheus.GaugeValue, float64(currentOffsetSum), group.GroupId, topic,
						)
						ch <- prometheus.MustNewConstMetric(
							consumergroupUncommittedOffsetsSum, prometheus.GaugeValue, float64(lagSum), group.GroupId, topic,
						)
					}
				}
			}
		}
	}

	plog.Info("Fetching consumer group metrics")
	if len(e.client.Brokers()) > 0 {
		for _, broker := range e.client.Brokers() {
			wg.Add(1)
			go getConsumerGroupMetrics(broker)
		}
		wg.Wait()
	} else {
		plog.Errorln("No valid broker, cannot get consumer group metrics")
	}

	calculateConsumerGroupLag := func() {
		defer wg.Done()

		admin, err := sarama.NewClusterAdminFromClient(e.client)
		if err != nil {
		}

		for group, topics := range e.consumerGroupLagTable.iMap {
			for topic, partitionMap := range topics {
				var partitionKeys []int32
				for key := range partitionMap {
					partitionKeys = append(partitionKeys, key)
				}

				// Blocks is map of topic to partition to offset
				response, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{
					topic: partitionKeys,
				})
				if err != nil {
				}

				for partition, offsets := range partitionMap {
					if len(offsets) < 2 {
						continue
					}
					if latestConsumedOffset, ok := response.Blocks[topic][partition]; ok {
						/*
							Sort offset keys
							If latestConsumedOffset < smallestMappedOffset then extrapolate
							Find two offsets that bound latestConsumedOffset
						*/
						var producedOffsets []int64
						e.mu.Lock()
						for offsetKey := range offsets {
							producedOffsets = append(producedOffsets, offsetKey)
						}
						sort.Slice(producedOffsets, func(i, j int) bool { return producedOffsets[i] < producedOffsets[j] })
						if latestConsumedOffset.Offset < producedOffsets[0] {
							// Because we do not have data points that bound the latestConsumedOffset we must use extrapolation
							highestOffset := producedOffsets[len(producedOffsets)-1]
							lowestOffset := producedOffsets[0]

							px := float64(offsets[highestOffset].UnixNano()/1000000) -
								float64(highestOffset-latestConsumedOffset.Offset)*
									float64((offsets[highestOffset].Sub(offsets[lowestOffset])).Milliseconds())/float64(highestOffset-lowestOffset)
							lagMillis := float64(time.Now().UnixNano()/1000000) - px
							plog.Infof("estimated lag for %s in ms: %f", group, lagMillis)

							ch <- prometheus.MustNewConstMetric(someMetricName, prometheus.GaugeValue, lagMillis)
							//ch <- prometheus.MustNewConstMetric(topicPartitionLagMillis, prometheus.GaugeValue, lagMillis, group, topic, strconv.FormatInt(int64(partition), 10))

						} else {
							nextHigherOffset := getNextHigherOffset(producedOffsets, latestConsumedOffset.Offset)
							nextLowerOffset := getNextLowerOffset(producedOffsets, latestConsumedOffset.Offset)
							px := float64(offsets[nextHigherOffset].UnixNano()/1000000) -
								float64(nextHigherOffset-latestConsumedOffset.Offset)*
									float64((offsets[nextHigherOffset].Sub(offsets[nextLowerOffset])).Milliseconds())/float64(nextHigherOffset-nextLowerOffset)
							lagMillis := float64(time.Now().UnixNano()/1000000) - px
							plog.Infof("estimated lag for %s in ms: %f", group, lagMillis)
							ch <- prometheus.MustNewConstMetric(someMetricName, prometheus.GaugeValue, lagMillis)
							//ch <- prometheus.MustNewConstMetric(topicPartitionLagMillis, prometheus.GaugeValue, lagMillis, group, topic, strconv.FormatInt(int64(partition), 10))

						}
						e.mu.Unlock()

					} else {
						//could not get latestConsumedOffset
					}
				}
			}
		}
	}

	plog.Infof("Calculating consumer group lag")
	wg.Add(1)
	go calculateConsumerGroupLag()
	wg.Wait()
}

func (i *interpolationMap) createOrUpdate(group, topic string, partition int32, offset int64) {
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
}

func getNextHigherOffset(offsets []int64, k int64) int64 {
	index := len(offsets) - 1
	max := offsets[index]

	for max >= k && index > 0 {
		if offsets[index-1] < k {
			return max
		}
		max = offsets[index]
		index--
	}
	return max
}

func getNextLowerOffset(offsets []int64, k int64) int64 {
	index := 0
	min := offsets[index]
	for min <= k && index < len(offsets)-1 {
		if offsets[index+1] > k {
			return min
		}
		min = offsets[index]
		index++
	}
	return min
}

func slowConsumer_old(opts kafkaOpts, topicFilter string, groupFilter string) {
	exporter, err := NewExporter(opts, topicFilter, groupFilter)
	consumer := Consumer{ready: make(chan bool)}
	ctx := context.Background()

	brokers := make([]string, 0)
	for _, addr := range exporter.client.Brokers() {
		brokers = append(brokers, addr.Addr())
	}
	client, err := sarama.NewConsumerGroup(brokers, "groupie-group", exporter.client.Config())
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	topics := []string{"test"}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {

			if err := client.Consume(ctx, topics, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	counter := 0
	for message := range claim.Messages() {
		counter++
		if counter >= 5000 {
			plog.Infof("TIME FOR LAG!!!")
			time.Sleep(50 * time.Second)
			counter = 0
		}
		//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}

func someProducer_old() {
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	counter := 0
	var enqueued, producerErrors int
ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "test", Key: nil, Value: sarama.StringEncoder("testing 123")}:
			enqueued++
			counter++
			if counter >= 50 {
				counter = 0
				time.Sleep(1 * time.Second)
				plog.Infof("sleeping")
			}
		case err := <-producer.Errors():
			log.Println("Failed to produce message", err)
			producerErrors++
		case <-signals:
			break ProducerLoop
		}
	}

	log.Printf("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9308").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		topicFilter   = kingpin.Flag("topic.filter", "Regex that determines which topics to collect.").Default(".*").String()
		groupFilter   = kingpin.Flag("group.filter", "Regex that determines which consumer groups to collect.").Default(".*").String()
		logSarama     = kingpin.Flag("log.enable-sarama", "Turn on Sarama logging.").Default("false").Bool()

		opts = kafkaOpts{}
	)
	kingpin.Flag("kafka.server", "Address (host:port) of Kafka server.").Default("kafka:9092").StringsVar(&opts.uri)
	kingpin.Flag("sasl.enabled", "Connect using SASL/PLAIN.").Default("false").BoolVar(&opts.useSASL)
	kingpin.Flag("sasl.handshake", "Only set this to false if using a non-Kafka SASL proxy.").Default("true").BoolVar(&opts.useSASLHandshake)
	kingpin.Flag("sasl.username", "SASL user name.").Default("").StringVar(&opts.saslUsername)
	kingpin.Flag("sasl.password", "SASL user password.").Default("").StringVar(&opts.saslPassword)
	kingpin.Flag("sasl.mechanism", "The SASL SCRAM SHA algorithm sha256 or sha512 as mechanism").Default("").StringVar(&opts.saslMechanism)
	kingpin.Flag("tls.enabled", "Connect using TLS.").Default("false").BoolVar(&opts.useTLS)
	kingpin.Flag("tls.ca-file", "The optional certificate authority file for TLS client authentication.").Default("").StringVar(&opts.tlsCAFile)
	kingpin.Flag("tls.cert-file", "The optional certificate file for client authentication.").Default("").StringVar(&opts.tlsCertFile)
	kingpin.Flag("tls.key-file", "The optional key file for client authentication.").Default("").StringVar(&opts.tlsKeyFile)
	kingpin.Flag("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure.").Default("false").BoolVar(&opts.tlsInsecureSkipTLSVerify)
	kingpin.Flag("kafka.version", "Kafka broker version").Default(sarama.V2_0_0_0.String()).StringVar(&opts.kafkaVersion)
	kingpin.Flag("use.consumelag.zookeeper", "if you need to use a group from zookeeper").Default("false").BoolVar(&opts.useZooKeeperLag)
	kingpin.Flag("zookeeper.server", "Address (hosts) of zookeeper server.").Default("localhost:2181").StringsVar(&opts.uriZookeeper)
	kingpin.Flag("kafka.labels", "Kafka cluster name").Default("").StringVar(&opts.labels)
	kingpin.Flag("refresh.metadata", "Metadata refresh interval").Default("1m").StringVar(&opts.metadataRefreshInterval)
	kingpin.Flag("concurrent.enable", "If true, all scrapes will trigger kafka operations otherwise, they will share results. WARN: This should be disabled on large clusters").Default("false").BoolVar(&opts.allowConcurrent)

	plog.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	plog.Infoln("Starting kafka_exporter", version.Info())
	plog.Infoln("Build context", version.BuildContext())

	labels := make(map[string]string)

	// Protect against empty labels
	if opts.labels != "" {
		for _, label := range strings.Split(opts.labels, ",") {
			splitted := strings.Split(label, "=")
			if len(splitted) >= 2 {
				labels[splitted[0]] = splitted[1]
			}
		}
	}

	clusterBrokers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "brokers"),
		"Number of Brokers in the Kafka Cluster.",
		nil, labels,
	)
	topicPartitions = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partitions"),
		"Number of partitions for this Topic",
		[]string{"topic"}, labels,
	)
	topicCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_current_offset"),
		"Current Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)
	topicOldestOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_oldest_offset"),
		"Oldest Offset of a Broker at Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionLeader = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader"),
		"Leader Broker ID of this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_replicas"),
		"Number of Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionInSyncReplicas = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_in_sync_replica"),
		"Number of In-Sync Replicas for this Topic/Partition",
		[]string{"topic", "partition"}, labels,
	)

	topicPartitionUsesPreferredReplica = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_leader_is_preferred"),
		"1 if Topic/Partition is using the Preferred Broker",
		[]string{"topic", "partition"}, labels,
	)

	topicUnderReplicatedPartition = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "topic", "partition_under_replicated_partition"),
		"1 if Topic/Partition is under Replicated",
		[]string{"topic", "partition"}, labels,
	)

	consumergroupCurrentOffset = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset"),
		"Current Offset of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupCurrentOffsetSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "current_offset_sum"),
		"Current Offset of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupUncomittedOffsets = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "uncommitted_offsets"),
		"Current Approximate count of uncommitted offsets for a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupUncommittedOffsetsZookeeper = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroupzookeeper", "uncommitted_offsets_zookeeper"),
		"Current Approximate count of uncommitted offsets(zookeeper) for a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)

	consumergroupUncommittedOffsetsSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "uncommitted_offsets_sum"),
		"Current Approximate count of uncommitted offsets for a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	//topicPartitionLagMillis = prometheus.NewDesc(
	//	prometheus.BuildFQName(namespace, "consumer", "lag_millis"),
	//	"Current approximation of consumer lag for a ConsumerGroup at Topic/Partition",
	//	[]string{"consumergroup", "topic", "partition"},
	//	labels,
	//)

	someMetricName = prometheus.NewDesc(prometheus.BuildFQName(namespace, "consumer", "some_metric"),
		"",
		nil,
		nil)

	if *logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, *topicFilter, *groupFilter)
	if err != nil {
		plog.Fatalln(err)
	}
	defer exporter.client.Close()
	prometheus.MustRegister(exporter)
	go someProducer_old()
	go slowConsumer_old(opts, *topicFilter, *groupFilter)

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

	plog.Infoln("Listening on", *listenAddress)
	plog.Fatal(http.ListenAndServe(*listenAddress, nil))
}
