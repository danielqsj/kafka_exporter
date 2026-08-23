package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/alecthomas/kingpin/v2"
	"github.com/aws/aws-msk-iam-sasl-signer-go/signer"
	"github.com/krallistic/kazoo-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/promlog"
	plogflag "github.com/prometheus/common/promlog/flag"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	versionCollector "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"k8s.io/klog/v2"
)

const (
	namespace = "kafka"
	clientID  = "kafka_exporter"
)

const (
	INFO  = 0
	DEBUG = 1
	TRACE = 2
)

var (
	clusterBrokers                     *prometheus.Desc
	clusterBrokerInfo                  *prometheus.Desc
	topicPartitions                    *prometheus.Desc
	topicCurrentOffset                 *prometheus.Desc
	topicOldestOffset                  *prometheus.Desc
	topicPartitionLeader               *prometheus.Desc
	topicPartitionReplicas             *prometheus.Desc
	topicPartitionInSyncReplicas       *prometheus.Desc
	topicPartitionUsesPreferredReplica *prometheus.Desc
	topicUnderReplicatedPartition      *prometheus.Desc
	consumergroupCurrentOffset         *prometheus.Desc
	consumergroupCurrentOffsetSum      *prometheus.Desc
	consumergroupLag                   *prometheus.Desc
	consumergroupLagSum                *prometheus.Desc
	consumergroupLagZookeeper          *prometheus.Desc
	consumergroupMembers               *prometheus.Desc
)

// Exporter collects Kafka stats from the given server and exports them using
// the prometheus metrics package.
type Exporter struct {
	client                  sarama.Client
	topicFilter             *regexp.Regexp
	topicExclude            *regexp.Regexp
	groupFilter             *regexp.Regexp
	groupExclude            *regexp.Regexp
	mu                      sync.RWMutex
	useZooKeeperLag         bool
	zookeeperClient         *kazoo.Kazoo
	nextMetadataRefresh     time.Time
	metadataRefreshInterval time.Duration
	offsetShowAll           bool
	topicWorkers            int
	groupWorkers            int
	allowConcurrent         bool
	sgMutex                 sync.Mutex
	sgWaitCh                chan struct{}
	sgChans                 []chan<- prometheus.Metric
	consumerGroupFetchAll   bool
	groupMetricsTimeout     time.Duration
}

// define a struct to handle the case where the lag is negative, and needs to be requested again
type deferredGroupTask struct {
	group  *sarama.GroupDescription
	blocks map[string]map[int32]*sarama.OffsetFetchResponseBlock
}

type kafkaOpts struct {
	uri                      []string
	useSASL                  bool
	useSASLHandshake         bool
	saslUsername             string
	saslPassword             string
	saslMechanism            string
	saslDisablePAFXFast      bool
	saslAwsRegion            string
	saslOAuthBearerTokenUrl  string
	saslOAuthBearerScopes    string
	useTLS                   bool
	tlsServerName            string
	tlsCAFile                string
	tlsCertFile              string
	tlsKeyFile               string
	serverUseTLS             bool
	serverMutualAuthEnabled  bool
	serverTlsCAFile          string
	serverTlsCertFile        string
	serverTlsKeyFile         string
	tlsInsecureSkipTLSVerify bool
	kafkaVersion             string
	useZooKeeperLag          bool
	uriZookeeper             []string
	labels                   string
	metadataRefreshInterval  string
	serviceName              string
	kerberosConfigPath       string
	realm                    string
	keyTabPath               string
	kerberosAuthType         string
	offsetShowAll            bool
	topicWorkers             int
	groupWorkers             int
	allowConcurrent          bool
	allowAutoTopicCreation   bool
	verbosityLogLevel        int
	groupMetricsTimeout      string
}

type MSKAccessTokenProvider struct {
	region string
}

func (m *MSKAccessTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := signer.GenerateAuthToken(context.TODO(), m.region)
	return &sarama.AccessToken{Token: token}, err
}

type OAuth2Config interface {
	Token(ctx context.Context) (*oauth2.Token, error)
}

type oauthbearerTokenProvider struct {
	tokenExpiration time.Time
	token           string
	oauth2Config    OAuth2Config
}

func newOauthbearerTokenProvider(oauth2Config OAuth2Config) *oauthbearerTokenProvider {
	return &oauthbearerTokenProvider{
		tokenExpiration: time.Time{},
		token:           "",
		oauth2Config:    oauth2Config,
	}
}

func (o *oauthbearerTokenProvider) Token() (*sarama.AccessToken, error) {
	var accessToken string
	var err error

	if o.token != "" && time.Now().Before(o.tokenExpiration.Add(time.Duration(-2)*time.Second)) {
		accessToken = o.token
		err = nil
	} else {
		token, err := o.oauth2Config.Token(context.Background())
		if err == nil {
			accessToken = token.AccessToken
			o.token = token.AccessToken
			o.tokenExpiration = token.Expiry
		}
	}

	return &sarama.AccessToken{Token: accessToken}, err
}

// CanReadCertAndKey returns true if the certificate and key files already exists,
// otherwise returns false. If lost one of cert and key, returns error.
func CanReadCertAndKey(certPath, keyPath string) (bool, error) {
	certReadable := canReadFile(certPath)
	keyReadable := canReadFile(keyPath)

	if !certReadable && !keyReadable {
		return false, nil
	}

	if !certReadable {
		return false, fmt.Errorf("error reading %s, certificate and key must be supplied as a pair", certPath)
	}

	if !keyReadable {
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
func NewExporter(opts kafkaOpts, topicFilter string, topicExclude string, groupFilter string, groupExclude string) (*Exporter, error) {
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

		saslPassword := opts.saslPassword
		if saslPassword == "" {
			saslPassword = os.Getenv("SASL_USER_PASSWORD")
		}

		switch opts.saslMechanism {
		case "scram-sha512":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
		case "scram-sha256":
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256)
		case "gssapi":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeGSSAPI)
			config.Net.SASL.GSSAPI.ServiceName = opts.serviceName
			config.Net.SASL.GSSAPI.KerberosConfigPath = opts.kerberosConfigPath
			config.Net.SASL.GSSAPI.Realm = opts.realm
			config.Net.SASL.GSSAPI.Username = opts.saslUsername
			if opts.kerberosAuthType == "keytabAuth" {
				config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
				config.Net.SASL.GSSAPI.KeyTabPath = opts.keyTabPath
			} else {
				config.Net.SASL.GSSAPI.AuthType = sarama.KRB5_USER_AUTH
				config.Net.SASL.GSSAPI.Password = saslPassword
			}
			if opts.saslDisablePAFXFast {
				config.Net.SASL.GSSAPI.DisablePAFXFAST = true
			}
		case "awsiam":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeOAuth)
			config.Net.SASL.TokenProvider = &MSKAccessTokenProvider{region: opts.saslAwsRegion}
		case "oauthbearer":
			config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeOAuth)
			tokenUrl := opts.saslOAuthBearerTokenUrl
			if tokenUrl == "" {
				tokenUrl = os.Getenv("SASL_OAUTHBEARER_TOKEN_URL")
			}
			if tokenUrl == "" {
				log.Fatalf("[ERROR] sasl.oauthbearer-token-url must be configured or SASL_OAUTHBEARER_TOKEN_URL environment variable must be set when using the OAuthBearer SASL mechanism")
			}
			saslUsername := opts.saslUsername
			if saslUsername == "" {
				log.Fatalf("[ERROR] sasl.username must be configured when using the OAuthBearer SASL mechanism")
			}
			oauth2Config := clientcredentials.Config{
				TokenURL:     tokenUrl,
				ClientID:     saslUsername,
				ClientSecret: saslPassword,
				Scopes:       strings.Split(opts.saslOAuthBearerScopes, ","),
			}
			config.Net.SASL.TokenProvider = newOauthbearerTokenProvider(&oauth2Config)
		case "plain":
		default:
			return nil, fmt.Errorf(
				`invalid sasl mechanism %q: can only be "scram-sha256", "scram-sha512", "gssapi", "awsiam" or "plain"`,
				opts.saslMechanism,
			)
		}

		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = opts.useSASLHandshake

		if opts.saslUsername != "" {
			config.Net.SASL.User = opts.saslUsername
		}

		if saslPassword != "" {
			config.Net.SASL.Password = saslPassword
		}
	}

	if opts.useTLS {
		config.Net.TLS.Enable = true

		config.Net.TLS.Config = &tls.Config{
			ServerName:         opts.tlsServerName,
			InsecureSkipVerify: opts.tlsInsecureSkipTLSVerify,
		}

		if opts.tlsCAFile != "" {
			if ca, err := os.ReadFile(opts.tlsCAFile); err == nil {
				config.Net.TLS.Config.RootCAs = x509.NewCertPool()
				config.Net.TLS.Config.RootCAs.AppendCertsFromPEM(ca)
			} else {
				return nil, err
			}
		}

		canReadCertAndKey, err := CanReadCertAndKey(opts.tlsCertFile, opts.tlsKeyFile)
		if err != nil {
			return nil, fmt.Errorf("error reading cert and key: %w", err)
		}
		if canReadCertAndKey {
			cert, err := tls.LoadX509KeyPair(opts.tlsCertFile, opts.tlsKeyFile)
			if err == nil {
				config.Net.TLS.Config.Certificates = []tls.Certificate{cert}
			} else {
				return nil, err
			}
		}
	}

	if opts.useZooKeeperLag {
		klog.V(DEBUG).Infoln("Using zookeeper lag, so connecting to zookeeper")
		zookeeperClient, err = kazoo.NewKazoo(opts.uriZookeeper, nil)
		if err != nil {
			return nil, fmt.Errorf("error connecting to zookeeper: %w", err)
		}
	}

	groupMetricsTimeout, err := time.ParseDuration(opts.groupMetricsTimeout)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse group metrics timeout: %w", err)
	}

	interval, err := time.ParseDuration(opts.metadataRefreshInterval)
	if err != nil {
		return nil, fmt.Errorf("Cannot parse metadata refresh interval: %w", err)
	}

	config.Metadata.RefreshFrequency = interval

	config.Metadata.AllowAutoTopicCreation = opts.allowAutoTopicCreation

	client, err := sarama.NewClient(opts.uri, config)
	if err != nil {
		return nil, fmt.Errorf("Error Init Kafka Client: %w", err)
	}

	klog.V(TRACE).Infoln("Done Init Clients")
	// Init our exporter.
	return &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile(topicFilter),
		topicExclude:            regexp.MustCompile(topicExclude),
		groupFilter:             regexp.MustCompile(groupFilter),
		groupExclude:            regexp.MustCompile(groupExclude),
		useZooKeeperLag:         opts.useZooKeeperLag,
		zookeeperClient:         zookeeperClient,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: interval,
		offsetShowAll:           opts.offsetShowAll,
		topicWorkers:            opts.topicWorkers,
		groupWorkers:            opts.groupWorkers,
		allowConcurrent:         opts.allowConcurrent,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   config.Version.IsAtLeast(sarama.V2_0_0_0),
		groupMetricsTimeout:     groupMetricsTimeout,
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

func boundedWorkerCount(total, limit int) int {
	if total <= 0 {
		return 0
	}
	if limit <= 0 || limit > total {
		return total
	}
	return limit
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
	ch <- consumergroupLag
	ch <- consumergroupLagZookeeper
	ch <- consumergroupLagSum
}

// Collect fetches the stats from configured Kafka location and delivers them
// as Prometheus metrics. It implements prometheus.Collector.
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
		klog.V(TRACE).Info("concurrent calls detected, waiting for first to finish")
	}
	// Put in another variable to ensure not overwriting it in another Collect once we wait
	waiter := e.sgWaitCh
	e.sgMutex.Unlock()
	// Released lock, we have insurance that our chan will be part of the collectChan slice
	<-waiter
	// collectChan finished
}

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
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime)
		klog.Infof("collect process took %v", duration)
	}()

	ch <- prometheus.MustNewConstMetric(
		clusterBrokers, prometheus.GaugeValue, float64(len(e.client.Brokers())),
	)
	for _, b := range e.client.Brokers() {
		ch <- prometheus.MustNewConstMetric(
			clusterBrokerInfo, prometheus.GaugeValue, 1, strconv.Itoa(int(b.ID())), b.Addr(),
		)
	}

	offset := make(map[string]map[int32]int64)
	// initialize the topic partition leader mapping
	topicPartitionLeaders := make(map[string]map[int32]int32)
	now := time.Now()

	if now.After(e.nextMetadataRefresh) {
		klog.V(DEBUG).Info("Refreshing client metadata")
		if err := e.client.RefreshMetadata(); err != nil {
			klog.Errorf("Cannot refresh topics, using cached data: %v", err)
		}
		e.nextMetadataRefresh = now.Add(e.metadataRefreshInterval)
	}

	// ==================== Phase 1: Collect Topic Metrics ====================
	phase1Start := time.Now()
	topics, err := e.client.Topics()
	if err != nil {
		klog.Errorf("Cannot get topics: %v", err)
		return
	}

	klog.V(DEBUG).Infof("Phase 1: Fetching topic offsets, Found %v topics", len(topics))
	// initialize the broker newest offset and oldest offset requests
	brokerNewestOffsetRequests := make(map[int32]*sarama.OffsetRequest)
	brokerOldestOffsetRequests := make(map[int32]*sarama.OffsetRequest)

	// iterate through all topic partitions, group by leader
	for _, topic := range topics {
		if !e.topicFilter.MatchString(topic) || e.topicExclude.MatchString(topic) {
			continue
		}

		partitions, err := e.client.Partitions(topic)
		if err != nil {
			klog.Errorf("Cannot get partitions of topic %s: %v", topic, err)
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			topicPartitions, prometheus.GaugeValue, float64(len(partitions)), topic,
		)
		e.mu.Lock()
		offset[topic] = make(map[int32]int64, len(partitions))
		topicPartitionLeaders[topic] = make(map[int32]int32, len(partitions))
		e.mu.Unlock()
		for _, partition := range partitions {
			leader, err := e.client.Leader(topic, partition)
			if err != nil {
				klog.Errorf("Cannot get leader of topic %s partition %d: %v", topic, partition, err)
				continue
			}
			e.mu.Lock()
			topicPartitionLeaders[topic][partition] = leader.ID()
			e.mu.Unlock()

			ch <- prometheus.MustNewConstMetric(
				topicPartitionLeader, prometheus.GaugeValue, float64(leader.ID()), topic, strconv.FormatInt(int64(partition), 10),
			)

			// build the newest offset request
			if _, ok := brokerNewestOffsetRequests[leader.ID()]; !ok {
				brokerNewestOffsetRequests[leader.ID()] = &sarama.OffsetRequest{}
			}
			brokerNewestOffsetRequests[leader.ID()].AddBlock(topic, partition, sarama.OffsetNewest, 1)

			// build the oldest offset request
			if _, ok := brokerOldestOffsetRequests[leader.ID()]; !ok {
				brokerOldestOffsetRequests[leader.ID()] = &sarama.OffsetRequest{}
			}
			brokerOldestOffsetRequests[leader.ID()].AddBlock(topic, partition, sarama.OffsetOldest, 1)

			replicas, err := e.client.Replicas(topic, partition)
			if err != nil {
				klog.Errorf("Cannot get replicas of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionReplicas, prometheus.GaugeValue, float64(len(replicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			inSyncReplicas, err := e.client.InSyncReplicas(topic, partition)
			if err != nil {
				klog.Errorf("Cannot get in-sync replicas of topic %s partition %d: %v", topic, partition, err)
			} else {
				ch <- prometheus.MustNewConstMetric(
					topicPartitionInSyncReplicas, prometheus.GaugeValue, float64(len(inSyncReplicas)), topic, strconv.FormatInt(int64(partition), 10),
				)
			}

			if leader != nil && replicas != nil && len(replicas) > 0 && leader.ID() == replicas[0] {
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

	fetchOffsetsFromBroker := func(brokerID int32, req *sarama.OffsetRequest, isNewest bool) {
		broker, err := e.client.Broker(brokerID)
		if err != nil || broker == nil {
			klog.Errorf("Cannot get broker %d (nil: %v): %v", brokerID, broker == nil, err)
			return
		}

		// send the batch request to get topic partition offsets
		resp, err := broker.GetAvailableOffsets(req)
		if err != nil {
			klog.Errorf("Cannot get offsets from broker %d: %v", brokerID, err)
			return
		}

		// parse and report the topic partition offsets
		for topic, partitions := range resp.Blocks {
			for partition, block := range partitions {
				if block.Err != sarama.ErrNoError {
					klog.Errorf("Error fetching offset for %s:%d from broker %d: %v", topic, partition, brokerID, block.Err)
					continue
				}

				val := block.Offsets[0]
				if isNewest {
					e.mu.Lock()
					offset[topic][partition] = val
					e.mu.Unlock()
					ch <- prometheus.MustNewConstMetric(
						topicCurrentOffset, prometheus.GaugeValue, float64(val),
						topic, strconv.FormatInt(int64(partition), 10),
					)
				} else {
					ch <- prometheus.MustNewConstMetric(
						topicOldestOffset, prometheus.GaugeValue, float64(val),
						topic, strconv.FormatInt(int64(partition), 10),
					)
				}
			}
		}
	}

	type offsetFetchTask struct {
		brokerID int32
		request  *sarama.OffsetRequest
		isNewest bool
	}

	offsetFetchTasks := make([]offsetFetchTask, 0, len(brokerNewestOffsetRequests)+len(brokerOldestOffsetRequests))
	for bid, req := range brokerNewestOffsetRequests {
		offsetFetchTasks = append(offsetFetchTasks, offsetFetchTask{brokerID: bid, request: req, isNewest: true})
	}
	for bid, req := range brokerOldestOffsetRequests {
		offsetFetchTasks = append(offsetFetchTasks, offsetFetchTask{brokerID: bid, request: req, isNewest: false})
	}

	if workerCount := boundedWorkerCount(len(offsetFetchTasks), e.topicWorkers); workerCount > 0 {
		var offsetWg sync.WaitGroup
		sem := make(chan struct{}, workerCount)
		for _, task := range offsetFetchTasks {
			offsetWg.Add(1)
			sem <- struct{}{}
			go func(task offsetFetchTask) {
				defer offsetWg.Done()
				defer func() { <-sem }()
				fetchOffsetsFromBroker(task.brokerID, task.request, task.isNewest)
			}(task)
		}
		offsetWg.Wait()
	}

	if e.useZooKeeperLag {
		ConsumerGroups, err := e.zookeeperClient.Consumergroups()
		if err != nil {
			klog.Errorf("Cannot get consumer group %v", err)
		} else {
			for _, group := range ConsumerGroups {
				for topic, partitions := range offset {
					for partition := range partitions {
						zkOffset, _ := group.FetchOffset(topic, partition)
						if zkOffset > 0 {
							e.mu.RLock()
							currentOffset := offset[topic][partition]
							e.mu.RUnlock()
							consumerGroupLag := currentOffset - zkOffset
							ch <- prometheus.MustNewConstMetric(
								consumergroupLagZookeeper, prometheus.GaugeValue, float64(consumerGroupLag),
								group.Name, topic, strconv.FormatInt(int64(partition), 10),
							)
						}
					}
				}
			}
		}
	}

	klog.V(DEBUG).Infof("Phase 1 (Topic Offsets) took %v", time.Since(phase1Start))

	// ==================== Phase 2: Collect Group Metrics and Calculate Lag ====================
	phase2Start := time.Now()
	klog.V(DEBUG).Info("Phase 2: Fetching consumer group offsets and calculating lag")

	var cgWg sync.WaitGroup
	var deferredTasks []*deferredGroupTask
	var tasksMu sync.Mutex

	processConsumerGroup := func(broker *sarama.Broker) {
		if err := broker.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
			klog.Errorf("Cannot connect to broker %d: %v", broker.ID(), err)
			return
		}
		defer broker.Close()

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			klog.Errorf("Cannot get consumer group: %v", err)
			return
		}
		groupIds := make([]string, 0)
		for groupId := range groups.Groups {
			if e.groupFilter.MatchString(groupId) && !e.groupExclude.MatchString(groupId) {
				groupIds = append(groupIds, groupId)
			}
		}

		describeGroups, err := broker.DescribeGroups(&sarama.DescribeGroupsRequest{Groups: groupIds})
		if err != nil {
			klog.Errorf("Cannot get describe groups: %v", err)
			return
		}
		for _, group := range describeGroups.Groups {
			if group.Err != 0 {
				klog.Errorf("Cannot describe for the group %s with error code %d", group.GroupId, group.Err)
				continue
			}

			// calculate and export the group metrics; groups with negative lag are deferred for later processing
			task := e.emitGroupMetric(group, broker, offset, ch)
			if task != nil {
				tasksMu.Lock()
				deferredTasks = append(deferredTasks, task)
				tasksMu.Unlock()
			}
		}
	}

	if len(e.client.Brokers()) > 0 {
		uniqueBrokerAddresses := make(map[string]bool)
		var servers []*sarama.Broker
		for _, broker := range e.client.Brokers() {
			normalizedAddress := strings.ToLower(broker.Addr())
			if !uniqueBrokerAddresses[normalizedAddress] {
				uniqueBrokerAddresses[normalizedAddress] = true
				servers = append(servers, broker)
			}
		}

		groupWorkerCount := boundedWorkerCount(len(servers), e.groupWorkers)
		groupSem := make(chan struct{}, groupWorkerCount)
		for _, broker := range servers {
			cgWg.Add(1)
			groupSem <- struct{}{}
			go func(broker *sarama.Broker) {
				defer cgWg.Done()
				defer func() { <-groupSem }()
				processConsumerGroup(broker)
			}(broker)
		}

		cgDone := make(chan struct{})
		go func() {
			cgWg.Wait()
			close(cgDone)
		}()
		if e.groupMetricsTimeout > 0 {
			select {
			case <-cgDone:
			case <-time.After(e.groupMetricsTimeout):
				klog.Errorf("Consumer group metrics exceeded timeout %v; waiting for in-flight requests to finish", e.groupMetricsTimeout)
				<-cgDone
			}
		} else {
			<-cgDone
		}
		klog.V(DEBUG).Info("All processConsumerGroup goroutines completed")
	} else {
		klog.Errorln("No valid broker, cannot get consumer group metrics")
	}

	klog.V(DEBUG).Infof("Phase 2 (Consumer Group Offsets + Lag) took %v", time.Since(phase2Start))

	// ==================== Phase 3: Process the case if the group lag is negative ====================
	phase3Start := time.Now()
	if len(deferredTasks) > 0 {
		klog.V(DEBUG).Infof("Phase 3: Processing %d groups with negative lag", len(deferredTasks))

		// 1. summarize all the topic partitions that need to be re-fetched
		toRefresh := make(map[int32]*sarama.OffsetRequest)
		for _, task := range deferredTasks {
			for topic, partitions := range task.blocks {
				for partition, block := range partitions {
					e.mu.RLock()
					bid, tracked := topicPartitionLeaders[topic][partition]
					lag := offset[topic][partition] - block.Offset
					e.mu.RUnlock()

					// Consumer groups may include topics excluded from Phase 1.
					// Skip them because no cached offset or leader is available.
					if tracked && block.Offset != -1 && lag < 0 {
						if _, ok := toRefresh[bid]; !ok {
							toRefresh[bid] = &sarama.OffsetRequest{}
						}
						toRefresh[bid].AddBlock(topic, partition, sarama.OffsetNewest, 1)
					}
				}
			}
		}

		// 2. send the batch request to get the new topic partition offsets and update the global offsetMap
		var refreshWg sync.WaitGroup
		for bid, req := range toRefresh {
			refreshWg.Add(1)
			go func(id int32, r *sarama.OffsetRequest) {
				defer refreshWg.Done()
				b, err := e.client.Broker(id)
				if err != nil || b == nil {
					klog.Errorf("Cannot get broker %d (it might be nil): %v", id, err)
					return
				}

				if ok, _ := b.Connected(); !ok {
					if err := b.Open(e.client.Config()); err != nil && err != sarama.ErrAlreadyConnected {
						klog.Errorf("Cannot open broker %d: %v", id, err)
						return
					}
				}
				resp, err := b.GetAvailableOffsets(r)
				if err != nil {
					klog.Errorf("Cannot get available offsets from broker %d: %v", id, err)
					return
				}
				for t, ps := range resp.Blocks {
					for p, block := range ps {
						if block.Err == sarama.ErrNoError {
							e.mu.Lock()
							offset[t][p] = block.Offsets[0]
							e.mu.Unlock()
						}
					}
				}
			}(bid, req)
		}
		refreshWg.Wait()

		// 3. iterate through all the deferred tasks again, and report the metrics using the updated offsetMap
		for _, task := range deferredTasks {
			e.reportGroupMetrics(task.group.GroupId, task.blocks, offset, ch)
		}
		klog.V(DEBUG).Infof("Phase 3 (Refresh negative lag offsets) took %v", time.Since(phase3Start))
	}
}

func (e *Exporter) emitGroupMetric(group *sarama.GroupDescription, broker *sarama.Broker, offsetMap map[string]map[int32]int64, ch chan<- prometheus.Metric) *deferredGroupTask {
	// build the offset fetch request
	offsetFetchRequest := sarama.OffsetFetchRequest{ConsumerGroup: group.GroupId, Version: e.fetchOffsetVersion()}
	if e.offsetShowAll {
		for topic, partitions := range offsetMap {
			for partition := range partitions {
				offsetFetchRequest.AddPartition(topic, partition)
			}
		}
	} else {
		for _, member := range group.Members {
			if len(member.MemberAssignment) == 0 {
				continue
			}
			assignment, err := member.GetMemberAssignment()
			if err != nil {
				continue
			}
			for topic, partitions := range assignment.Topics {
				for _, partition := range partitions {
					offsetFetchRequest.AddPartition(topic, partition)
				}
			}
		}
	}

	ch <- prometheus.MustNewConstMetric(
		consumergroupMembers, prometheus.GaugeValue, float64(len(group.Members)), group.GroupId,
	)

	offsetFetchResponse, err := broker.FetchOffset(&offsetFetchRequest)
	if err != nil {
		klog.Errorf("Cannot get offset of group %s: %v", group.GroupId, err)
		return nil
	}

	hasNegativeLag := false
	for topic, partitions := range offsetFetchResponse.Blocks {
		for partition, block := range partitions {
			e.mu.RLock()
			cachedOffset := offsetMap[topic][partition]
			e.mu.RUnlock()
			if block.Offset != -1 && cachedOffset-block.Offset < 0 {
				hasNegativeLag = true
				break
			}
		}
		if hasNegativeLag {
			break
		}
	}

	if hasNegativeLag {
		// 发现负 Lag，返回任务供后续批量处理
		return &deferredGroupTask{
			group:  group,
			blocks: offsetFetchResponse.Blocks,
		}
	}

	// 如果没有负 Lag，执行监控上报逻辑
	e.reportGroupMetrics(group.GroupId, offsetFetchResponse.Blocks, offsetMap, ch)
	return nil
}

// reportGroupMetrics 统一上报消费组指标
func (e *Exporter) reportGroupMetrics(
	groupId string,
	blocks map[string]map[int32]*sarama.OffsetFetchResponseBlock,
	offsetMap map[string]map[int32]int64, // 各分区的 Topic 最新位点
	ch chan<- prometheus.Metric,
) {
	for topic, partitions := range blocks {
		var currentOffsetSum int64
		var lagSum int64
		topicConsumed := false

		for _, block := range partitions {
			if block.Offset != -1 {
				topicConsumed = true
				break
			}
		}
		if !topicConsumed {
			continue
		}

		for partition, block := range partitions {
			if block.Err != sarama.ErrNoError {
				klog.Errorf("Error for partition %d: %v", partition, block.Err.Error())
				continue
			}

			currentOffset := block.Offset
			if currentOffset != -1 {
				currentOffsetSum += currentOffset
			}

			ch <- prometheus.MustNewConstMetric(
				consumergroupCurrentOffset, prometheus.GaugeValue, float64(currentOffset),
				groupId, topic, strconv.FormatInt(int64(partition), 10),
			)

			var lag int64
			if block.Offset == -1 {
				lag = -1
			} else {
				e.mu.RLock()
				cachedOffset, _ := offsetMap[topic][partition]
				e.mu.RUnlock()

				lag = cachedOffset - block.Offset

				lagSum += lag
			}
			ch <- prometheus.MustNewConstMetric(
				consumergroupLag, prometheus.GaugeValue, float64(lag),
				groupId, topic, strconv.FormatInt(int64(partition), 10),
			)
		}

		if topicConsumed {
			ch <- prometheus.MustNewConstMetric(
				consumergroupCurrentOffsetSum, prometheus.GaugeValue, float64(currentOffsetSum),
				groupId, topic,
			)
			ch <- prometheus.MustNewConstMetric(
				consumergroupLagSum, prometheus.GaugeValue, float64(lagSum),
				groupId, topic,
			)
		}
	}
}

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(versionCollector.NewCollector("kafka_exporter"))
}

//func toFlag(name string, help string) *kingpin.FlagClause {
//	flag.CommandLine.String(name, "", help) // hack around flag.Parse and klog.init flags
//	return kingpin.Flag(name, help)
//}

// hack around flag.Parse and klog.init flags
func toFlagString(name string, help string, value string) *string {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	return kingpin.Flag(name, help).Default(value).String()
}

func toFlagBool(name string, help string, value bool, valueString string) *bool {
	flag.CommandLine.Bool(name, value, help) // hack around flag.Parse and klog.init flags
	return kingpin.Flag(name, help).Default(valueString).Bool()
}

func toFlagStringsVar(name string, help string, value string, target *[]string) {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(value).StringsVar(target)
}

func toFlagStringVar(name string, help string, value string, target *string) {
	flag.CommandLine.String(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(value).StringVar(target)
}

func toFlagBoolVar(name string, help string, value bool, valueString string, target *bool) {
	flag.CommandLine.Bool(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(valueString).BoolVar(target)
}

func toFlagIntVar(name string, help string, value int, valueString string, target *int) {
	flag.CommandLine.Int(name, value, help) // hack around flag.Parse and klog.init flags
	kingpin.Flag(name, help).Default(valueString).IntVar(target)
}

func main() {
	var (
		listenAddress = toFlagString("web.listen-address", "Address to listen on for web interface and telemetry.", ":9308")
		metricsPath   = toFlagString("web.telemetry-path", "Path under which to expose metrics.", "/metrics")
		topicFilter   = toFlagString("topic.filter", "Regex that determines which topics to collect.", ".*")
		topicExclude  = toFlagString("topic.exclude", "Regex that determines which topics to exclude.", "^$")
		groupFilter   = toFlagString("group.filter", "Regex that determines which consumer groups to collect.", ".*")
		groupExclude  = toFlagString("group.exclude", "Regex that determines which consumer groups to exclude.", "^$")
		logSarama     = toFlagBool("log.enable-sarama", "Turn on Sarama logging, default is false.", false, "false")

		opts = kafkaOpts{}
	)

	toFlagStringsVar("kafka.server", "Address (host:port) of Kafka server.", "kafka:9092", &opts.uri)
	toFlagBoolVar("sasl.enabled", "Connect using SASL/PLAIN, default is false.", false, "false", &opts.useSASL)
	toFlagBoolVar("sasl.handshake", "Only set this to false if using a non-Kafka SASL proxy, default is true.", true, "true", &opts.useSASLHandshake)
	toFlagStringVar("sasl.username", "SASL user name.", "", &opts.saslUsername)
	toFlagStringVar("sasl.password", "SASL user password.", "", &opts.saslPassword)
	toFlagStringVar("sasl.aws-region", "The AWS region for IAM SASL authentication", os.Getenv("AWS_REGION"), &opts.saslAwsRegion)
	toFlagStringVar("sasl.oauthbearer-token-url", "The url to retrieve OAuthBearer tokens from, for OAuthBearer SASL authentication", "", &opts.saslOAuthBearerTokenUrl)
	toFlagStringVar("sasl.oauthbearer-scopes", "The comma-separated scopes to use for OAuthBearer SASL authentication", "", &opts.saslOAuthBearerScopes)
	toFlagStringVar("sasl.mechanism", "SASL SCRAM SHA algorithm: sha256 or sha512 or SASL mechanism: gssapi, awsiam or oauthbearer", "", &opts.saslMechanism)
	toFlagStringVar("sasl.service-name", "Service name when using kerberos Auth", "", &opts.serviceName)
	toFlagStringVar("sasl.kerberos-config-path", "Kerberos config path", "", &opts.kerberosConfigPath)
	toFlagStringVar("sasl.realm", "Kerberos realm", "", &opts.realm)
	toFlagStringVar("sasl.kerberos-auth-type", "Kerberos auth type. Either 'keytabAuth' or 'userAuth'", "", &opts.kerberosAuthType)
	toFlagStringVar("sasl.keytab-path", "Kerberos keytab file path", "", &opts.keyTabPath)
	toFlagBoolVar("sasl.disable-PA-FX-FAST", "Configure the Kerberos client to not use PA_FX_FAST, default is false.", false, "false", &opts.saslDisablePAFXFast)
	toFlagBoolVar("tls.enabled", "Connect to Kafka using TLS, default is false.", false, "false", &opts.useTLS)
	toFlagStringVar("tls.server-name", "Used to verify the hostname on the returned certificates unless tls.insecure-skip-tls-verify is given. The kafka server's name should be given.", "", &opts.tlsServerName)
	toFlagStringVar("tls.ca-file", "The optional certificate authority file for Kafka TLS client authentication.", "", &opts.tlsCAFile)
	toFlagStringVar("tls.cert-file", "The optional certificate file for Kafka client authentication.", "", &opts.tlsCertFile)
	toFlagStringVar("tls.key-file", "The optional key file for Kafka client authentication.", "", &opts.tlsKeyFile)
	toFlagBoolVar("server.tls.enabled", "Enable TLS for web server, default is false.", false, "false", &opts.serverUseTLS)
	toFlagBoolVar("server.tls.mutual-auth-enabled", "Enable TLS client mutual authentication, default is false.", false, "false", &opts.serverMutualAuthEnabled)
	toFlagStringVar("server.tls.ca-file", "The certificate authority file for the web server.", "", &opts.serverTlsCAFile)
	toFlagStringVar("server.tls.cert-file", "The certificate file for the web server.", "", &opts.serverTlsCertFile)
	toFlagStringVar("server.tls.key-file", "The key file for the web server.", "", &opts.serverTlsKeyFile)
	toFlagBoolVar("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure. Default is false", false, "false", &opts.tlsInsecureSkipTLSVerify)
	toFlagStringVar("kafka.version", "Kafka broker version", sarama.V2_0_0_0.String(), &opts.kafkaVersion)
	toFlagBoolVar("use.consumelag.zookeeper", "if you need to use a group from zookeeper, default is false", false, "false", &opts.useZooKeeperLag)
	toFlagStringsVar("zookeeper.server", "Address (hosts) of zookeeper server.", "localhost:2181", &opts.uriZookeeper)
	toFlagStringVar("kafka.labels", "Kafka cluster name", "", &opts.labels)
	toFlagStringVar("refresh.metadata", "Metadata refresh interval", "30s", &opts.metadataRefreshInterval)
	toFlagBoolVar("offset.show-all", "Whether show the offset/lag for all consumer group, otherwise, only show connected consumer groups, default is true", true, "true", &opts.offsetShowAll)
	toFlagBoolVar("concurrent.enable", "If true, all scrapes will trigger kafka operations otherwise, they will share results. WARN: This should be disabled on large clusters. Default is false", false, "false", &opts.allowConcurrent)
	toFlagIntVar("topic.workers", "Maximum number of concurrent broker offset fetch tasks; <= 0 means no limit", 100, "100", &opts.topicWorkers)
	toFlagIntVar("group.workers", "Maximum number of concurrent broker consumer group tasks; <= 0 means no limit", 100, "100", &opts.groupWorkers)
	toFlagBoolVar("kafka.allow-auto-topic-creation", "If true, the broker may auto-create topics that we requested which do not already exist, default is false.", false, "false", &opts.allowAutoTopicCreation)
	toFlagIntVar("verbosity", "Verbosity log level", 0, "0", &opts.verbosityLogLevel)
	toFlagStringVar("group.metrics.timeout", "Timeout for emitting consumer group metrics", "5m", &opts.groupMetricsTimeout)

	plConfig := plog.Config{}
	plogflag.AddFlags(kingpin.CommandLine, &plConfig)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

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

	setup(*listenAddress, *metricsPath, *topicFilter, *topicExclude, *groupFilter, *groupExclude, *logSarama, opts, labels)
}

func setup(
	listenAddress string,
	metricsPath string,
	topicFilter string,
	topicExclude string,
	groupFilter string,
	groupExclude string,
	logSarama bool,
	opts kafkaOpts,
	labels map[string]string,
) {
	klog.InitFlags(flag.CommandLine)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Errorf("Error on setting logtostderr to true: %v", err)
	}
	err := flag.Set("v", strconv.Itoa(opts.verbosityLogLevel))
	if err != nil {
		klog.Errorf("Error on setting v to %v: %v", strconv.Itoa(opts.verbosityLogLevel), err)
	}
	defer klog.Flush()

	klog.V(INFO).Infoln("Starting kafka_exporter", version.Info())
	klog.V(DEBUG).Infoln("Build context", version.BuildContext())

	clusterBrokers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "brokers"),
		"Number of Brokers in the Kafka Cluster.",
		nil, labels,
	)
	clusterBrokerInfo = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "broker_info"),
		"Information about the Kafka Broker.",
		[]string{"id", "address"}, labels,
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

	consumergroupLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag"),
		"Current Approximate Lag of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, labels,
	)

	consumergroupLagZookeeper = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroupzookeeper", "lag_zookeeper"),
		"Current Approximate Lag(zookeeper) of a ConsumerGroup at Topic/Partition",
		[]string{"consumergroup", "topic", "partition"}, nil,
	)

	consumergroupLagSum = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "lag_sum"),
		"Current Approximate Lag of a ConsumerGroup at Topic for all partitions",
		[]string{"consumergroup", "topic"}, labels,
	)

	consumergroupMembers = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "consumergroup", "members"),
		"Amount of members in a consumer group",
		[]string{"consumergroup"}, labels,
	)

	if logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	exporter, err := NewExporter(opts, topicFilter, topicExclude, groupFilter, groupExclude)
	if err != nil {
		klog.Fatalln(err)
	}
	defer exporter.client.Close()
	prometheus.MustRegister(exporter)

	mux := http.NewServeMux()

	mux.Handle(metricsPath, promhttp.Handler())
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`<html>
	        <head><title>Kafka Exporter</title></head>
	        <body>
	        <h1>Kafka Exporter</h1>
	        <p><a href='` + metricsPath + `'>Metrics</a></p>
	        </body>
	        </html>`))
		if err != nil {
			klog.Error("Error handle / request", err)
		}
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// need more specific sarama check
		_, err := w.Write([]byte("ok"))
		if err != nil {
			klog.Error("Error handle /healthz request", err)
		}
	})

	if opts.serverUseTLS {
		klog.V(INFO).Infoln("Listening on HTTPS", listenAddress)

		_, err := CanReadCertAndKey(opts.serverTlsCertFile, opts.serverTlsKeyFile)
		if err != nil {
			klog.Error("error reading server cert and key")
		}

		clientAuthType := tls.NoClientCert
		if opts.serverMutualAuthEnabled {
			clientAuthType = tls.RequireAndVerifyClientCert
		}

		certPool := x509.NewCertPool()
		if opts.serverTlsCAFile != "" {
			if caCert, err := os.ReadFile(opts.serverTlsCAFile); err == nil {
				certPool.AppendCertsFromPEM(caCert)
			} else {
				klog.Error("error reading server ca")
			}
		}

		tlsConfig := &tls.Config{
			ClientCAs:        certPool,
			ClientAuth:       clientAuthType,
			MinVersion:       tls.VersionTLS12,
			CurvePreferences: []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
			CipherSuites: []uint16{
				tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
				tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
				tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
				tls.TLS_RSA_WITH_AES_256_CBC_SHA,
				tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
			},
		}
		server := &http.Server{
			Addr:      listenAddress,
			Handler:   mux,
			TLSConfig: tlsConfig,
		}
		klog.Fatal(server.ListenAndServeTLS(opts.serverTlsCertFile, opts.serverTlsKeyFile))
	}

	server := &http.Server{
		Addr:    listenAddress,
		Handler: mux,
	}

	klog.V(INFO).Infoln("Listening on HTTP", listenAddress)
	klog.Fatal(server.ListenAndServe())
}
