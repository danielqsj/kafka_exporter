package main

import (
	"log"
	"net/http"
	"os"

	"github.com/Shopify/sarama"
	"github.com/davidmparrott/kafka_exporter/v2/exporter"
	klog "github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	plog "github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
	"github.com/rcrowley/go-metrics"
	"gopkg.in/alecthomas/kingpin.v2"
)

func init() {
	metrics.UseNilMetrics = true
	prometheus.MustRegister(version.NewCollector("kafka_exporter"))
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").Default(":9308").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		logSarama     = kingpin.Flag("log.enable-sarama", "Turn on Sarama logging.").Default("false").Bool()
		topicFilter   = kingpin.Flag("topic.filter", "Regex that determines which topics to collect.").Default(".*").String()
		groupFilter   = kingpin.Flag("group.filter", "Regex that determines which consumer groups to collect.").Default(".*").String()
		kafkaConfig   = exporter.Options{}
	)

	kingpin.Flag("kafka.server", "Address (host:port) of Kafka server.").Default("kafka:9092").StringsVar(&kafkaConfig.Uri)
	kingpin.Flag("sasl.enabled", "Connect using SASL/PLAIN.").Default("false").BoolVar(&kafkaConfig.UseSASL)
	kingpin.Flag("sasl.handshake", "Only set this to false if using a non-Kafka SASL proxy.").Default("true").BoolVar(&kafkaConfig.UseSASLHandshake)
	kingpin.Flag("sasl.username", "SASL user name.").Default("").StringVar(&kafkaConfig.SaslUsername)
	kingpin.Flag("sasl.password", "SASL user password.").Default("").StringVar(&kafkaConfig.SaslPassword)
	kingpin.Flag("sasl.mechanism", "The SASL SCRAM SHA algorithm sha256 or sha512 as mechanism").Default("").StringVar(&kafkaConfig.SaslMechanism)
	kingpin.Flag("tls.enabled", "Connect using TLS.").Default("false").BoolVar(&kafkaConfig.UseTLS)
	kingpin.Flag("tls.ca-file", "The optional certificate authority file for TLS client authentication.").Default("").StringVar(&kafkaConfig.TlsCAFile)
	kingpin.Flag("tls.cert-file", "The optional certificate file for client authentication.").Default("").StringVar(&kafkaConfig.TlsCertFile)
	kingpin.Flag("tls.key-file", "The optional key file for client authentication.").Default("").StringVar(&kafkaConfig.TlsKeyFile)
	kingpin.Flag("tls.insecure-skip-tls-verify", "If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure.").Default("false").BoolVar(&kafkaConfig.TlsInsecureSkipTLSVerify)
	kingpin.Flag("kafka.version", "Kafka broker version").Default(sarama.V2_0_0_0.String()).StringVar(&kafkaConfig.KafkaVersion)
	kingpin.Flag("use.consumelag.zookeeper", "if you need to use a group from zookeeper").Default("false").BoolVar(&kafkaConfig.UseZooKeeperLag)
	kingpin.Flag("zookeeper.server", "Address (hosts) of zookeeper server.").Default("localhost:2181").StringsVar(&kafkaConfig.UriZookeeper)
	kingpin.Flag("kafka.labels", "Kafka cluster name").Default("").StringVar(&kafkaConfig.Labels)
	kingpin.Flag("refresh.metadata", "Metadata refresh interval").Default("1m").StringVar(&kafkaConfig.MetadataRefreshInterval)
	kingpin.Flag("concurrent.enable", "If true, all scrapes will trigger kafka operations otherwise, they will share results. WARN: This should be disabled on large clusters").Default("false").BoolVar(&kafkaConfig.AllowConcurrent)
	kingpin.Flag("max.offsets", "Maximum number of offsets to store in the interpolation table for a partition").Default("1000").IntVar(&kafkaConfig.MaxOffsets)
	kingpin.Flag("prune.interval", "How frequently should the interpolation table be pruned, in seconds").Default("30").IntVar(&kafkaConfig.PruneIntervalSeconds)

	plog.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("kafka_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	plog.Infoln("Starting kafka_exporter", version.Info())
	plog.Infoln("Build context", version.BuildContext())

	if *logSarama {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	w := klog.NewSyncWriter(os.Stdout)
	logger := klog.NewLogfmtLogger(w)

	newExporter, err := exporter.New(logger, kafkaConfig, *topicFilter, *groupFilter)
	if err != nil {
		plog.Fatalln(err)
	}
	defer newExporter.Close()
	prometheus.MustRegister(newExporter)

	quitChannel := make(chan struct{})
	go newExporter.RunPruner(quitChannel)
	defer close(quitChannel)

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
