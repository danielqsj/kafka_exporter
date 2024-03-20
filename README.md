![kafka_exporter](https://socialify.git.ci/danielqsj/kafka_exporter/image?description=1&font=Inter&forks=1&pattern=Signal&stargazers=1&theme=Light)

kafka_exporter
==============

[![CI](https://github.com/danielqsj/kafka_exporter/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/danielqsj/kafka_exporter/actions/workflows/ci.yml)[![Docker Pulls](https://img.shields.io/docker/pulls/danielqsj/kafka-exporter.svg)](https://hub.docker.com/r/danielqsj/kafka-exporter)[![Go Report Card](https://goreportcard.com/badge/github.com/danielqsj/kafka_exporter)](https://goreportcard.com/report/github.com/danielqsj/kafka_exporter)[![Language](https://img.shields.io/badge/language-Go-red.svg)](https://github.com/danielqsj/kafka-exporter)[![GitHub release](https://img.shields.io/badge/release-1.7.0-green.svg)](https://github.com/danielqsj/kafka-exporter/releases)[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

Kafka exporter for Prometheus. For other metrics from Kafka, have a look at the [JMX exporter](https://github.com/prometheus/jmx_exporter).

Table of Contents
-----------------

-	[Compatibility](#compatibility)
-	[Dependency](#dependency)
-	[Download](#download)
-	[Compile](#compile)
	-	[Build Binary](#build-binary)
	-	[Build Docker Image](#build-docker-image)
-	[Run](#run)
	-	[Run Binary](#run-binary)
	-	[Run Docker Image](#run-docker-image)
 -	-	[Run Docker Compose](#run-docker-compose)
-	[Flags](#flags)
    -	[Notes](#notes)
-	[Metrics](#metrics)
	-	[Brokers](#brokers)
	-	[Topics](#topics)
	-	[Consumer Groups](#consumer-groups)
-	[Grafana Dashboard](#grafana-dashboard)
-   [Contribute](#contribute)
-   [Donation](#donation)
-   [License](#license)

Compatibility
-------------

Support [Apache Kafka](https://kafka.apache.org) version 0.10.1.0 (and later).

Dependency
----------

-	[Prometheus](https://prometheus.io)
-	[Sarama](https://shopify.github.io/sarama)
-	[Golang](https://golang.org)

Download
--------

Binary can be downloaded from [Releases](https://github.com/danielqsj/kafka_exporter/releases) page.

Compile
-------

### Build Binary

```shell
make
```

### Build Docker Image

```shell
make docker
```

Docker Hub Image
----------------

```shell
docker pull danielqsj/kafka-exporter:latest
```

It can be used directly instead of having to build the image yourself. ([Docker Hub danielqsj/kafka-exporter](https://hub.docker.com/r/danielqsj/kafka-exporter)\)

Run
---

### Run Binary

```shell
kafka_exporter --kafka.server=kafka:9092 [--kafka.server=another-server ...]
```

### Run Docker Image

```
docker run -ti --rm -p 9308:9308 danielqsj/kafka-exporter --kafka.server=kafka:9092 [--kafka.server=another-server ...]
```

### Run Docker Compose
make a `docker-compose.yml` flie
```
services:
  kafka-exporter:
    image: danielqsj/kafka-exporter 
    command: ["--kafka.server=kafka:9092", "[--kafka.server=another-server ...]"]
    ports:
      - 9308:9308     
```
then run it
```
docker-compose up -d
```

Flags
-----

This image is configurable using different flags

| Flag name                      | Default        | Description                                                                                                                                    |
|--------------------------------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| kafka.server                   | kafka:9092     | Addresses (host:port) of Kafka server                                                                                                          |
| kafka.version                  | 2.0.0          | Kafka broker version                                                                                                                           |
| sasl.enabled                   | false          | Connect using SASL/PLAIN                                                                                                                       |
| sasl.handshake                 | true           | Only set this to false if using a non-Kafka SASL proxy                                                                                         |
| sasl.username                  |                | SASL user name                                                                                                                                 |
| sasl.password                  |                | SASL user password                                                                                                                             |
| sasl.mechanism                 |                | The SASL mechanism: gssapi, awsiam or plain or SASL SCRAM SHA algorithm: sha256 or sha512                                                      |
| sasl.aws-region                | AWS_REGION env | The AWS region for IAM SASL authentication                                                                                                     |
| sasl.service-name              |                | Service name when using Kerberos Auth                                                                                                          |
| sasl.kerberos-config-path      |                | Kerberos config path                                                                                                                           |
| sasl.realm                     |                | Kerberos realm                                                                                                                                 |
| sasl.keytab-path               |                | Kerberos keytab file path                                                                                                                      |
| sasl.kerberos-auth-type        |                | Kerberos auth type. Either 'keytabAuth' or 'userAuth'                                                                                          |
| tls.enabled                    | false          | Connect to Kafka using TLS                                                                                                                     |
| tls.server-name                |                | Used to verify the hostname on the returned certificates unless tls.insecure-skip-tls-verify is given. The kafka server's name should be given |
| tls.ca-file                    |                | The optional certificate authority file for Kafka TLS client authentication                                                                    |
| tls.cert-file                  |                | The optional certificate file for Kafka client authentication                                                                                  |
| tls.key-file                   |                | The optional key file for Kafka client authentication                                                                                          |
| tls.insecure-skip-tls-verify   | false          | If true, the server's certificate will not be checked for validity                                                                             |
| server.tls.enabled             | false          | Enable TLS for web server                                                                                                                      |
| server.tls.mutual-auth-enabled | false          | Enable TLS client mutual authentication                                                                                                        |
| server.tls.ca-file             |                | The certificate authority file for the web server                                                                                              |
| server.tls.cert-file           |                | The certificate file for the web server                                                                                                        |
| server.tls.key-file            |                | The key file for the web server                                                                                                                |
| topic.filter                   | .*             | Regex that determines which topics to collect                                                                                                  |
| topic.exclude                  | ^$             | Regex that determines which topics to exclude                                                                                                  |
| group.filter                   | .*             | Regex that determines which consumer groups to collect                                                                                         |
| group.exclude                  | ^$             | Regex that determines which consumer groups to exclude                                                                                         |
| web.listen-address             | :9308          | Address to listen on for web interface and telemetry                                                                                           |
| web.telemetry-path             | /metrics       | Path under which to expose metrics                                                                                                             |
| log.enable-sarama              | false          | Turn on Sarama logging                                                                                                                         |
| use.consumelag.zookeeper       | false          | if you need to use a group from zookeeper                                                                                                      |
| zookeeper.server               | localhost:2181 | Address (hosts) of zookeeper server                                                                                                            |
| kafka.labels                   |                | Kafka cluster name                                                                                                                             |
| refresh.metadata               | 30s            | Metadata refresh interval                                                                                                                      |
| offset.show-all                | true           | Whether show the offset/lag for all consumer group, otherwise, only show connected consumer groups                                             |
| concurrent.enable              | false          | If true, all scrapes will trigger kafka operations otherwise, they will share results. WARN: This should be disabled on large clusters         |
| topic.workers                  | 100            | Number of topic workers                                                                                                                        |
| verbosity                      | 0              | Verbosity log level                                                                                                                            |

### Notes

Boolean values are uniquely managed by [Kingpin](https://github.com/alecthomas/kingpin/blob/master/README.md#boolean-values). Each boolean flag will have a negative complement:
`--<name>` and `--no-<name>`.

For example:

If you need to disable `sasl.handshake`, you could add flag `--no-sasl.handshake`

Metrics
-------

Documents about exposed Prometheus metrics.

For details on the underlying metrics please see [Apache Kafka](https://kafka.apache.org/documentation).

### Brokers

**Metrics details**

| Name            | Exposed informations                   |
|-----------------|----------------------------------------|
| `kafka_brokers` | Number of Brokers in the Kafka Cluster |

**Metrics output example**

```txt
# HELP kafka_brokers Number of Brokers in the Kafka Cluster.
# TYPE kafka_brokers gauge
kafka_brokers 3
```

### Topics

**Metrics details**

| Name                                               | Exposed informations                                |
|----------------------------------------------------|-----------------------------------------------------|
| `kafka_topic_partitions`                           | Number of partitions for this Topic                 |
| `kafka_topic_partition_current_offset`             | Current Offset of a Broker at Topic/Partition       |
| `kafka_topic_partition_oldest_offset`              | Oldest Offset of a Broker at Topic/Partition        |
| `kafka_topic_partition_in_sync_replica`            | Number of In-Sync Replicas for this Topic/Partition |
| `kafka_topic_partition_leader`                     | Leader Broker ID of this Topic/Partition            |
| `kafka_topic_partition_leader_is_preferred`        | 1 if Topic/Partition is using the Preferred Broker  |
| `kafka_topic_partition_replicas`                   | Number of Replicas for this Topic/Partition         |
| `kafka_topic_partition_under_replicated_partition` | 1 if Topic/Partition is under Replicated            |

**Metrics output example**

```txt
# HELP kafka_topic_partitions Number of partitions for this Topic
# TYPE kafka_topic_partitions gauge
kafka_topic_partitions{topic="__consumer_offsets"} 50

# HELP kafka_topic_partition_current_offset Current Offset of a Broker at Topic/Partition
# TYPE kafka_topic_partition_current_offset gauge
kafka_topic_partition_current_offset{partition="0",topic="__consumer_offsets"} 0

# HELP kafka_topic_partition_oldest_offset Oldest Offset of a Broker at Topic/Partition
# TYPE kafka_topic_partition_oldest_offset gauge
kafka_topic_partition_oldest_offset{partition="0",topic="__consumer_offsets"} 0

# HELP kafka_topic_partition_in_sync_replica Number of In-Sync Replicas for this Topic/Partition
# TYPE kafka_topic_partition_in_sync_replica gauge
kafka_topic_partition_in_sync_replica{partition="0",topic="__consumer_offsets"} 3

# HELP kafka_topic_partition_leader Leader Broker ID of this Topic/Partition
# TYPE kafka_topic_partition_leader gauge
kafka_topic_partition_leader{partition="0",topic="__consumer_offsets"} 0

# HELP kafka_topic_partition_leader_is_preferred 1 if Topic/Partition is using the Preferred Broker
# TYPE kafka_topic_partition_leader_is_preferred gauge
kafka_topic_partition_leader_is_preferred{partition="0",topic="__consumer_offsets"} 1

# HELP kafka_topic_partition_replicas Number of Replicas for this Topic/Partition
# TYPE kafka_topic_partition_replicas gauge
kafka_topic_partition_replicas{partition="0",topic="__consumer_offsets"} 3

# HELP kafka_topic_partition_under_replicated_partition 1 if Topic/Partition is under Replicated
# TYPE kafka_topic_partition_under_replicated_partition gauge
kafka_topic_partition_under_replicated_partition{partition="0",topic="__consumer_offsets"} 0
```

### Consumer Groups

**Metrics details**

| Name                                         | Exposed informations                                                     |
|----------------------------------------------|--------------------------------------------------------------------------|
| `kafka_consumergroup_current_offset`         | Current Offset of a ConsumerGroup at Topic/Partition                     |
| `kafka_consumergroup_lag`                    | Current Approximate Lag of a ConsumerGroup at Topic/Partition            |
| `kafka_consumergroupzookeeper_lag_zookeeper` | Current Approximate Lag(zookeeper) of a ConsumerGroup at Topic/Partition |

#### Important Note

To be able to collect the metrics `kafka_consumergroupzookeeper_lag_zookeeper`, you must set the following flags:

* `use.consumelag.zookeeper`: enable collect consume lag from zookeeper
* `zookeeper.server`: address for connection to zookeeper

**Metrics output example**

```txt
# HELP kafka_consumergroup_current_offset Current Offset of a ConsumerGroup at Topic/Partition
# TYPE kafka_consumergroup_current_offset gauge
kafka_consumergroup_current_offset{consumergroup="KMOffsetCache-kafka-manager-3806276532-ml44w",partition="0",topic="__consumer_offsets"} -1

# HELP kafka_consumergroup_lag Current Approximate Lag of a ConsumerGroup at Topic/Partition
# TYPE kafka_consumergroup_lag gauge
kafka_consumergroup_lag{consumergroup="KMOffsetCache-kafka-manager-3806276532-ml44w",partition="0",topic="__consumer_offsets"} 1
```

Grafana Dashboard
-------

Grafana Dashboard ID: 7589, name: Kafka Exporter Overview.

For details of the dashboard please see [Kafka Exporter Overview](https://grafana.com/grafana/dashboards/7589-kafka-exporter-overview/).

Contribute
----------

If you like Kafka Exporter, please give me a star. This will help more people know Kafka Exporter.

Please feel free to send me [pull requests](https://github.com/danielqsj/kafka_exporter/pulls).

Contributors ✨
----------

Thanks goes to these wonderful people:

<a href="https://github.com/danielqsj/kafka_exporter/graphs/contributors">
<img src="https://contrib.rocks/image?repo=danielqsj/kafka_exporter" />
</a>

Star ⭐
----------

[![Stargazers over time](https://starchart.cc/danielqsj/kafka_exporter.svg)](https://starchart.cc/danielqsj/kafka_exporter)

Donation
--------

Your donation will encourage me to continue to improve Kafka Exporter. Support Alipay donation.

![](https://github.com/danielqsj/kafka_exporter/raw/master/alipay.jpg)

License
-------

Code is licensed under the [Apache License 2.0](https://github.com/danielqsj/kafka_exporter/blob/master/LICENSE).
