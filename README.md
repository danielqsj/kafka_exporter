kafka_exporter
==============

[![Build Status](https://travis-ci.org/danielqsj/kafka_exporter.svg?branch=master)](https://travis-ci.org/danielqsj/kafka_exporter)[![Docker Pulls](https://img.shields.io/docker/pulls/danielqsj/kafka-exporter.svg)](https://hub.docker.com/r/danielqsj/kafka-exporter)[![Go Report Card](https://goreportcard.com/badge/github.com/danielqsj/kafka_exporter)](https://goreportcard.com/report/github.com/danielqsj/kafka_exporter)

Kafka exporter for Prometheus

Table of Contents
-----------------

-	[Compatibility](#compatibility)
-	[Dependency](#dependency)
-	[Download](#download)
-	[Compile](#compile)
	-	[build binary](#build-binary)
	-	[build docker image](#build-docker-image)
-	[Run](#run)
	-	[run binary](#run-binary)
	-	[run docker image](#run-docker-image)
-	[Options](#options)
-	[Metrics](#metrics)
	-	[Brokers](#brokers)
	-	[Topics](#topics)
	-	[Consumer Groups](#consumer-groups)

Compatibility
-------------
Support [Apache Kafka](https://kafka.apache.org) version 0.10.1.0 (and later).

Dependency
----------

-	[Prometheus](https://prometheus.io)
-	[Golang](https://golang.org)
-	[Dep](https://github.com/golang/dep)

Download
--------

Binary can be downloaded from [Releases](https://github.com/danielqsj/kafka_exporter/releases) page.

Compile
-------

### build binary

```shell
make
```

### build docker image

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

### run binary

```shell
kafka_exporter --kafka.server=kafka:9092
```

### run docker image

```
docker run  -ti --rm danielqsj/kafka-exporter --kafka.server=kafka:9092
```

Flags
-----

This image is configurable using different flags

| Flag name          | Default    | Description                                          |
|--------------------|------------|------------------------------------------------------|
| kafka.server       | kafka:9092 | Address (host:port) of Kafka server                  |
| web.listen-address | :9206      | Address to listen on for web interface and telemetry |
| web.telemetry-path | /metrics   | Path under which to expose metrics                   |

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

| Name                                 | Exposed informations                                          |
|--------------------------------------|---------------------------------------------------------------|
| `kafka_consumergroup_current_offset` | Current Offset of a ConsumerGroup at Topic/Partition          |
| `kafka_consumergroup_lag`            | Current Approximate Lag of a ConsumerGroup at Topic/Partition |

**Metrics output example**

```txt
# HELP kafka_consumergroup_current_offset Current Offset of a ConsumerGroup at Topic/Partition
# TYPE kafka_consumergroup_current_offset gauge
kafka_consumergroup_current_offset{consumergroup="KMOffsetCache-kafka-manager-3806276532-ml44w",partition="0",topic="__consumer_offsets"} -1

# HELP kafka_consumergroup_lag Current Approximate Lag of a ConsumerGroup at Topic/Partition
# TYPE kafka_consumergroup_lag gauge
kafka_consumergroup_lag{consumergroup="KMOffsetCache-kafka-manager-3806276532-ml44w",partition="0",topic="__consumer_offsets"} 1
```
