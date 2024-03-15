HELM INSTALL
==============

### Install Basic

```shell
helm upgrade -i kafka-exporter kafka_exporter/charts/kafka-exporter --namespace=kafka-exporter --create-namespace \
  --set kafkaExporter.kafka.servers="{kafka1:9092,kafka2:9092,.....}"
```

### Install with Datadog support

```shell
helm upgrade -i kafka-exporter kafka_exporter/charts/kafka-exporter --namespace=kafka-exporter --create-namespace \
  --set kafkaExporter.kafka.servers="{kafka1:9092,kafka2:9092,.....}" \
  --set datadog.prefix=testing-kafka-cluster \
  --set datadog.use_datadog=true \
  --set prometheus.serviceMonitor.enabled=false
```

Sample Datadog collector installation:
```shell
helm repo add datadog https://helm.datadoghq.com
helm repo update

helm upgrade -i datadog datadog/datadog --namespace=kafka-exporter \
  --set datadog.apiKey=<key>  \
  --set targetSystem=linux \
  --set datadog.prometheusScrape.enabled=true \
  --set datadog.prometheusScrape.serviceEndpoints=true
```


### Install with Azure Managed Prometheus support

```shell
helm upgrade -i kafka-exporter kafka_exporter/charts/kafka-exporter --namespace=kafka-exporter --create-namespace \
  --set kafkaExporter.kafka.servers="{kafka1:9092,kafka2:9092,.....}" --set prometheus.serviceMonitor.enabled=true --set azuremanagedprometheus.use_azuremanagedprometheus=true
```
