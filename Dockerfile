FROM        quay.io/prometheus/busybox:latest
MAINTAINER  David Parrott <david@davidmparrott.com>

COPY kafka_exporter /bin/kafka_exporter

EXPOSE     9308
ENTRYPOINT [ "/bin/kafka_exporter" ]