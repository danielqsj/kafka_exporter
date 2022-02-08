FROM       alpine:3.15
MAINTAINER Maxim Pogozhiy <foxdalas@gmail.com>

COPY kafka-exporter /bin/kafka_exporter

ENTRYPOINT ["/bin/kafka_exporter"]
EXPOSE     9308
