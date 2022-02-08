FROM       alpine:3.15
MAINTAINER Maxim Pogozhiy <foxdalas@gmail.com>

RUN apk add --no-cache gcompat
COPY kafka-exporter /bin/kafka_exporter

ENTRYPOINT ["/bin/kafka_exporter"]
EXPOSE     9308
