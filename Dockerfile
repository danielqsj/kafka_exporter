FROM        quay.io/prometheus/busybox:latest
MAINTAINER  Daniel Qian <qsj.daniel@gmail.com>

ARG TARGETARCH
ARG BIN_DIR=.build/linux-${TARGETARCH}/

COPY ${BIN_DIR}/kafka_exporter /bin/kafka_exporter

EXPOSE     9308
USER nobody

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD wget -q -O /dev/null http://localhost:9308/metrics || exit 1

ENTRYPOINT [ "/bin/kafka_exporter" ]
