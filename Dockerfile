FROM golang:1.14
MAINTAINER  Daniel Qian <qsj.daniel@gmail.com>

WORKDIR /go/src/app
COPY . .

RUN go build -mod=vendor

EXPOSE     9308
ENTRYPOINT [ "./kafka_exporter" ]
