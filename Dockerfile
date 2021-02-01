FROM        golang:alpine AS build
MAINTAINER  David Parrott <david@davidmparrott.com>

ADD . /src
WORKDIR /src
RUN go build -o /server

FROM golang:alpine
EXPOSE     9308
COPY --from=build /server /
CMD [ "/server" ]