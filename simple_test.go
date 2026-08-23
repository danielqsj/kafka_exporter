package main

import (
	"errors"
	"io"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

var bootstrap_servers = []string{"localhost:9092"}

func TestSmoke(t *testing.T) {
	log.Print("testing " + t.Name())

	if !assumeKafka() {
		t.Skip("Kafka is not running ... skipping the test")
		return
	}

	go runServer()

	execute(func(resp *http.Response) {
		log.Println(resp.Status)

		defer resp.Body.Close()
		bytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalln(err)
		} else {
			log.Println(string(bytes))
		}
	})
}

func TestBoundedWorkerCount(t *testing.T) {
	tests := []struct {
		name  string
		total int
		limit int
		want  int
	}{
		{name: "no work", total: 0, limit: 100, want: 0},
		{name: "zero limit uses all work", total: 3, limit: 0, want: 3},
		{name: "negative limit uses all work", total: 3, limit: -1, want: 3},
		{name: "limit above work caps at work", total: 3, limit: 100, want: 3},
		{name: "limit below work is used", total: 10, limit: 4, want: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := boundedWorkerCount(tt.total, tt.limit); got != tt.want {
				t.Fatalf("boundedWorkerCount(%d, %d) = %d, want %d", tt.total, tt.limit, got, tt.want)
			}
		})
	}
}

func assumeKafka() bool {
	client, err := sarama.NewClient(bootstrap_servers, nil)
	if err != nil {
		return false
	}
	defer client.Close()
	_, err = client.Topics()
	return err == nil
}

func execute(handler func(response *http.Response)) {
	e := errors.New("dummy")
	for e != nil {
		resp, err := http.Get("http://localhost:9304/metrics")
		if err != nil {
			time.Sleep(time.Millisecond * 100)
		}
		e = err
		if resp != nil {
			handler(resp)
		}
	}
}

func runServer() {
	opts := kafkaOpts{}
	opts.uri = bootstrap_servers
	opts.uriZookeeper = []string{"localhost:2181"}
	opts.kafkaVersion = sarama.V1_0_0_0.String()
	opts.metadataRefreshInterval = "30s"
	setup("localhost:9304", "/metrics", ".*", "^$", ".*", "^$", false, opts, nil)
}
