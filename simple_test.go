package main

import (
	"errors"
	"github.com/Shopify/sarama"
	"io"
	"log"
	"net/http"
	"testing"
	"time"
)

var bootstrap_servers = []string{"localhost:9092"}

func TestSmoke(t *testing.T) {
	log.Print("testing " + t.Name())

	if !assumeKafka(t) {
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

func assumeKafka(t *testing.T) bool {
	client, err := sarama.NewClient(bootstrap_servers, nil)
	if err != nil {
		return false
	}
	defer client.Close()
	_, err = client.Topics()

	return err == nil
}

func execute(handler func(response *http.Response)) {
	var e = errors.New("dummy")
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
	setup("localhost:9304", "/metrics", ".*", ".*", false, opts, nil)
}
