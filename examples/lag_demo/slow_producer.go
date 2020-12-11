package main

import (
	"github.com/Shopify/sarama"
	plog "github.com/prometheus/common/log"
	"os"
	"os/signal"
	"sync"
	"time"
)

func slowProducer(wg sync.WaitGroup) {
	defer wg.Done()
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			plog.Fatalf("Error closing producer: %s", err.Error())
		}
	}()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	counter := 0
	var enqueued, producerErrors int
ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "test", Key: nil, Value: sarama.StringEncoder("testing 123")}:
			enqueued++
			counter++
			if counter >= 50 {
				counter = 0
				time.Sleep(1 * time.Second)
				plog.Debugf("Pausing producer for one second to throttle message production")
			}
		case err := <-producer.Errors():
			plog.Infof("Failed to produce message", err)
			producerErrors++
		case <-signals:
			break ProducerLoop
		}
	}

	plog.Infof("Enqueued: %d; errors: %d\n", enqueued, producerErrors)
}
