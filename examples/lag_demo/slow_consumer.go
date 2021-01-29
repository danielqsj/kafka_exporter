package main

import (
	"context"
	"github.com/Shopify/sarama"
	plog "github.com/prometheus/common/log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Consumer struct {
	ready chan bool
}

func slowConsumer(wg *sync.WaitGroup) {
	defer wg.Done()
	consumer := Consumer{ready: make(chan bool)}
	ctx := context.Background()

	client, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, "groupie-group", nil)
	if err != nil {
		plog.Fatalf("Error creating consumer group client: %v", err)
	}

	topics := []string{"test"}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {

			if err := client.Consume(ctx, topics, &consumer); err != nil {
				plog.Fatalf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	plog.Infof("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		plog.Infof("terminating: context cancelled")
	case <-sigterm:
		plog.Infof("terminating: via signal")
	}
	wg.Wait()
	if err = client.Close(); err != nil {
		plog.Fatalf("Error closing client: %v", err)
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	plog.Infof("Consuming from test topic")
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	counter := 0
	for message := range claim.Messages() {
		counter++
		if counter >= 5000 {
			plog.Infof("Pausing consumer for 50 seconds")
			time.Sleep(50 * time.Second)
			counter = 0
		}
		plog.Debugf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}
