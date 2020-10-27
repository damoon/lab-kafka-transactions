package streams

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	setupTimeout = 60 * time.Second
	channelCap   = 100
)

// KafkaKey is a key of a kafka message.
type KafkaKey []byte

// StreamingApplication allows exactly once processing between Kafka streams.
type StreamingApplication struct {
	applicationName, instance, brokers string

	consumer      *kafka.Consumer
	producer      *kafka.Producer
	subscriptions map[string]topic
	commits       chan interface{}
}

type topic struct {
	ch       <-chan kafka.Message
	commitCh <-chan interface{}
}

// NewStreamingApplication initilizes a new streaming application.
func NewStreamingApplication(applicationName, instance, brokers string) *StreamingApplication {
	return &StreamingApplication{
		applicationName: applicationName,
		instance:        instance,
		brokers:         brokers,

		subscriptions: map[string]topic{},
		commits:       make(chan interface{}),
	}
}

func initTransaction(p *kafka.Producer) error {
	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

retryInitTransaction:
	err := p.InitTransactions(ctx)
	if err == nil {
		return nil
	}

	if err.(kafka.Error).IsRetriable() {
		goto retryInitTransaction
	}

	return err
}

// Run streams messages from Kafka to Kafka using exactly once semantics.
func (s *StreamingApplication) Run() error {
	groupID := s.applicationName
	instanceID := groupID + "-" + s.instance

	consumerConfig := &kafka.ConfigMap{
		// General
		"bootstrap.servers": s.brokers,
		"client.id":         instanceID,

		// Consumer
		"group.id":                        groupID,
		"group.instance.id":               instanceID,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"auto.offset.reset":               "earliest",
		"enable.auto.commit":              false,
		"isolation.level":                 "read_committed",
	}

	producerConfig := &kafka.ConfigMap{
		// General
		"bootstrap.servers": s.brokers,
		"client.id":         instanceID,

		// Producer
		"enable.idempotence":     true,
		"transaction.timeout.ms": 10_000,
		"transactional.id":       instanceID,
		"compression.codec":      "zstd",
	}

	p, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return fmt.Errorf("create producer: %v", err)
	}

	defer func() {
		p.Close()
	}()

	err = initTransaction(p)
	if err != nil {
		return fmt.Errorf("init transactional producer: %v", err)
	}

	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return fmt.Errorf("create consumer: %s", err)
	}

	defer func() {
		s.producer.Close()
		err := s.consumer.Close()
		if err != nil {
			// TODO handle error
			fmt.Errorf("close consumer: %v", err)
		}
	}()

	s.consumer = c
	s.producer = p

	topicNames := []string{}
	for topicName := range s.subscriptions {
		topicNames = append(topicNames, topicName)
	}

	err = c.SubscribeTopics(topicNames, nil)
	if err != nil {
		return fmt.Errorf("subscribe topics: %v", err)
	}

	_ = p

	// go func() {
	// 	for i := 0; i < max; i++ {
	// 		ch <- IntIntMsg{
	// 			Key:   i,
	// 			Value: i,
	// 		}
	// 	}
	// 	close(ch)
	// 	commitCh <- struct{}{}
	// 	close(commitCh)
	// }()

	return nil
}
