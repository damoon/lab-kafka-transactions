//go:generate ../bin/genny -in=../stateless.go -out=kafka_stream.go gen "KeyType=KafkaKey ValueType=KafkaValue"

//go:generate sed -i "s/KafkaKeyKafkaValueStream/Topic/g" kafka_stream.go
//go:generate sed -i "s/KafkaKeyKafkaValueMsg/KafkaMsg/g" kafka_stream.go

//go:generate sed -i "s/package streams/package example/" kafka_stream.go
// TODO check out gorename
// TODO check out mergol

package example

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

type KafkaKey []byte
type KafkaValue []byte

// StreamingApplication allows exactly once processing between Kafka streams.
type StreamingApplication struct {
	consumer      *kafka.Consumer
	producer      *kafka.Producer
	subscriptions map[string]Topic
}

func NewStreamingApplication(applicationName, instance, brokers string) (*StreamingApplication, error) {
	groupID := applicationName
	instanceID := groupID + "-" + instance

	consumerConfig := &kafka.ConfigMap{
		// General
		"bootstrap.servers": brokers,
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
		"bootstrap.servers": brokers,
		"client.id":         instanceID,

		// Producer
		"enable.idempotence":     true,
		"transaction.timeout.ms": 10_000,
		"transactional.id":       instanceID,
		"compression.codec":      "zstd",
	}

	p, err := kafka.NewProducer(producerConfig)
	if err != nil {
		return nil, fmt.Errorf("create producer: %v", err)
	}

	defer func() {
		p.Close()
	}()

	err = initTransaction(p)
	if err != nil {
		return nil, fmt.Errorf("init transactional producer: %v", err)
	}

	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %s", err)
	}

	defer func() {
		err := c.Close()
		if err != nil {
			fmt.Printf("close consumer: %v\n", err)
		}
	}()

	return &StreamingApplication{
		consumer:      c,
		producer:      p,
		subscriptions: make(map[string]Topic, 1),
	}, nil
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

// Close shuts down the application.
func (s StreamingApplication) Close() error {
	s.producer.Close()
	err := s.consumer.Close()
	if err != nil {
		return fmt.Errorf("close consumer: %v", err)
	}

	return nil
}

// StreamTopic subscribes to a topic and streams its messages.
func (s StreamingApplication) StreamTopic(topicName string) Topic {
	ch := make(chan KafkaMsg, channelCap)
	commitCh := make(chan interface{}, 1)
	stream := Topic{
		ch:       ch,
		commitCh: commitCh,
	}

	s.subscriptions[topicName] = stream
	return stream
}

// WriteTo persists messages to a topic.
func (t Topic) WriteTo(s *StreamingApplication, topicName string) Topic {

	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		msg.Value = m(msg.Value)
		ch <- msg
	}

	return s.Process(task)

	ch := make(chan KafkaMsg, channelCap)
	commitCh := make(chan interface{}, 1)
	stream := Topic{
		ch:       ch,
		commitCh: commitCh,
	}

	s.subscriptions[topicName] = stream
	return stream
}

func (s StreamingApplication) Run() error {
	c := s.consumer
	p := s.producer

	topicNames := []string{}
	for topicName := range s.subscriptions {
		topicNames = append(topicNames, topicName)
	}

	err := c.SubscribeTopics(topicNames, nil)
	if err != nil {
		return fmt.Errorf("subscribe topics: %v", err)
	}

	//go func() {
	//	for i := 0; i < max; i++ {
	//		ch <- IntIntMsg{
	//			Key:   i,
	//			Value: i,
	//		}
	//	}
	//	close(ch)
	//	commitCh <- struct{}{}
	//	close(commitCh)
	//}()

	return nil
}
