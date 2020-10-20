package streams

import (
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Topic struct {
	Stream
}

func ReadTopic(topicName string, cap int) (Topic, error) {
	topic := Topic{}

	hostname, err := os.Hostname()
	if err != nil {
		return topic, fmt.Errorf("Failed to look up hostname: %v", err)
	}

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":  "host1:9092,host2:9092",
		"client.id":          hostname,
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return topic, fmt.Errorf("Failed to create consumer: %v", err)
	}

	err = c.Subscribe(topicName, groupRebalance)
	if err != nil {
		return topic, fmt.Errorf("Failed to subscribe to topic: %v", err)
	}

	ch := make(chan Msg, cap)
	topic.Stream = Stream{
		ch: ch,
	}

	return topic, nil
}

func groupRebalance(consumer *kafka.Consumer, event kafka.Event) error {
	return nil
	//	switch e := event.(type) {
	//	case kafka.AssignedPartitions:
	//		// Create a producer per input partition.
	//		for _, tp := range e.Partitions {
	//			err := createTransactionalProducer(tp)
	//			if err != nil {
	//				fatal(err)
	//			}
	//		}
	//
	//		err := consumer.Assign(e.Partitions)
	//		if err != nil {
	//			fatal(err)
	//		}
	//
	//	case kafka.RevokedPartitions:
	//		// Abort any current transactions and close the
	//		// per-partition producers.
	//		for _, producer := range producers {
	//			err := destroyTransactionalProducer(producer)
	//			if err != nil {
	//				fatal(err)
	//			}
	//		}
	//
	//		// Clear producer and intersection states
	//		producers = make(map[int32]*kafka.Producer)
	//		intersectionStates = make(map[string]*intersectionState)
	//
	//		err := consumer.Unassign()
	//		if err != nil {
	//			fatal(err)
	//		}
	//	}
	//
	//	return nil
}

//
//func (s Stream) WriteToTopic(name string) {
//
//}
//
