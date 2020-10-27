package streams

import "github.com/confluentinc/confluent-kafka-go/kafka"

// StreamingApplication gets generated via genny.
type StreamingApplication struct {
	consumer      *kafka.Consumer
	producer      *kafka.Producer
	subscriptions map[string]Topic
}
