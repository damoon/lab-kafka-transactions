package streams

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	setupTimeout       = 60 * time.Second
	channelCap         = 100
	transactionTimeout = 10 * time.Second
	flushTime          = 100 * time.Millisecond
)

// ByteArray is required to allow genny work around the array syntax.
type ByteArray []byte

// StreamingApplication allows exactly once processing between Kafka streams.
type StreamingApplication struct {
	applicationName, instance, brokers string

	consumer      *kafka.Consumer
	producer      *kafka.Producer
	subscriptions map[string]topic
	aggregations  map[string]func(partition int32)

	commits         chan interface{}
	requiredCommits int
}

type topic struct {
	ch       chan<- *kafka.Message
	commitCh chan<- interface{}
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

// Run streams messages from Kafka to Kafka using exactly once semantics.
func (s *StreamingApplication) Run(sigchan <-chan os.Signal) error {
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
		err := c.Close()
		if err != nil {
			log.Fatalf("close consumer: %v", err)
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

	err = s.process(sigchan)
	if err != nil {
		return fmt.Errorf("subscribe topics: %v", err)
	}

	return nil
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

func (s *StreamingApplication) process(sigchan <-chan os.Signal) error {

	run := true

	termCh, doneCh := printEvents(s.producer, &run)
	defer func() {
		termCh <- struct{}{}
		<-doneCh
	}()

	var flush <-chan time.Time
	var flushDeadline time.Time
	var ctx context.Context

	for run {

		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			err := s.commit(ctx, s.consumer, s.producer)
			if err != nil {
				return fmt.Errorf("commit transaction: %v", err)
			}
			run = false

		case <-flush:
			err := s.commit(ctx, s.consumer, s.producer)
			if err != nil {
				return fmt.Errorf("flush time limit reached: commit transaction: %v", err)
			}
			ctx = nil

		default:
			pollTimeMs := 100
			if ctx != nil {
				pollTimeMs = int(time.Until(flushDeadline) / time.Millisecond)
			}

			ev := s.consumer.Poll(pollTimeMs)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// log.Printf("%% Message on %s: %s\n", e.TopicPartition, string(e.Value))
				// if e.Headers != nil {
				// 	log.Printf("%% Headers: %v\n", e.Headers)
				// }

				if ctx == nil {
				beginTransaction:
					// log.Println("beginTransaction")
					err := s.producer.BeginTransaction()
					if err != nil {
						if err.(kafka.Error).IsFatal() {
							return err
						}

						log.Printf("begin transaction: %v\n", err)
						time.Sleep(time.Second) // TODO: add exponential back off here
						goto beginTransaction
					}

					ctx2, cancel := context.WithTimeout(context.Background(), transactionTimeout)
					defer cancel()
					ctx = ctx2

					flush = time.After(flushTime)
					flushDeadline = time.Now().Add(100 * time.Millisecond)
				}

				s.subscriptions[*e.TopicPartition.Topic].ch <- e

			case kafka.AssignedPartitions:
				log.Printf("%% %v\n", e)
				err := abortTransaction(ctx, s.producer, s.consumer)
				if err != nil {
					return err
				}
				err = s.consumer.Assign(e.Partitions)
				if err != nil {
					return err
				}
				ctx = nil

				for _, partition := range e.Partitions {
					callback, ok := s.aggregations[*partition.Topic]
					if ok {
						callback(partition.Partition)
					}
				}

			case kafka.RevokedPartitions:
				log.Printf("%% %v\n", e)
				err := abortTransaction(ctx, s.producer, s.consumer)
				if err != nil {
					return err
				}
				err = s.consumer.Unassign()
				if err != nil {
					return err
				}
				ctx = nil

			case kafka.PartitionEOF:
				// log.Printf("%v\n", e)
				// TODO commit when all partitions reached end of file

			case kafka.Error:
				if e.IsFatal() {
					return e
				}

				log.Printf("Consumer error: %v\n", e)

			default:
				log.Printf("Ignored %v\n", e)
			}
		}
	}

	return nil
}

func (s *StreamingApplication) commit(ctx context.Context, c *kafka.Consumer, p *kafka.Producer) error {
	if ctx == nil {
		return nil
	}

	s.flush()

retryCommitOffsets:
	err := commitOffsets(ctx, c, p)
	if err != nil {
		if err.(kafka.Error).IsRetriable() {
			goto retryCommitOffsets
		}
		if err.(kafka.Error).IsFatal() {
			return err
		}
		if err.(kafka.Error).TxnRequiresAbort() {
			err2 := abortTransaction(ctx, p, c)
			if err2 != nil {
				return fmt.Errorf("commit offsets: %v: abort transaction: %v", err, err2)
			}
			return nil
		}
		return fmt.Errorf("commit offsets: %v", err)
	}

retryCommit:
	err = p.CommitTransaction(ctx)
	if err != nil {
		if err.(kafka.Error).IsRetriable() {
			goto retryCommit
		}
		if err.(kafka.Error).IsFatal() {
			return err
		}
		if err.(kafka.Error).TxnRequiresAbort() {
			err2 := abortTransaction(ctx, p, c)
			if err2 != nil {
				return fmt.Errorf("commit transaction: %v: abort transaction: %v", err, err2)
			}
			return nil
		}
		return fmt.Errorf("commit transaction: %v", err)
	}

	return nil
}

func (s *StreamingApplication) flush() {
	for _, subscription := range s.subscriptions {
		subscription.commitCh <- struct{}{}
	}

	commits := 0
	for range s.commits {
		commits++
		if commits == s.requiredCommits {
			return
		}
	}
}

func commitOffsets(ctx context.Context, c *kafka.Consumer, p *kafka.Producer) error {
	consumerMetadata, err := c.GetConsumerGroupMetadata()
	if err != nil {
		return err
	}

	partitions, err := c.Assignment()
	if err != nil {
		return err
	}

	offsets, err := c.Position(partitions)
	if err != nil {
		return err
	}

	err = p.SendOffsetsToTransaction(ctx, offsets, consumerMetadata)
	if err != nil {
		return err
	}

	return nil
}

func abortTransaction(ctx context.Context, p *kafka.Producer, c *kafka.Consumer) error {
	if ctx == nil {
		return nil
	}

abortTransaction:
	err := p.AbortTransaction(ctx)
	if err != nil {
		if err.(kafka.Error).IsRetriable() {
			goto abortTransaction
		}
		return err
	}

	err = rewindOffsets(ctx, c)
	if err != nil {
		return fmt.Errorf("rewind offsets: %v", err)
	}

	return nil
}

func rewindOffsets(ctx context.Context, c *kafka.Consumer) error {
	partitions, err := c.Assignment()
	if err != nil {
		return fmt.Errorf("look up assigned partitions: %v", err)
	}

	timeout, err := ctxTimeout(ctx)
	if err != nil {
		return err
	}

	positions, err := c.Committed(partitions, timeout)
	if err != nil {
		return fmt.Errorf("fetch commited offsets: %v", err)
	}

	for _, position := range positions {
		timeout, err := ctxTimeout(ctx)
		if err != nil {
			return err
		}

		err = c.Seek(position, timeout)
		if err != nil {
			return fmt.Errorf("rewind offset: %v", err)
		}
	}

	return nil
}

func printEvents(p *kafka.Producer, run *bool) (chan<- interface{}, <-chan interface{}) {
	termCh := make(chan interface{}, 1)
	doneCh := make(chan interface{})

	go func() {
		doTerm := false
		for !doTerm {
			select {
			case <-termCh:
				doTerm = true

			case e := <-p.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error == nil {
						continue
					}
					if ev.TopicPartition.Error.(kafka.Error).Code() == kafka.ErrPurgeQueue {
						continue
					}

					log.Printf("Message failed: %v\n", ev.TopicPartition.Error)

				case kafka.Error:
					if ev.IsFatal() {
						log.Fatalf("Fatal producer error: %v: terminating\n", e)
						f := false
						run = &f
						doTerm = true
						continue
					}

					log.Printf("Producer error: %v\n", e)

				default:
					log.Printf("Unhandled producer event: (%T) %s\n", e, ev)
				}
			}
		}

		close(doneCh)
	}()

	return termCh, doneCh
}

func ctxTimeout(ctx context.Context) (int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, fmt.Errorf("context has no deadline specified")
	}

	milliseconds := time.Until(deadline) / time.Millisecond

	if milliseconds <= 0 {
		return 0, fmt.Errorf("context has timed out")
	}

	return int(milliseconds), nil
}

// DecodeByteArray casts a byte array to the type alias ByteArray.
func DecodeByteArray(k []byte) (ByteArray, error) {
	return k, nil
}

// DecodeString casts a byte array to a string.
func DecodeString(k []byte) (string, error) {
	return string(k), nil
}

// DecodeInt converts a byte array to a string.
func DecodeInt(k []byte) (int, error) {
	i, len := binary.Varint(k)
	if len == 0 {
		return 0, fmt.Errorf("failed to decode int: byte array to short")
	}
	if len < 0 {
		return 0, fmt.Errorf("failed to decode int: overflow")
	}

	return int(i), nil
}

// EncodeByteArray casts the ByteArray type alias to a byte array.
func EncodeByteArray(k ByteArray) ([]byte, error) {
	return k, nil
}

// EncodeString casts a string to a byte array.
func EncodeString(s string) ([]byte, error) {
	return []byte(s), nil
}

// EncodeInt converts an int to a byte array.
func EncodeInt(i int) ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, int64(i))
	b := buf[:n]
	return b, nil
}
