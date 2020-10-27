package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	setupTimeout       = 60 * time.Second
	transactionTimeout = 10 * time.Second
	flushTime          = 100 * time.Millisecond
	pollTimeMs         = 10
)

func main() {
	err := stream()
	if err != nil {
		log.Fatalf("Failed to run transaction: %v", err)
	}
}

func stream() error {
	if len(os.Args) < 6 {
		return fmt.Errorf("usage: %s <broker> <group> <hostname> <target> <topics..>", os.Args[0])
	}

	broker := os.Args[1]
	group := os.Args[2]
	hostname := os.Args[3]
	target := os.Args[4]
	topics := os.Args[5:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	var err error

	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("look up hostname: %v\n", err)
		}
	}

	groupID := "copy-transactional-" + group
	instanceID := groupID + "-" + hostname

	consumerConfig := &kafka.ConfigMap{
		// General
		"bootstrap.servers": broker,
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
		"bootstrap.servers": broker,
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

	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		return fmt.Errorf("subscribe topics: %v", err)
	}

	defer func() {
		err := c.Close()
		if err != nil {
			fmt.Printf("close consumer: %v\n", err)
		}
	}()

	err = copy(sigchan, target, topics, c, p)
	if err != nil {
		return err
	}

	log.Println("done")

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

func copy(sigchan <-chan os.Signal, target string, topics []string, c *kafka.Consumer, p *kafka.Producer) error {

	run := true

	termCh, doneCh := printEvents(p, &run)

	var flush <-chan time.Time = nil
	var flushDeadline time.Time
	var ctx context.Context
	var msgCountTrx int

	msgCount := 0
	msgCountPrev := 0
	showLog := time.Tick(time.Second)

	for run == true {

		select {
		case sig := <-sigchan:
			//log.Printf("%%1 messages in transaction %v\n", msgCountTrx)

			fmt.Printf("Caught signal %v: terminating\n", sig)
			err := commit(ctx, c, p)
			if err != nil {
				return fmt.Errorf("commit transaction: %v", err)
			}
			run = false

		case <-showLog:
			log.Printf("messages/s %d\n", (msgCount - msgCountPrev))
			msgCountPrev = msgCount

		case <-flush:
			//log.Printf("%%3 messages in transaction %v\n", msgCountTrx)

			err := commit(ctx, c, p)
			if err != nil {
				return fmt.Errorf("flush time limit reached: commit transaction: %v", err)
			}
			ctx = nil

		default:
			//			log.Println("polling")

			pollTimeMs := 100
			if ctx != nil {
				pollTimeMs = int(flushDeadline.Sub(time.Now()) / time.Millisecond)
			}

			ev := c.Poll(pollTimeMs)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				//				log.Println("new message")
				//log.Printf("%% Message on %s: %s\n", e.TopicPartition, string(e.Value))
				//if e.Headers != nil {
				//	log.Printf("%% Headers: %v\n", e.Headers)
				//}

				if ctx == nil {
				beginTransaction:
					//log.Println("beginTransaction")
					err := p.BeginTransaction()
					if err != nil {
						if err.(kafka.Error).IsFatal() {
							return err
						}

						log.Printf("begin transaction: %v\n", err)
						time.Sleep(time.Second)
						goto beginTransaction
					}

					ctx2, cancel := context.WithTimeout(context.Background(), transactionTimeout)
					defer cancel()
					ctx = ctx2

					flush = time.After(flushTime)
					flushDeadline = time.Now().Add(100 * time.Millisecond)

					msgCountTrx = 0
				}

			produce:
				err := p.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{
						Topic:     &target,
						Partition: kafka.PartitionAny,
					},
					Value: e.Value,
					Key:   e.Key,
				}, nil)
				if err != nil {
					if err.(kafka.Error).IsFatal() {
						return err
					}

					log.Printf("produce message: %v", err)
					time.Sleep(10 * time.Millisecond)
					goto produce
				}
				msgCountTrx++
				msgCount++

			case kafka.AssignedPartitions:
				log.Printf("%% %v\n", e)
				err := abortTransaction(ctx, p, c)
				if err != nil {
					return err
				}
				err = c.Assign(e.Partitions)
				if err != nil {
					return err
				}
				ctx = nil

			case kafka.RevokedPartitions:
				log.Printf("%% %v\n", e)
				err := abortTransaction(ctx, p, c)
				if err != nil {
					return err
				}
				err = c.Unassign()
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

	termCh <- struct{}{}
	<-doneCh

	return nil
}

func commit(ctx context.Context, c *kafka.Consumer, p *kafka.Producer) error {
	if ctx == nil {
		return nil
	}

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

	timeout := deadline.Sub(time.Now())
	milliseconds := int(timeout) / int(time.Millisecond)

	if milliseconds <= 0 {
		return 0, fmt.Errorf("context has timed out")
	}

	return milliseconds, nil
}
