package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	streams "github.com/damoon/lab-kafka-transactions"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("%s <broker> <topic>\n", os.Args[0])
	}

	broker := os.Args[1]
	topic := os.Args[2]
	instanceID := fmt.Sprintf("verify-%d", os.Getpid())
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	var err error

	cfg := &kafka.ConfigMap{
		// General
		"bootstrap.servers": broker,
		"client.id":         instanceID,

		// Consumer
		"group.id":                        instanceID,
		"group.instance.id":               instanceID,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"auto.offset.reset":               "earliest",
		//		"enable.auto.commit":              false,
		"isolation.level": "read_committed",
		// "partition.assignment.strategy": "cooperative-sticky", // until https://github.com/edenhill/librdkafka/issues/1992
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		log.Fatalf("create consumer: %v\n", err)
	}

	err = c.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("subscribe to topic %s: %v\n", topic, err)
	}

	values := map[int32]int{}
	allValues := map[int]int8{}
	eof := 0
	count := 0

	run := true
	for run {

		select {
		case sig := <-sigchan:
			fmt.Printf("signal %v: terminating\n", sig)
			run = false

		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				i, err := streams.DecodeInt(e.Value)
				if err != nil {
					log.Fatalf("decode int: %v", err)
				}

				if values[e.TopicPartition.Partition] >= i {
					log.Fatalf("got %d before %d", values[e.TopicPartition.Partition], i)
				}
				values[e.TopicPartition.Partition] = i

				allValues[i]++
				count++

			case kafka.AssignedPartitions:
				log.Printf("%v\n", e)

				for _, parition := range e.Partitions {
					values[parition.Partition] = -1
				}

				err := c.Assign(e.Partitions)
				if err != nil {
					run = false
				}

				eof = len(e.Partitions)
			case kafka.RevokedPartitions:
				log.Printf("%v\n", e)
				err := c.Unassign()
				if err != nil {
					run = false
				}
			case kafka.PartitionEOF:
				log.Printf("%v\n", e)
				eof--

				if eof == 0 {
					log.Printf("all messages a ordered per partition\n")
					log.Printf("found %d values overall\n", count)

					log.Print("checking all numbers form 0 to 9_999_999 have been send exactly once")
					for i := 0; i < 10_000_000; i++ {
						if allValues[i] != 1 {
							log.Fatalf("value %d found %d times", i, allValues[i])
						}
					}
					log.Println("done")
					run = false
				}

			case kafka.Error:
				log.Printf("error: %v\n", e)
				if e.IsFatal() {
					err = e
					run = false
				}
			default:
				fmt.Printf("ignored: %v\n", e)
			}
		}
	}

	c.Close()

	if err != nil {
		os.Exit(1)
	}
}
