package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	if len(os.Args) < 5 {
		log.Fatalf("%s <broker> <group> <hostname> <topics..>\n", os.Args[0])
	}

	broker := os.Args[1]
	group := os.Args[2]
	hostname := os.Args[3]
	topics := os.Args[4:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	var err error

	if hostname == "" {
		hostname, err = os.Hostname()
		if err != nil {
			log.Fatalf("look up hostname: %v\n", err)
		}
	}

	groupID := "consumer-" + group
	instanceID := groupID + "-" + hostname

	cfg := &kafka.ConfigMap{
		// General
		"bootstrap.servers": broker,
		"client.id":         instanceID,

		// Consumer
		"group.id":                        groupID,
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

	err = c.SubscribeTopics(topics, nil)

	msgCount := 0
	msgCountPrev := 0
	showLog := time.Tick(time.Second)

	run := true
	for run {

		select {
		case sig := <-sigchan:
			fmt.Printf("signal %v: terminating\n", sig)
			run = false

		case <-showLog:
			log.Printf("messages/s %d\n", (msgCount - msgCountPrev))
			msgCountPrev = msgCount

		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				msgCount++
			case kafka.AssignedPartitions:
				log.Printf("%v\n", e)
				err := c.Assign(e.Partitions)
				if err != nil {
					run = false
				}
			case kafka.RevokedPartitions:
				log.Printf("%v\n", e)
				err := c.Unassign()
				if err != nil {
					run = false
				}
			case kafka.PartitionEOF:
				//				log.Printf("%v\n", e)
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
