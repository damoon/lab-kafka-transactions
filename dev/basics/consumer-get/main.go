// Example function-based high-level Apache Kafka consumer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// consumer_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events.

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	//	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family":           "v4",
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"auto.offset.reset":               "earliest",

		"client.id":          "consumer-example",
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	manageSubscriptions := func(client *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			client.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			client.Unassign()
		}

		return nil
	}

	err = c.SubscribeTopics(topics, manageSubscriptions)

	run := true

	i := 0

	log.Println("1")
	for run == true {
		log.Println("2")
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			msg, err := c.ReadMessage(100)
			if err != nil {
				e := err.(kafka.Error)
				if e.Code() == kafka.ErrTimedOut {
					log.Println("timeout reading from kafka")
					continue
				}

				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			}

			i++

			if i == 3555652 {
				log.Println("fetched 3555652 messages")
				run = false
			}

			fmt.Printf("%% Message on %s:\n%s\n", msg.TopicPartition, string(msg.Value))
			if msg.Headers != nil {
				fmt.Printf("%% Headers: %v\n", msg.Headers)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
