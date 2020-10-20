// Example channel-based high-level Apache Kafka consumer
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

	log.Printf("start\n")

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
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              group,
		//		"session.timeout.ms":              6000,
		// "transaction.timeout.ms": 10000,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		// Enable generation of PartitionEOF when the
		// end of a partition is reached.
		"enable.partition.eof": true,
		"auto.offset.reset":    "earliest",

		//		"go.events.channel.size": 100000,

		"client.id":          "consumer-example",
		"enable.auto.commit": false,
		"isolation.level":    "read_committed",
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	run := true

	i := 0

	//	p := 4

	log.Printf("begin\n")
	//	log.Println("1")
	for run == true {
		//		log.Println("2")
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false

		case ev := <-c.Events():
			log.Printf("event\n")
			switch e := ev.(type) {
			case *kafka.Message:
				log.Printf("%% Message on %s: %s\n", e.TopicPartition, string(e.Value))
				i++
			case kafka.AssignedPartitions:
				log.Printf("%% %v\n", e)
				c.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				log.Printf("%% %v\n", e)
				c.Unassign()
			case kafka.PartitionEOF:
			//	log.Printf("%% Reached %v\n", e)
			//				run = false
			//p--
			case kafka.Error:
				// Errors should generally be considered as informational, the client will try to automatically recover
				log.Printf("%% Error: %v\n", e)
			default:
				log.Printf("%% Unhandled: %v\n", e)
			}

			//if p == 0 {
			//	log.Printf("Closing consumer, count %d\n", i)
			//	os.Exit(0)
			//}
		}
		//		log.Println("3")
	}

	fmt.Printf("Closing consumer, count %d\n", i)
	c.Close()
}
