package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	if len(os.Args) != 4 {
		log.Fatalf("%s <broker> <topic> <target-mps>\n", os.Args[0])
	}

	broker := os.Args[1]
	topic := os.Args[2]

	var err error

	targetMps, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("failed to parse delay: %v\n", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("look up hostname: %v\n", err)
	}

	instanceID := "producer-" + hostname

	cfg := &kafka.ConfigMap{
		// General
		"bootstrap.servers": broker,
		"client.id":         instanceID,

		// Producer
		"enable.idempotence": true,
		"compression.codec":  "zstd",
	}

	p, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	run := true
	termChan := make(chan bool, 1)
	doneChan := make(chan bool)

	go func() {
		doTerm := false
		for !doTerm {
			select {
			case e := <-p.Events():
				switch ev := e.(type) {
				case *kafka.Message:
					m := ev
					//					fmt.Printf("Delivered: %v\n", m.TopicPartition)
					if m.TopicPartition.Error != nil {
						fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					}

				case kafka.Error:
					e := ev
					if e.IsFatal() {
						fmt.Printf("FATAL ERROR: %v: terminating\n", e)
						run = false
						continue
					}
					fmt.Printf("Error: %v\n", e)

				default:
					fmt.Printf("ignored: %v\n", ev)
				}

			case <-termChan:
				doTerm = true
			}
		}

		close(doneChan)
	}()

	msgcnt := 0
	msgcntPrev := 0
	showLog := time.Tick(time.Second)
	delay := float64(int(time.Millisecond))

	for run {
		select {
		case <-showLog:
			count := msgcnt - msgcntPrev
			msgcntPrev = msgcnt
			if targetMps == -1 {
				log.Printf("messages/s: %d\n", count)
			} else {
				delay = float64(count) / float64(targetMps) * delay
				log.Printf("messages/s: %d / delay %s\n", count, time.Duration(delay))
			}
		default:
		}

		value := fmt.Sprintf("text #%d", msgcnt)
	produce:
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
				//Partition: 1,
			},
			Value: []byte(value),
			Key:   []byte(strconv.Itoa(msgcnt)),
		}, nil)

		if err != nil {
			log.Printf("produce message: %v", err)
			time.Sleep(10 * time.Millisecond)
			goto produce
		}

		msgcnt++

		//if msgcnt == 10_000_000 {
		//	run = false
		//}

		if targetMps != -1 {
			time.Sleep(time.Duration(delay))
		}

	}

	fmt.Println("flushing producer")
	for c := p.Flush(1000); c != 0; {
		fmt.Printf("%d messages left\n", c)
	}

	termChan <- true
	<-doneChan

	fatalErr := p.GetFatalError()

	p.Close()

	if fatalErr != nil {
		os.Exit(1)
	}
}
