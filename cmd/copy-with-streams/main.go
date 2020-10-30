package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/damoon/lab-kafka-transactions/copy"
)

func main() {
	appName := "test-application-copy-strings"
	instanceID := fmt.Sprintf("%d", os.Getpid())
	brokers := "127.0.0.1:9092"

	err := run(appName, instanceID, brokers)
	if err != nil {
		log.Fatalf("streaming exactly once: %v", err)
	}
}

func run(appName, instanceID, brokers string) error {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	err := copy.NewStreamingApplication(appName, instanceID, brokers).
		StreamByteArrayStringTopic("topic1", copy.DecodeByteArray, copy.DecodeString).
		WriteTo("topic2", copy.EncodeByteArray, copy.EncodeString).
		Run(sigchan)
	if err != nil {
		return err
	}

	return nil
}
