package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	err := transaction()
	if err != nil {
		log.Fatalf("Failed to run transaction: %v", err)
	}
}

const (
	setupTimeout       = 60 * time.Second
	transactionTimeout = 10 * time.Second
)

func transaction() error {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("Failed to look up hostname: %v", err)
	}

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":      "host1:9092,host2:9092",
		"client.id":              hostname,
		"enable.idempotence":     true,
		"transactional.id":       hostname,
		"transaction.timeout.ms": 10 * 1000,
		"enable.auto.commit":     false,
		"isolation.level":        "read_committed",
	}

	p, err := kafka.NewProducer(cfg)
	if err != nil {
		return fmt.Errorf("Failed to create producer: %v", err)
	}

	c, err := kafka.NewConsumer(cfg)
	if err != nil {
		return fmt.Errorf("Failed to create consumer: %v", err)
	}

	err = c.Subscribe(inputTopic, groupRebalance)
	if err != nil {
		fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	err = initTransaction(ctx, p)
	if err != nil {
		return err
	}

process:
	ev := c.Poll(100)
	if ev == nil {
		goto process
	}

	switch e := ev.(type) {
	case *kafka.Message:
		// Process ingress car event message
		processIngressCarMessage(e)
	case kafka.Error:
		// Errors are generally just informational.
		addLog(fmt.Sprintf("Consumer error: %sn", ev))
	default:
		addLog(fmt.Sprintf("Consumer event: %s: ignored", ev))
	}

	e := <-p.Events()

	err = p.BeginTransaction()
	if err != nil {
		return err
	}

	//p.Produce

retryCommit:
	ctx, cancel = context.WithTimeout(ctx, transactionTimeout)
	defer cancel()

	kafka.LibrdkafkaLinkInfo

	err = p.CommitTransaction(ctx)
	if err == nil {
		goto process
	}

	if err.(kafka.Error).IsRetriable() {
		goto retryCommit
	}

	if err.(kafka.Error).TxnRequiresAbort() {
	abortCommit:

		ctx, cancel := context.WithTimeout(ctx, transactionTimeout)
		defer cancel()

		err = p.AbortTransaction(ctx)

		if err.(kafka.Error).IsRetriable() {
			goto abortCommit
		}

		// rewind consumer
		// rewind state

		goto process
	}

	return err
}

func initTransaction(ctx context.Context, p *kafka.Producer) error {
retryInitTransaction:
	innerCtx, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()

	err := p.InitTransactions(innerCtx)
	if err == nil {
		return nil
	}

	if err.(kafka.Error).IsRetriable() {
		goto retryInitTransaction
	}

	return err
}
