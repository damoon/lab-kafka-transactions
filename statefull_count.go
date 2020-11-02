package streams

import (
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger/v2"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Count counts the values of a stream.
func (s KeyTypeValueTypeGroupedStream) Count(
	topicName string,
	keyEncoder func(v KeyType) ([]byte, error),
	valueEncoder func(v ValueType) ([]byte, error),
) ValueTypeIntStream {
	path := fmt.Sprintf("/tmp/badger-%s-%s", s.app.applicationName, topicName)
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		log.Fatalf("open badger database: %v", err)
	}

	// TODO
	defer db.Close()
	defer txn.Discard()

	rebalanceTopic := s.topic
	s.app.aggregations[s.topic] = func(partition int32) {
		tp := kafka.TopicPartition{
			Partition: partition,
			Topic:     &topicName,
		}
		loadPartitions(s.app, db, tp)
	}

	txn := db.NewTransaction(true)

	getInt := func(v []byte) (int, error) {
		item, err := txn.Get(v)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return 0, nil
			}
			return 0, fmt.Errorf("look up: %v", err)
		}

		v, err = item.ValueCopy(nil)
		if err != nil {
			return 0, fmt.Errorf("copy value: %v", err)
		}

		i, err := DecodeInt(v)
		if err != nil {
			return 0, fmt.Errorf("decode value: %v", err)
		}

		return i, nil
	}

	task := func(ch chan KeyTypeValueTypeMsg, m KeyTypeValueTypeMsg) error {
		valueBytes, err := valueEncoder(m.Value)
		if err != nil {
			log.Fatalf("encode value: %v", err)
		}

		i, err := getInt(valueBytes)
		if err != nil {
			log.Fatalf("get current value: %v", err)
		}

		i++

		newValue, err := EncodeInt(i)
		if err != nil {
			log.Fatalf("encode new value: %v", err)
		}

		err = txn.Set(valueBytes, newValue)
		if err != nil {
			log.Fatalf("set new value: %v", err)
		}

		ch <- ValueTypeIntMsg{
			Key:   valueBytes,
			Value: newValue,
		}

		return nil
	}

	s.
		Process(task, txn.Commit).
		WriteTo(topicName, keyEncoder, EncodeInt)

	//	s.app.aggregations[]

	// when grouped partition is assigned (*-rebalance)
	// - read all of (*-compact) until EOF and save to badger

	// for each incomming Msg:
	// - read from badger transaction
	// - increase
	// - write to -compart topic
	// - write to badger transaction

	// allow to abort transactions

	// allow to commit transaction

	stream := s.app.StreamKeyTypeValueTypeTopic(topicName, keyDecoder, valueDecoder)
	return KeyTypeIntStream{
		app:      stream.app,
		ch:       stream.ch,
		commitCh: stream.commitCh,
	}
}

// loadPartitions loads all messages from a partion into the database.
func loadPartitions(app *StreamingApplication, db *badger.DB, partition kafka.TopicPartition) error {
	instanceID := app.applicationName + "-" + app.instance + "-partition-loader"

	consumerConfig := &kafka.ConfigMap{
		// General
		"bootstrap.servers": app.brokers,
		"client.id":         instanceID,

		// Consumer
		"enable.partition.eof": true,
		"auto.offset.reset":    "earliest",
		"enable.auto.commit":   false,
		"isolation.level":      "read_committed",
	}

	c, err := kafka.NewConsumer(consumerConfig)
	if err != nil {
		return fmt.Errorf("create consumer: %s", err)
	}
	defer c.Close()

	err = c.Assign([]kafka.TopicPartition{partition})
	if err != nil {
		return fmt.Errorf("assign partitions: %s", err)
	}

	wb := db.NewWriteBatch()
	defer wb.Flush()

	for {
		pollTimeMs := 100
		ev := c.Poll(pollTimeMs)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			wb.Set(e.Key, e.Value)

		case kafka.PartitionEOF:
			return nil

		case kafka.Error:
			if e.IsFatal() {
				return e
			}

			log.Printf("load partitions: consumer error: %v\n", e)

		default:
			log.Printf("load partitions: ignored %v\n", e)
		}
	}

	return nil
}
