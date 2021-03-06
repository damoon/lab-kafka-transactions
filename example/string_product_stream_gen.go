// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package example

import (
	"log"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// StringProductStream computes on a stream of key (String) value (Product) messages.
type StringProductStream struct {
	app      *StreamingApplication
	ch       <-chan StringProductMsg
	commitCh <-chan interface{}
}

// StringProductMsg is a key value pair send through the computation graph.
type StringProductMsg struct {
	Key   string
	Value *Product
}

// Branch splits the stream into count new branches. Idx selects routes messages between the streams.
func (s StringProductStream) Branch(count int, idx func(m StringProductMsg) int) []StringProductStream {
	chs := make([]chan StringProductMsg, count)
	commitChs := make([]chan interface{}, count)
	streams := make([]StringProductStream, count)

	for i := 0; i < count; i++ {
		chs[i] = make(chan StringProductMsg, cap(s.ch))
		commitChs[i] = make(chan interface{}, 1)
		streams[i] = StringProductStream{
			ch:       chs[i],
			commitCh: commitChs[i],
		}
	}

	go func() {
		for {
			select {
			case msg := <-s.ch:
				chs[idx(msg)] <- msg

			case _, ok := <-s.commitCh:
				for len(s.ch) > 0 {
					msg := <-s.ch
					chs[idx(msg)] <- msg
				}

				for i := 0; i < count; i++ {
					commitChs[i] <- struct{}{}
				}

				if !ok {
					for i := 0; i < count; i++ {
						close(chs[i])
						close(commitChs[i])
					}
					return
				}
			}
		}
	}()

	return streams
}

// Filter keeps the messages with f(message) == True.
func (s StringProductStream) Filter(f func(m StringProductMsg) bool) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		if f(msg) {
			ch <- msg
		}
	}

	return s.Process(task)
}

// InverseFilter keeps the messages with f(message) == False.
func (s StringProductStream) InverseFilter(f func(m StringProductMsg) bool) StringProductStream {
	inverse := func(m StringProductMsg) bool {
		return !f(m)
	}
	return s.Filter(inverse)
}

// FlatMap creates 0-N messages per message.
func (s StringProductStream) FlatMap(f func(m StringProductMsg, e func(StringProductMsg))) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		e := func(m StringProductMsg) {
			ch <- m
		}

		f(msg, e)
	}

	return s.Process(task)
}

// FlatMapValues creates 0-N messages per message while keeping the key.
func (s StringProductStream) FlatMapValues(f func(v *Product, e func(v *Product))) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		e := func(v *Product) {
			m := StringProductMsg{
				Key:   msg.Key,
				Value: v,
			}
			ch <- m
		}

		f(msg.Value, e)
	}

	return s.Process(task)
}

// Foreach executes f per messages. The function is terminal and blocks until completion.
func (s StringProductStream) Foreach(f func(StringProductMsg)) {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		f(msg)
	}

	stream := s.Process(task)

	go func() {
		s.app.requiredCommits++
		for range stream.commitCh {
			s.app.commits <- struct{}{}
		}
	}()
}

// Map uses m to compute a new message per message.
func (s StringProductStream) Map(m func(m StringProductMsg) StringProductMsg) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		ch <- m(msg)
	}

	return s.Process(task)
}

// MapValues uses m to compute new values for each message.
func (s StringProductStream) MapValues(m func(m *Product) *Product) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		msg.Value = m(msg.Value)
		ch <- msg
	}

	return s.Process(task)
}

// Merge combines multiple streams into one.
func (s StringProductStream) Merge(ss ...StringProductStream) StringProductStream {
	ch := make(chan StringProductMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	streamsCount := len(ss) + 1
	internalCommitCh := make(chan interface{}, streamsCount)
	stream := StringProductStream{
		ch:       ch,
		commitCh: commitCh,
	}
	wg := sync.WaitGroup{}

	copy := func(s StringProductStream) {
		for {
			select {
			case msg := <-s.ch:
				ch <- msg

			case _, ok := <-s.commitCh:
				for len(s.ch) > 0 {
					msg := <-s.ch
					ch <- msg
				}
				internalCommitCh <- struct{}{}

				if !ok {
					wg.Done()
					return
				}
			}
		}
	}

	wg.Add(1)
	go copy(s)
	for _, s := range ss {
		wg.Add(1)
		go copy(s)
	}

	go func() {
		wg.Wait()
		close(ch)
		close(commitCh)
		close(internalCommitCh)
	}()

	go func() {
		i := 0
		for range internalCommitCh {
			i := (i + 1) % streamsCount
			if i == 0 {
				commitCh <- struct{}{}
			}
		}
	}()

	return stream
}

// Peek executes p per message.
func (s StringProductStream) Peek(p func(StringProductMsg)) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		p(msg)
		ch <- msg
	}

	return s.Process(task)
}

// Print logs each message to the stderr.
func (s StringProductStream) Print() {
	s.Foreach(PrintStringProductMsg)
}

// PrintStringProductMsg prints a message to stderr.
func PrintStringProductMsg(m StringProductMsg) {
	log.Printf("%v, %v\n", m.Key, m.Value)
}

// SelectKey creates a new key per message.
func (s StringProductStream) SelectKey(k func(m StringProductMsg) string) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		ch <- StringProductMsg{
			Key:   k(msg),
			Value: msg.Value,
		}
	}

	return s.Process(task)
}

// StreamStringProductTopic subscribes to a topic and streams its messages.
func (s *StreamingApplication) StreamStringProductTopic(topicName string, keyDecoder func(k []byte) (string, error), valueDecoder func(v []byte) (*Product, error)) StringProductStream {
	// TODO enforce only one read per topic, because only one offset is stored per stream.

	kafkaMsgCh := make(chan *kafka.Message, channelCap)
	commitCh := make(chan interface{}, 1)
	topic := topic{
		ch:       kafkaMsgCh,
		commitCh: commitCh,
	}
	s.subscriptions[topicName] = topic

	ch := make(chan StringProductMsg, channelCap)
	stream := StringProductStream{
		app:      s,
		ch:       ch,
		commitCh: commitCh,
	}

	convert := func(m *kafka.Message) StringProductMsg {
		key, err := keyDecoder(m.Key)
		if err != nil {
			log.Fatalf("decode key: key %v: %v", m.Key, err)
		}

		value, err := valueDecoder(m.Value)
		if err != nil {
			log.Fatalf("decode value: value %v: %v", m.Value, err)
		}

		return StringProductMsg{
			Key:   key,
			Value: value,
		}
	}

	go func() {
		for {
			select {
			case msg := <-kafkaMsgCh:
				ch <- convert(msg)

			case _, ok := <-commitCh:
				for len(kafkaMsgCh) > 0 {
					msg := <-kafkaMsgCh
					ch <- convert(msg)
				}
				commitCh <- struct{}{}

				if !ok {
					close(ch)
					close(commitCh)
					return
				}
			}
		}
	}()

	return stream
}

// WriteTo persists messages to a kafka topic.
func (s StringProductStream) WriteTo(topicName string, keyEncoder func(k string) ([]byte, error), valueEncoder func(v *Product) ([]byte, error)) *StreamingApplication {

	retries := 0
	task := func(m StringProductMsg) {
		key, err := keyEncoder(m.Key)
		if err != nil {
			log.Fatalf("encode key: key %v: %v", m.Key, err)
		}

		value, err := valueEncoder(m.Value)
		if err != nil {
			log.Fatalf("encode value: value %v: %v", m.Value, err)
		}

	produce:
		err = s.app.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topicName,
				Partition: kafka.PartitionAny,
			},
			Key:   key,
			Value: value,
		}, nil)
		if err != nil {
			ke := err.(kafka.Error)
			if ke.IsFatal() {
				log.Fatalf("fatal error: produce message: %v", err)
			}

			if ke.IsRetriable() {
				time.Sleep(10 * time.Millisecond)
				goto produce
			}

			if retries >= 20 {
				log.Fatalf("produce message: code %d: %v", ke.Code(), ke.Error())
			}

			time.Sleep(50 * time.Millisecond) // TODO: add exponential back off here
			retries++
			goto produce
		}

		retries = 0
	}

	go func() {
		s.app.requiredCommits++
		for {
			select {
			case msg := <-s.ch:
				task(msg)

			case <-s.commitCh:
				for len(s.ch) > 0 {
					msg := <-s.ch
					task(msg)
				}
				s.app.commits <- struct{}{}
			}
		}
	}()

	return s.app
}

// Repartition persists the stream into a topic and sorts the messages, based on their key, into partitions.
func (s StringProductStream) Repartition(
	topicName string,
	keyEncoder func(k string) ([]byte, error),
	valueEncoder func(v *Product) ([]byte, error),
	keyDecoder func(k []byte) (string, error),
	valueDecoder func(v []byte) (*Product, error),
) StringProductStream {
	return s.WriteTo(topicName, keyEncoder, valueEncoder).
		StreamStringProductTopic(topicName, keyDecoder, valueDecoder)
}

// Process executes the task and creates a new stream.
func (s StringProductStream) Process(task func(ch chan StringProductMsg, m StringProductMsg)) StringProductStream {
	ch := make(chan StringProductMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	stream := StringProductStream{
		app:      s.app,
		ch:       ch,
		commitCh: commitCh,
	}

	go func() {
		for {
			select {
			case msg := <-s.ch:
				task(ch, msg)

			case _, ok := <-s.commitCh:
				for len(s.ch) > 0 {
					msg := <-s.ch
					task(ch, msg)
				}
				commitCh <- struct{}{}

				if !ok {
					close(ch)
					close(commitCh)
					return
				}
			}
		}
	}()

	return stream
}
