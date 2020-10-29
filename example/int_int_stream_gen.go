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

// IntIntStream computes on a stream of key (Int) value (Int) messages.
type IntIntStream struct {
	app      *StreamingApplication
	ch       <-chan IntIntMsg
	commitCh <-chan interface{}
}

// IntIntMsg is a key value pair send through the computation graph.
type IntIntMsg struct {
	Key   int
	Value int
}

// Branch splits the stream into count new branches. Idx selects routes messages between the streams.
func (s IntIntStream) Branch(count int, idx func(m IntIntMsg) int) []IntIntStream {
	chs := make([]chan IntIntMsg, count)
	commitChs := make([]chan interface{}, count)
	streams := make([]IntIntStream, count)

	for i := 0; i < count; i++ {
		chs[i] = make(chan IntIntMsg, cap(s.ch))
		commitChs[i] = make(chan interface{}, 1)
		streams[i] = IntIntStream{
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
func (s IntIntStream) Filter(f func(m IntIntMsg) bool) IntIntStream {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		if f(msg) {
			ch <- msg
		}
	}

	return s.Process(task)
}

// InverseFilter keeps the messages with f(message) == False.
func (s IntIntStream) InverseFilter(f func(m IntIntMsg) bool) IntIntStream {
	inverse := func(m IntIntMsg) bool {
		return !f(m)
	}
	return s.Filter(inverse)
}

// FlatMap creates 0-N messages per message.
func (s IntIntStream) FlatMap(f func(m IntIntMsg, e func(IntIntMsg))) IntIntStream {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		e := func(m IntIntMsg) {
			ch <- m
		}

		f(msg, e)
	}

	return s.Process(task)
}

// FlatMapValues creates 0-N messages per message while keeping the key.
func (s IntIntStream) FlatMapValues(f func(v int, e func(v int))) IntIntStream {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		e := func(v int) {
			m := IntIntMsg{
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
func (s IntIntStream) Foreach(f func(IntIntMsg)) {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
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
func (s IntIntStream) Map(m func(m IntIntMsg) IntIntMsg) IntIntStream {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		ch <- m(msg)
	}

	return s.Process(task)
}

// MapValues uses m to compute new values for each message.
func (s IntIntStream) MapValues(m func(m int) int) IntIntStream {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		msg.Value = m(msg.Value)
		ch <- msg
	}

	return s.Process(task)
}

// Merge combines multiple streams into one.
func (s IntIntStream) Merge(ss ...IntIntStream) IntIntStream {
	ch := make(chan IntIntMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	streamsCount := len(ss) + 1
	internalCommitCh := make(chan interface{}, streamsCount)
	stream := IntIntStream{
		ch:       ch,
		commitCh: commitCh,
	}
	wg := sync.WaitGroup{}

	copy := func(s IntIntStream) {
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
func (s IntIntStream) Peek(p func(IntIntMsg)) IntIntStream {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		p(msg)
		ch <- msg
	}

	return s.Process(task)
}

// Print logs each message to the stderr.
func (s IntIntStream) Print() {
	s.Foreach(PrintIntIntMsg)
}

// PrintIntIntMsg prints a message to stderr.
func PrintIntIntMsg(m IntIntMsg) {
	log.Printf("%v, %v\n", m.Key, m.Value)
}

// SelectKey creates a new key per message.
func (s IntIntStream) SelectKey(k func(m IntIntMsg) int) IntIntStream {
	task := func(ch chan IntIntMsg, msg IntIntMsg) {
		ch <- IntIntMsg{
			Key:   k(msg),
			Value: msg.Value,
		}
	}

	return s.Process(task)
}

// StreamIntIntTopic subscribes to a topic and streams its messages.
func (s *StreamingApplication) StreamIntIntTopic(topicName string, keyDecoder func(k []byte) (int, error), valueDecoder func(v []byte) (int, error)) IntIntStream {
	// TODO enforce only one read per topic, because only one offset is stored per stream.

	kafkaMsgCh := make(chan *kafka.Message, channelCap)
	commitCh := make(chan interface{}, 1)
	topic := topic{
		ch:       kafkaMsgCh,
		commitCh: commitCh,
	}
	s.subscriptions[topicName] = topic

	ch := make(chan IntIntMsg, channelCap)
	stream := IntIntStream{
		app:      s,
		ch:       ch,
		commitCh: commitCh,
	}

	convert := func(m *kafka.Message) IntIntMsg {
		key, err := keyDecoder(m.Key)
		if err != nil {
			log.Fatalf("decode key: key %v: %v", m.Key, err)
		}

		value, err := valueDecoder(m.Value)
		if err != nil {
			log.Fatalf("decode value: value %v: %v", m.Value, err)
		}

		return IntIntMsg{
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
func (s IntIntStream) WriteTo(topicName string, keyEncoder func(k int) ([]byte, error), valueEncoder func(v int) ([]byte, error)) *StreamingApplication {

	task := func(m IntIntMsg) {
	produce:
		key, err := keyEncoder(m.Key)
		if err != nil {
			log.Fatalf("encode key: key %v: %v", m.Key, err)
		}

		value, err := valueEncoder(m.Value)
		if err != nil {
			log.Fatalf("encode value: value %v: %v", m.Value, err)
		}

		// TODO use produce channel instead
		err = s.app.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topicName,
				Partition: kafka.PartitionAny,
			},
			Key:   key,
			Value: value,
		}, nil)
		if err != nil {
			if err.(kafka.Error).IsFatal() {
				log.Fatalf("fatal error: produce message: %v", err)
			}

			log.Printf("produce message: %v", err)
			time.Sleep(10 * time.Millisecond) // TODO: add exponential back off here
			goto produce
		}
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

// Process executes the task and creates a new stream.
func (s IntIntStream) Process(task func(ch chan IntIntMsg, m IntIntMsg)) IntIntStream {
	ch := make(chan IntIntMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	stream := IntIntStream{
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
