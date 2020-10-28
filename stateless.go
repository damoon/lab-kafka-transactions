package streams

import (
	"log"
	"sync"
	"time"

	"github.com/cheekybits/genny/generic"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KeyType is the type for keys of messages. This will be replaced by genny.
type KeyType generic.Type

// ValueType is the type for values of messages. This will be replaced by genny.
type ValueType generic.Type

// KeyTypeValueTypeStream computes on a stream of key (KeyType) value (ValueType) messages.
type KeyTypeValueTypeStream struct {
	app      *StreamingApplication
	ch       <-chan KeyTypeValueTypeMsg
	commitCh <-chan interface{}
}

// KeyTypeValueTypeMsg is a key value pair send through the computation graph.
type KeyTypeValueTypeMsg struct {
	Key   KeyType
	Value ValueType
}

// Branch splits the stream into count new branches. Idx selects routes messages between the streams.
func (s KeyTypeValueTypeStream) Branch(count int, idx func(m KeyTypeValueTypeMsg) int) []KeyTypeValueTypeStream {
	chs := make([]chan KeyTypeValueTypeMsg, count)
	commitChs := make([]chan interface{}, count)
	streams := make([]KeyTypeValueTypeStream, count)

	for i := 0; i < count; i++ {
		chs[i] = make(chan KeyTypeValueTypeMsg, cap(s.ch))
		commitChs[i] = make(chan interface{}, 1)
		streams[i] = KeyTypeValueTypeStream{
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
						return
					}
				}
			}
		}
	}()

	return streams
}

// Filter keeps the messages with f(message) == True.
func (s KeyTypeValueTypeStream) Filter(f func(m KeyTypeValueTypeMsg) bool) KeyTypeValueTypeStream {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		if f(msg) {
			ch <- msg
		}
	}

	return s.Process(task)
}

// InverseFilter keeps the messages with f(message) == False.
func (s KeyTypeValueTypeStream) InverseFilter(f func(m KeyTypeValueTypeMsg) bool) KeyTypeValueTypeStream {
	inverse := func(m KeyTypeValueTypeMsg) bool {
		return !f(m)
	}
	return s.Filter(inverse)
}

// FlatMap creates 0-N messages per message.
func (s KeyTypeValueTypeStream) FlatMap(f func(m KeyTypeValueTypeMsg, e func(KeyTypeValueTypeMsg))) KeyTypeValueTypeStream {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		e := func(m KeyTypeValueTypeMsg) {
			ch <- m
		}

		f(msg, e)
	}

	return s.Process(task)
}

// FlatMapValues creates 0-N messages per message while keeping the key.
func (s KeyTypeValueTypeStream) FlatMapValues(f func(v ValueType, e func(v ValueType))) KeyTypeValueTypeStream {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		e := func(v ValueType) {
			m := KeyTypeValueTypeMsg{
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
func (s KeyTypeValueTypeStream) Foreach(f func(KeyTypeValueTypeMsg)) {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		f(msg)
	}

	stream := s.Process(task)

	for range stream.commitCh {
	}
}

// Map uses m to compute a new message per message.
func (s KeyTypeValueTypeStream) Map(m func(m KeyTypeValueTypeMsg) KeyTypeValueTypeMsg) KeyTypeValueTypeStream {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		ch <- m(msg)
	}

	return s.Process(task)
}

// MapValues uses m to compute new values for each message.
func (s KeyTypeValueTypeStream) MapValues(m func(m ValueType) ValueType) KeyTypeValueTypeStream {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		msg.Value = m(msg.Value)
		ch <- msg
	}

	return s.Process(task)
}

// Merge combines multiple streams into one.
func (s KeyTypeValueTypeStream) Merge(ss ...KeyTypeValueTypeStream) KeyTypeValueTypeStream {
	ch := make(chan KeyTypeValueTypeMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	streamsCount := len(ss) + 1
	internalCommitCh := make(chan interface{}, streamsCount)
	stream := KeyTypeValueTypeStream{
		ch:       ch,
		commitCh: commitCh,
	}
	wg := sync.WaitGroup{}

	copy := func(s KeyTypeValueTypeStream) {
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
func (s KeyTypeValueTypeStream) Peek(p func(KeyTypeValueTypeMsg)) KeyTypeValueTypeStream {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		p(msg)
		ch <- msg
	}

	return s.Process(task)
}

// Print logs each message to the stderr.
func (s KeyTypeValueTypeStream) Print() {
	s.Foreach(PrintKeyTypeValueTypeMsg)
}

// PrintKeyTypeValueTypeMsg prints a message to stderr.
func PrintKeyTypeValueTypeMsg(m KeyTypeValueTypeMsg) {
	log.Printf("%v, %v\n", m.Key, m.Value)
}

// SelectKey creates a new key per message.
func (s KeyTypeValueTypeStream) SelectKey(k func(m KeyTypeValueTypeMsg) KeyType) KeyTypeValueTypeStream {
	task := func(ch chan KeyTypeValueTypeMsg, msg KeyTypeValueTypeMsg) {
		ch <- KeyTypeValueTypeMsg{
			Key:   k(msg),
			Value: msg.Value,
		}
	}

	return s.Process(task)
}

// StreamKeyTypeValueTypeTopic subscribes to a topic and streams its messages.
func (s *StreamingApplication) StreamKeyTypeValueTypeTopic(topicName string) KeyTypeValueTypeStream {
	kafkaMsgCh := make(chan kafka.Message, channelCap)
	commitCh := make(chan interface{}, 1)
	topic := topic{
		ch:       kafkaMsgCh,
		commitCh: commitCh,
	}
	s.subscriptions[topicName] = topic

	ch := make(chan KeyTypeValueTypeMsg, channelCap)
	stream := KeyTypeValueTypeStream{
		app:      s,
		ch:       ch,
		commitCh: commitCh,
	}

	convert := func(m kafka.Message) KeyTypeValueTypeMsg {
		return KeyTypeValueTypeMsg{
			Key:   DecodeKeyType(m.Key),
			Value: DecodeValueType(m.Value),
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
func (s KeyTypeValueTypeStream) WriteTo(topicName string) *StreamingApplication {

	task := func(m KeyTypeValueTypeMsg) {
	produce:
		err := s.app.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topicName,
				Partition: kafka.PartitionAny,
			},
			Key:   EncodeKeyType(m.Key),
			Value: EncodeValueType(m.Value),
		}, nil)
		if err != nil {
			if err.(kafka.Error).IsFatal() {
				log.Fatalf("fatal error: produce message: %v", err)
			}

			log.Printf("produce message: %v", err)
			time.Sleep(10 * time.Millisecond)
			goto produce
		}
	}

	go func() {
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
func (s KeyTypeValueTypeStream) Process(task func(ch chan KeyTypeValueTypeMsg, m KeyTypeValueTypeMsg)) KeyTypeValueTypeStream {
	ch := make(chan KeyTypeValueTypeMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	stream := KeyTypeValueTypeStream{
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
