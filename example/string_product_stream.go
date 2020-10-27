// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/cheekybits/genny

package example

import (
	"log"
	"sync"
)

// StringProductStream computes on a stream of key (String) value (Product) messages.
type StringProductStream struct {
	ch       <-chan StringProductMsg
	commitCh <-chan interface{}
}

// StringProductMsg is a key value pair send through the computation graph.
type StringProductMsg struct {
	Key   string
	Value Product
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
						return
					}
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
func (s StringProductStream) FlatMapValues(f func(v Product, e func(v Product))) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		e := func(v Product) {
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

	for range stream.commitCh {
	}
}

// Map uses m to compute a new message per message.
func (s StringProductStream) Map(m func(m StringProductMsg) StringProductMsg) StringProductStream {
	task := func(ch chan StringProductMsg, msg StringProductMsg) {
		ch <- m(msg)
	}

	return s.Process(task)
}

// MapValues uses m to compute new values for each message.
func (s StringProductStream) MapValues(m func(m Product) Product) StringProductStream {
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

// Process executes the task and creates a new stream.
func (s StringProductStream) Process(t func(ch chan StringProductMsg, m StringProductMsg)) StringProductStream {
	ch := make(chan StringProductMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	stream := StringProductStream{
		ch:       ch,
		commitCh: commitCh,
	}

	go func() {
		for {
			select {
			case msg := <-s.ch:
				t(ch, msg)

			case _, ok := <-s.commitCh:
				for len(s.ch) > 0 {
					msg := <-s.ch
					t(ch, msg)
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
