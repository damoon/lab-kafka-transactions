package streams

import (
	"log"
	"sync"
)

type Brancher = func(m Msg) int

func (s Stream) Branch(count int, b Brancher) []Stream {
	chs := make([]chan Msg, count)
	commitChs := make([]chan interface{}, count)
	streams := make([]Stream, count)

	for i := 0; i < count; i++ {
		chs[i] = make(chan Msg, cap(s.ch))
		commitChs[i] = make(chan interface{}, 1)
		streams[i] = Stream{
			ch:       chs[i],
			commitCh: commitChs[i],
		}
	}

	go func() {
		for {
			select {
			case msg := <-s.ch:
				chs[b(msg)] <- msg

			case _, ok := <-s.commitCh:
				for len(s.ch) > 0 {
					msg := <-s.ch
					chs[b(msg)] <- msg
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

type Filterer = func(m Msg) bool

func (s Stream) Filter(f Filterer) Stream {
	task := func(ch chan Msg, msg Msg) {
		if f(msg) {
			ch <- msg
		}
	}

	return s.process(task)
}

func (s Stream) InverseFilter(f Filterer) Stream {
	inverse := func(m Msg) bool {
		return !f(m)
	}
	return s.Filter(inverse)
}

type Emitter = func(Msg)

type FlatMapper = func(m Msg, e Emitter)

func (s Stream) FlatMap(f FlatMapper) Stream {
	task := func(ch chan Msg, msg Msg) {
		e := func(m Msg) {
			ch <- msg
		}

		f(msg, e)
	}

	return s.process(task)
}

type ValueEmitter = func(v int)

type ValuesFlatMapper = func(v int, e ValueEmitter)

func (s Stream) FlatMapValues(f ValuesFlatMapper) Stream {
	task := func(ch chan Msg, msg Msg) {
		e := func(v int) {
			msg.Value = v
			ch <- msg
		}

		f(msg.Value, e)
	}

	return s.process(task)
}

type Foreacher = func(Msg)

func (s Stream) Foreach(f Foreacher) {
	task := func(ch chan Msg, msg Msg) {
		f(msg)
	}

	stream := s.process(task)

	// Foreach does not return a stream to be further processed.
	// This loop terminates the accumulated commit messages.
	go func() {
		for range stream.commitCh {
		}
	}()
}

type Mapper = func(m Msg) Msg

func (s Stream) Map(m Mapper) Stream {
	task := func(ch chan Msg, msg Msg) {
		ch <- m(msg)
	}

	return s.process(task)
}

type ValuesMapper = func(m int) int

func (s Stream) MapValues(m ValuesMapper) Stream {
	task := func(ch chan Msg, msg Msg) {
		msg.Value = m(msg.Value)
		ch <- msg
	}

	return s.process(task)
}

func (s Stream) Merge(ss ...Stream) Stream {

	ch := make(chan Msg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	streamsCount := len(ss) + 1
	internalCommitCh := make(chan interface{}, streamsCount)
	stream := Stream{
		ch:       ch,
		commitCh: commitCh,
	}
	wg := sync.WaitGroup{}

	copy := func(s Stream) {
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

type Peeker = func(Msg)

func (s Stream) Peek(p Peeker) Stream {
	task := func(ch chan Msg, msg Msg) {
		p(msg)
		ch <- msg
	}

	return s.process(task)
}

func (s Stream) Print() {
	s.Foreach(Print)
}

func Print(m Msg) {
	log.Printf("%v, %v\n", m.Key, m.Value)
}

type KeySelector = func(m Msg) string

func (s Stream) SelectKey(k KeySelector) Stream {
	task := func(ch chan Msg, msg Msg) {
		ch <- Msg{
			Key:   k(msg),
			Value: msg.Value,
		}
	}

	return s.process(task)
}

type task = func(ch chan Msg, m Msg)

func (s Stream) process(t task) Stream {
	ch := make(chan Msg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	stream := Stream{
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
