package main

import (
	"log"
	"strconv"
)

func main() {

	randomMsgs := RandomMsgs(10)
	chain := randomMsgs.Filter(IsEven).Map(Square)

	for i := 0; i < 1000000; i++ {
		randomMsgs.Send(10)

		done := false
		for !done {
			select {
			case msg := <-chain.ch:
				_ = msg
				//				log.Printf("%s: %d\n", msg.key, msg.value)

			case <-chain.commitCh:
				for len(chain.ch) > 0 {
					msg := <-chain.ch
					_ = msg
					//					log.Printf("%s: %d\n", msg.key, msg.value)
				}
				done = true
			}
		}
	}

	randomMsgs.Close()

	for _ = range chain.commitCh {
	}
}

func IsEven(m Msg) bool {
	return m.value%2 == 0
}

func Square(m Msg) Msg {
	return Msg{
		m.key,
		m.value * m.value,
	}
}

type Stream struct {
	ch       chan Msg
	commitCh chan interface{}
}

type Msg struct {
	key   string
	value int
}

func RandomMsgs(cap int) MockStream {
	ch := make(chan Msg, cap)
	commitCh := make(chan interface{})
	return MockStream{
		Stream: Stream{
			commitCh: commitCh,
			ch:       ch,
		},
	}
}

func (r MockStream) Send(count int) {
	go func() {
		for i := 0; i < count; i++ {
			r.ch <- Msg{
				key:   strconv.Itoa(i),
				value: i,
			}
		}

		r.Stream.commitCh <- struct{}{}
	}()
}

func (r MockStream) Close() {
	log.Println("close")
	close(r.Stream.ch)
	close(r.Stream.commitCh)
}

type MockStream struct {
	Stream
}

type Mapper = func(m Msg) Msg

func (s Stream) Map(m Mapper) Stream {
	ch := make(chan Msg, cap(s.ch))
	commitCh := make(chan interface{})

	go func() {
		for {
			select {
			case msg, ok := <-s.ch:
				if !ok {
					s.ch = nil
				} else {
					ch <- m(msg)
				}

			case _, ok := <-s.commitCh:
				if !ok {
					s.commitCh = nil
				} else {
					for len(s.ch) > 0 {
						msg := <-s.ch
						ch <- m(msg)
					}
					commitCh <- struct{}{}
				}
			}

			if s.ch == nil && s.commitCh == nil {
				log.Println("close")
				close(ch)
				close(commitCh)
				return
			}
		}
	}()

	return Stream{
		ch:       ch,
		commitCh: commitCh,
	}
}

type Filterer = func(m Msg) bool

func (s Stream) Filter(f Filterer) Stream {
	ch := make(chan Msg, cap(s.ch))
	commitCh := make(chan interface{})

	go func() {
		for {
			select {
			case msg, ok := <-s.ch:
				if !ok {
					s.ch = nil
				} else {
					if f(msg) {
						ch <- msg
					}
				}

			case _, ok := <-s.commitCh:
				if !ok {
					s.commitCh = nil
				} else {
					for len(s.ch) > 0 {
						msg := <-s.ch
						if f(msg) {
							ch <- msg
						}
					}
					commitCh <- struct{}{}
				}
			}

			if s.ch == nil && s.commitCh == nil {
				log.Println("close")
				close(ch)
				close(commitCh)
				return
			}
		}
	}()

	return Stream{
		ch:       ch,
		commitCh: commitCh,
	}
}
