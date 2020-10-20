package main

import (
	"log"
	"strconv"
)

func main() {

	in := make(chan Msg, 100)

	even := Filter(in, func(msg Msg) bool {
		i, err := strconv.Atoi(string(msg.key))
		if err != nil {
			log.Panicf("filter for even numbers: %v", err)
		}

		return i%2 == 0
	})

	out := Map(even, func(msg Msg) Msg {
		i, err := strconv.Atoi(string(msg.key))
		if err != nil {
			log.Panicf("square numbers: %v", err)
		}

		i = i * i

		return IntMsg(i, i)
	})

	for i := 0; i < 10; i++ {
		in <- Msg{
			key:   []byte(strconv.Itoa(i)),
			value: []byte(strconv.Itoa(i)),
		}
	}
	in <- Msg{
		done: true,
	}

	for n := range out {
		if n.done {
			log.Println("commit")
			close(in)
		} else {
			log.Println(string(n.key))
		}
	}
}

func IntMsg(i, j int) Msg {
	return Msg{
		key:   []byte(strconv.Itoa(i)),
		value: []byte(strconv.Itoa(j)),
	}
}

type Msg struct {
	key, value []byte
	done       bool
}

type Filterer = func(m Msg) bool

func Filter(in <-chan Msg, f Filterer) <-chan Msg {
	out := make(chan Msg)

	go func() {
		for msg := range in {
			if msg.done {
				out <- msg
				continue
			}

			if f(msg) {
				out <- msg
			}
		}
		close(out)
	}()

	return out
}

type Maper = func(m Msg) Msg

func Map(in <-chan Msg, m Maper) <-chan Msg {
	out := make(chan Msg)

	go func() {
		for msg := range in {
			if msg.done {
				out <- msg
				continue
			}

			out <- m(msg)
		}
		close(out)
	}()

	return out
}
