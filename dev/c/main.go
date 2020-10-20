package main

import (
	"strconv"
)

func main() {

	for i := 0; i < 1000000; i++ {

		in := make(chan Msg, 10)

		even := Filter(in, func(msg Msg) bool {
			return (msg.value).(int)%2 == 0
		})

		out := Map(even, func(msg Msg) Msg {
			i := msg.value.(int)
			i = i * i

			return Msg{key: msg.key, value: i}
		})

		go func() {
			for i := 0; i < 10; i++ {
				in <- Msg{
					key:   strconv.Itoa(i),
					value: i,
				}
			}
			close(in)
		}()

		for n := range out {
			//				log.Println((n.key).(string))
			_ = n
		}
		//log.Println("commit")
	}
}

type Msg struct {
	key, value interface{}
}

type Filterer = func(m Msg) bool

func Filter(in <-chan Msg, f Filterer) <-chan Msg {
	out := make(chan Msg, cap(in))

	go func() {
		for msg := range in {
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
	out := make(chan Msg, cap(in))

	go func() {
		for msg := range in {
			out <- m(msg)
		}
		close(out)
	}()

	return out
}
