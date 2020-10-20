package main

import (
	"log"
	"strconv"
	"unsafe"
)

const buffer = 10000

func main() {

	in := make(chan Msg, buffer)

	even := Filter(in, func(msg Msg) bool {
		return (msg.value).(int)%2 == 0
	})

	out := Map(even, func(msg Msg) Msg {
		i := msg.value.(int)
		i = i * i

		return Msg{key: msg.key, value: i}
	})

	for i := 0; i < 500; i++ {
		go func() {
			for i := 0; i < 10000; i++ {
				in <- Msg{
					key:   strconv.Itoa(i),
					value: i,
				}
			}
			in <- Msg{
				done: true,
			}
		}()

		for n := range out {
			if n.done {
				goto Commit
			} else {
				//				log.Println((n.key).(string))
			}
		}
	Commit:
		//		log.Println("commit")
	}
	close(in)

	log.Printf("msg size: %v", int(unsafe.Sizeof(Msg{})))
}

type Msg struct {
	key, value interface{}
	done       bool
}

type Filterer = func(m Msg) bool

func Filter(in <-chan Msg, f Filterer) <-chan Msg {
	out := make(chan Msg, buffer)

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
	out := make(chan Msg, buffer)

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
