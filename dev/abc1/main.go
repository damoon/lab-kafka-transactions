package main

import (
	"log"
	"os"
)

func main() {
	log.Println("hi")
	in := make(chan equal)
	out := unique(in)

	go func() {
		in <- myint(1)
		in <- myint(1)
		in <- myint(2)
		in <- myint(2)
		in <- myint(3)
		in <- myint(3)
		close(in)
	}()

	for i := range out {
		j := i + 1
		log.Printf("%+v\n", j)
	}
	os.Exit(0)
}

type myint int

func (i myint) equal(j equal) bool {
	return i == j
}

type equal interface {
	equal(a equal) bool
}

func unique(in <-chan equal) <-chan equal {
	out := make(chan equal)
	go func() {
		v := <-in
		out <- v
		for next := range in {
			if !v.equal(next) {
				out <- next
				v = next
			}
		}
		close(out)
	}()
	return out
}
