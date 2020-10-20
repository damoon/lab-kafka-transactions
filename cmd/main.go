package main

import (
	streams "lab/kafka-transactions/d"
	"log"
)

func main() {

	c := 0
	count := func(m streams.Msg) {
		c++
	}

	for i := 0; i < 100; i++ {
		streams.RandomMsgs(10000, 1000).Filter(isEven).MapValues(square).Foreach(count)
	}

	log.Println(c)
}

func isEven(m streams.Msg) bool {
	return m.Value%2 == 0
}

func square(n int) int {
	return n * n
}
