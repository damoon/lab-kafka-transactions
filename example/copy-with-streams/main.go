package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"unicode/utf8"

	e "github.com/damoon/lab-kafka-transactions/example"
)

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	err := e.NewStreamingApplication("test-application-reverse-strings", fmt.Sprintf("copy-%d", os.Getpid()), "127.0.0.1:9092").
		StreamByteArrayStringTopic("topic1", e.DecodeByteArray, e.DecodeString).
		MapValues(reverse).
		WriteTo("topic2", e.EncodeByteArray, e.EncodeString).
		Run(sigchan)
	if err != nil {
		log.Fatalf("streaming exactly once: %v", err)
	}
}

func reverse(s string) string {
	o := make([]rune, utf8.RuneCountInString(s))
	i := len(o)
	for _, c := range s {
		i--
		o[i] = c
	}
	return string(o)
}
