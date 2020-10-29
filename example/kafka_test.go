//go:generate cp ../streaming_application.go kafka_gen.go
//go:generate sed -i "s/package streams/package example/" kafka_gen.go
//go:generate genny -in=../stateless.go -out=kafka_stream_gen.go gen "KeyType=ByteArray ValueType=string"
//go:generate sed -i "s/package streams/package example/" kafka_stream_gen.go

package example

import (
	"os"
	"os/signal"
	"syscall"
	"testing"
	"unicode/utf8"
)

func TestCopy(t *testing.T) {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	err := NewStreamingApplication("test-application-reverse-strings", "unit-test", "127.0.0.1:9092").
		StreamByteArrayStringTopic("test-topic-in", DecodeByteArray, DecodeString).
		MapValues(reverse).
		WriteTo("test-topic-out", EncodeByteArray, EncodeString).
		Run(sigchan)
	if err != nil {
		t.Errorf("run app: %v", err)
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
