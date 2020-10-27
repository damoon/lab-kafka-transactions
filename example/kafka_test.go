//go:generate cp ../streaming_application.go kafka.go
//go:generate sed -i "s/package streams/package example/" kafka.go
//go:generate ../bin/genny -in=../stateless.go -out=kafka_stream.go gen "KeyType=KafkaKey ValueType=string"
//go:generate sed -i "s/package streams/package example/" kafka_stream.go

package example

import (
	"testing"
	"unicode/utf8"
)

func TestCopy(t *testing.T) {
	bytesToKey := func(in []byte) KafkaKey { return in }
	keyToBytes := func(in KafkaKey) []byte { return in }
	toString := func(in []byte) string { return string(in) }
	fromString := func(in string) []byte { return []byte(in) }

	err := NewStreamingApplication("test-application-reverse-strings", "unit-test", "127.0.0.1:9092").
		StreamKafkaKeyStringTopic("test-topic-in", bytesToKey, toString).
		MapValues(reverse).
		WriteTo("test-topic-out", keyToBytes, fromString).
		Run()
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
