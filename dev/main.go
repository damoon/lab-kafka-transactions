package main

func main() {
	in := &KafkaSource{"test_in"}
	in := &KafkaSink{"test_out"}
	f := func(msg []byte) bool {
		return true
	}
	out := in.Filter(f)
}

type Emitter = func(key, value []byte)

type Processor = func(key []byte, value []byte, e Emitter)

func Stream(group, inTopic, outTopic string, p Processor) {

}

type Mapper = func(key []byte, value []byte) ([]byte, []byte)

func Map(group, inTopic, outTopic string, m Mapper) {
	p := func(key []byte, value []byte, e Emitter) {
		k, v := m(key, value)
		e(k, v)
	}
	Stream(group, inTopic, outTopic, p)
}

type FlatMapper = Processor

func FlatMap(group, inTopic, outTopic string, fm FlatMapper) {
	Stream(group, inTopic, outTopic, fm)
}

type Filterer = func(key []byte, value []byte) bool

func Filter(group, inTopic, outTopic string, f Filterer) {
	p := func(key []byte, value []byte, e Emitter) {
		if f(key, value) {
			e(key, value)
		}
	}
	Stream(group, inTopic, outTopic, p)
}
