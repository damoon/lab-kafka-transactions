package streams

import "strconv"

type Stream struct {
	ch       <-chan Msg
	commitCh <-chan interface{}
}

type Msg struct {
	Key   string
	Value int
}

func RandomMsgs(count, cap int) Stream {
	ch := make(chan Msg, cap)
	stream := Stream{
		ch: ch,
	}

	go func() {
		for i := 0; i < count; i++ {
			ch <- Msg{
				Key:   strconv.Itoa(i),
				Value: i,
			}
		}

		close(ch)
	}()

	return stream
}
