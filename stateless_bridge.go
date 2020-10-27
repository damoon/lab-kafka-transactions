package streams

import "github.com/cheekybits/genny/generic"

// InKey is the type for keys of input messages. This will be replaced by genny.
type InKey generic.Type

// InValue is the type for values of input messages. This will be replaced by genny.
type InValue generic.Type

// OutKey is the type for keys of output messages. This will be replaced by genny.
type OutKey generic.Type

// OutValue is the type for values of output messages. This will be replaced by genny.
type OutValue generic.Type

// FlatMapToOutKeyOutValue creates 0-N messages per message.
func (s InKeyInValueStream) FlatMapToOutKeyOutValue(f func(m InKeyInValueMsg, e func(OutKeyOutValueMsg))) OutKeyOutValueStream {
	task := func(ch chan OutKeyOutValueMsg, msg InKeyInValueMsg) {
		e := func(m OutKeyOutValueMsg) {
			ch <- m
		}

		f(msg, e)
	}

	return s.ProcessToOutKeyOutValue(task)
}

// MapToOutKeyOutValue uses m to compute a new message per message.
func (s InKeyInValueStream) MapToOutKeyOutValue(m func(m InKeyInValueMsg) OutKeyOutValueMsg) OutKeyOutValueStream {
	task := func(ch chan OutKeyOutValueMsg, msg InKeyInValueMsg) {
		ch <- m(msg)
	}

	return s.ProcessToOutKeyOutValue(task)
}

// ProcessToOutKeyOutValue executes the task and creates a new stream.
func (s InKeyInValueStream) ProcessToOutKeyOutValue(t func(ch chan OutKeyOutValueMsg, m InKeyInValueMsg)) OutKeyOutValueStream {
	ch := make(chan OutKeyOutValueMsg, cap(s.ch))
	commitCh := make(chan interface{}, 1)
	stream := OutKeyOutValueStream{
		ch:       ch,
		commitCh: commitCh,
	}

	go func() {
		for {
			select {
			case msg := <-s.ch:
				t(ch, msg)

			case _, ok := <-s.commitCh:
				for len(s.ch) > 0 {
					msg := <-s.ch
					t(ch, msg)
				}
				commitCh <- struct{}{}

				if !ok {
					close(ch)
					close(commitCh)
					return
				}
			}
		}
	}()

	return stream
}
