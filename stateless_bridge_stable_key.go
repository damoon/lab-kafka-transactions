package streams

// MapValuesToOutValue computes a new message per message where value'=m(value) while keeping the key.
func (s InKeyInValueStream) MapValuesToOutValue(m func(m InValue) OutValue) OutKeyOutValueStream {
	task := func(ch chan OutKeyOutValueMsg, msg InKeyInValueMsg) {
		m := OutKeyOutValueMsg{
			Key:   msg.Key,
			Value: m(msg.Value),
		}
		ch <- m
	}

	return s.ProcessToOutKeyOutValue(task)
}

// FlatMapToOutValues creates 0-N messages per message while keeping the key.
func (s InKeyInValueStream) FlatMapToOutValues(f func(v InValue, e func(v OutValue))) OutKeyOutValueStream {
	task := func(ch chan OutKeyOutValueMsg, msg InKeyInValueMsg) {
		e := func(v OutValue) {
			m := OutKeyOutValueMsg{
				Key:   msg.Key,
				Value: v,
			}
			ch <- m
		}

		f(msg.Value, e)
	}

	return s.ProcessToOutKeyOutValue(task)
}
