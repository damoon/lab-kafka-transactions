package streams

// SelectKeyToOutKey creates a new key per message.
func (s InKeyInValueStream) SelectKeyToOutKey(k func(m InKeyInValueMsg) OutKey) OutKeyOutValueStream {
	task := func(ch chan OutKeyOutValueMsg, msg InKeyInValueMsg) {
		ch <- OutKeyOutValueMsg{
			Key:   k(msg),
			Value: msg.Value,
		}
	}

	return s.ProcessToOutKeyOutValue(task)
}
