package streams

// InKeyInValueStream gets replaces via genny.
type InKeyInValueStream struct {
	ch       <-chan InKeyInValueMsg
	commitCh <-chan interface{}
}

// InKeyInValueMsg gets replaces via genny.
type InKeyInValueMsg struct {
	Key   InKey
	Value InValue
}

// OutKeyOutValueStream gets replaces via genny.
type OutKeyOutValueStream struct {
	ch       <-chan OutKeyOutValueMsg
	commitCh <-chan interface{}
}

// OutKeyOutValueMsg gets replaces via genny.
type OutKeyOutValueMsg struct {
	Key   OutKey
	Value OutValue
}
