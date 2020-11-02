package streams

// ValueTypeIntStream is a placeholder for development.
type ValueTypeIntStream struct {
	app      *StreamingApplication
	ch       <-chan ValueTypeIntMsg
	commitCh <-chan interface{}
}

// ValueTypeIntMsg is a placeholder for development.
type ValueTypeIntMsg struct {
	Key   ValueType
	Value int
}
