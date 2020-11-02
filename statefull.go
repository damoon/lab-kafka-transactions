package streams

import "github.com/cheekybits/genny/generic"

// AggType is the type for values of messages from Aggregate streams. This will be replaced by genny.
type AggType generic.Type

// Reduce combines all values of the same key using the reducer function.
func (s KeyTypeValueTypeGroupedStream) Reduce(
	topicName string,
	reducer func(aggValue, newValue ValueType) ValueType,
	keyDecoder func(k []byte) (KeyType, error),
	valueDecoder func(v []byte) (ValueType, error),
) KeyTypeValueTypeStream {

	// when grouped partition is assigned (*-rebalance)
	// - read all of (*-compact) until EOF and save to badger

	// for each incomming Msg:
	// - read from badger transaction
	// - increase
	// - write to -compart topic
	// - write to badger transaction

	// allow to abort transactions

	// allow to commit transaction

	table := s.app.StreamKeyTypeValueTypeTopic(topicName, keyDecoder, valueDecoder)
	return KeyTypeValueTypeTable{
		app:      s.app,
		ch:       table.ch,
		commitCh: table.commitCh,
	}
}

// Aggregate combines all values of the same key using the aggregator function.
func (s KeyTypeValueTypeGroupedStream) Aggregate(
	topicName string,
	initializer func() AggType,
	aggregator func(aggValue AggType, newValue ValueType) AggType,
) KeyTypeAggTypeStream {
}
