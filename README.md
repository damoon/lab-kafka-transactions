
https://github.com/lazybeaver/xorshift

https://kafka.apache.org/documentation/streams/
https://github.com/lovoo/goka

https://strimzi.io/quickstarts/

https://dgraph.io/docs/badger/get-started/#read-write-transactions
https://github.com/cockroachdb/pebble
https://github.com/dgraph-io/badger
https://dgraph.io/docs/badger/get-started/#read-write-transactions

https://medium.com/a-journey-with-go/go-stringer-command-efficiency-through-code-generation-df49f97f3954

https://docs.confluent.io/current/installation/configuration/consumer-configs.html#cp-config-consumer
https://docs.confluent.io/current/installation/configuration/producer-configs.html#cp-config-producer
https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#hdr-Transactional_producer_API

https://gitlab.com/utopia-planitia/hetznerctl/-/commit/86f6ebd9a277017404e91e5da4444db4340809ca


SELECT * FROM https://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#window-final-results WHERE line LIKE '%Stream%'

https://www.zirous.com/2020/05/20/the-state-of-stateful-kafka-operations/

https://mydeveloperplanet.com/2019/10/30/kafka-streams-joins-explored/

// TODO check out gorename
// TODO check out mergol

## Stateless Transformations

 Branch                  KStream → KStream[]
 Filter                  KStream → KStream
 Inverse Filter          KStream → KStream
 FlatMap                 KStream → KStream
 FlatMap (values only)   KStream → KStream
 Foreach                 KStream → void
 GroupByKey              KStream → KGroupedStream
 GroupBy                 KStream → KGroupedStream
Cogroup                 KGroupedStream → CogroupedKStream
 Map                     KStream → KStream
 Map (values only)       KStream → KStream
 Merge                   KStream → KStream
 Peek                    KStream → KStream
 Print                   KStream → void
 SelectKey               KStream → KStream
 Repartition             KStream → KStream

## Stateful transformations

Aggregate                           KGroupedStream → KTable
Aggregate (windowed)                KGroupedStream → KTable
Count                               KGroupedStream → KTable
Count (windowed)                    KGroupedStream → KTable
Reduce                              KGroupedStream → KTable
Reduce (windowed)                   KGroupedStream → KTable

### Joining

Inner Join (windowed)               (KStream, KStream) → KStream
Left Join (windowed)                (KStream, KStream) → KStream
Outer Join (windowed)               (KStream, KStream) → KStream
Inner Join                          (KStream, KTable) → KStream
Left Join                           (KStream, KTable) → KStream

### Windowing

Hopping time windows
Tumbling time windows
Sliding time windows
Session Windows
Window Final Results

## Processors and Transformers

Process                 KStream -> void
Transform               KStream -> KStream
Transform (values only) KStream -> KStream

