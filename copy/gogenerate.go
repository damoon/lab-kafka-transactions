//go:generate cp ../streaming_application.go streaming_application_gen.go
//go:generate sed -i "s/package streams/package copy/" streaming_application_gen.go
//go:generate genny -in=../stateless.go -out=stream_gen.go gen "KeyType=ByteArray ValueType=string"
//go:generate sed -i "s/package streams/package copy/" stream_gen.go

package copy
