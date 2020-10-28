//go:generate ../bin/genny -in=../stateless.go -out=string_product_stream.go gen "KeyType=string ValueType=*Product"
//go:generate sed -i "s/package streams/package example/" string_product_stream.go
//go:generate ../bin/genny -in=../stateless.go -out=int_int_stream.go gen "KeyType=int ValueType=int"
//go:generate sed -i "s/package streams/package example/" int_int_stream.go
//go:generate ../bin/genny -in=../stateless_bridge.go -out=int_int_to_string_product.go gen "InKey=int InValue=int InStream=IntIntStream OutKey=string OutValue=Product OutMsg=ProductMsg"
//go:generate sed -i "s/package streams/package example/" int_int_to_string_product.go
//go:generate protoc --go_out=. --go_opt=paths=source_relative product.proto
package example
