//go:generate genny -in=../stateless.go -out=string_product_stream_gen.go gen "KeyType=string ValueType=*Product"
//go:generate sed -i "s/package streams/package example/" string_product_stream_gen.go
//go:generate genny -in=../stateless.go -out=int_int_stream_gen.go gen "KeyType=int ValueType=int"
//go:generate sed -i "s/package streams/package example/" int_int_stream_gen.go
//go:generate genny -in=../stateless_bridge.go -out=int_int_to_string_product_gen.go gen "InKey=int InValue=int InStream=IntIntStream OutKey=string OutValue=Product OutMsg=ProductMsg"
//go:generate sed -i "s/package streams/package example/" int_int_to_string_product_gen.go
//go:generate protoc --go_out=. --go_opt=paths=source_relative product.proto

package example

import (
	"testing"
)

func NaturalNumbers(max, cap int) IntIntStream {
	ch := make(chan IntIntMsg, cap)
	commitCh := make(chan interface{}, 1)
	stream := IntIntStream{
		ch:       ch,
		commitCh: commitCh,
	}

	go func() {
		for i := 0; i < max; i++ {
			ch <- IntIntMsg{
				Key:   i,
				Value: i,
			}
		}

		close(ch)
		commitCh <- struct{}{}
		close(commitCh)
	}()

	return stream
}

func TestIntIntStream_Filter(t *testing.T) {
	isEven := func(m IntIntMsg) bool {
		return m.Value%2 == 0
	}

	square := func(n int) int {
		return n * n
	}

	got := 0
	sum := func(m IntIntMsg) {
		got += m.Value
	}

	NaturalNumbers(1_000_000, 10).Filter(isEven).MapValues(square).Foreach(sum)
	//	time.Sleep(time.Second)

	want := 166666166667000000
	if got != want {
		t.Errorf("Sum() = %v, want %v", got, want)
	}
}
