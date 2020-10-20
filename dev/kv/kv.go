package kv

type KV struct {
}

func NewKV(name string) (KV, error) {

}

func (kv KV) Set(key, value []byte) error {

}

func (kv KV) Get(key) error {

}

func (kv KV) Delete(key) error {

}

func (kv KV) BeginTransaction() error {

}

func (kv KV) CommitTransaction() error {

}

func (kv KV) AbortTransaction() error {

}
