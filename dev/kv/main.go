package main

import (
	"log"

	"github.com/DanielMorsing/rocksdb"
)

func main() {
	err := transaction()
	if err != nil {
		log.Fatalf("Failed to run transaction: %v", err)
	}
}

func transaction() error {

	opts := rocksdb.NewOptions()
	opts.SetCache(rocksdb.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(true)
	db, err := rocksdb.Open("/path/to/db", opts)

	return nil
}
