up:
	docker-compose up -d
	docker-compose logs -f

down:
	docker-compose down

produce:
	go run producer/main.go 127.0.0.1:9092 topic1 1000

produce-over-9000:
	go run producer/main.go 127.0.0.1:9092 topic1 -1

copy-1:
	go run copy/main.go 127.0.0.1:9092 group1 host1 topic2 topic1

copy-2:
	go run copy/main.go 127.0.0.1:9092 group1 host2 topic2 topic1

copy-3:
	go run copy/main.go 127.0.0.1:9092 group1 host3 topic2 topic1

copy-4:
	go run copy/main.go 127.0.0.1:9092 group1 host4 topic2 topic1

consume-1:
	go run consumer/main.go 127.0.0.1:9092 group1 host1 topic2

consume-2:
	go run consumer/main.go 127.0.0.1:9092 group1 host2 topic2

consume-3:
	go run consumer/main.go 127.0.0.1:9092 group1 host3 topic2

consume-4:
	go run consumer/main.go 127.0.0.1:9092 group1 host4 topic2
