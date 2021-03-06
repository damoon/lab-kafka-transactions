
export GOBIN=${PWD}/bin
PROTOC_VERSION=3.13.0
PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip

all: generate

generate: bin/protoc bin/genny bin/protoc-gen-go bin/gogenerate
	gogenerate -skipCache ./...

bin/protoc: bin/${PROTOC_ZIP}
	unzip -o bin/${PROTOC_ZIP} -d . bin/protoc
	#unzip -o ${PROTOC_ZIP} -d . 'include/*'
	touch bin/protoc

bin/${PROTOC_ZIP}:
	curl -L -o bin/${PROTOC_ZIP} https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}

bin/genny: go.sum
	go get github.com/cheekybits/genny

bin/protoc-gen-go: go.sum
	go get github.com/golang/protobuf/protoc-gen-go

bin/gogenerate: go.sum
	go get myitcv.io/cmd/gogenerate
