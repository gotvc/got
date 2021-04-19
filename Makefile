
.PHONY: test testv

test: protobuf
	go test ./...
testv: protobuf
	go test -v -count=1 ./...

install: protobuf
	go install ./cmd/got

protobuf:
	cd ./pkg/gotfs && ./build_protobuf.sh

