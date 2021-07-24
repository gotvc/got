
.PHONY: test testv

test: protobuf
	go test ./...
testv: protobuf
	go test -v -count=1 ./...

install: protobuf
	go install ./cmd/got

protobuf:
	cd ./pkg/gotfs && ./build_protobuf.sh

add-replace:
	go mod edit -replace=github.com/brendoncarroll/go-state=../../brendoncarroll/go-state
	go mod edit -replace=github.com/inet256/inet256=../../inet256/inet256

drop-replace:
	go mod edit -dropreplace=github.com/brendoncarroll/go-state
	go mod edit -dropreplace=github.com/inet256/inet256
