
test: protobuf
	go test ./...

testv: protobuf
	go test -v -count=1 ./...

bench:
	go test -v -bench=. ./... -run Benchmark

install: protobuf
	go install ./cmd/got

protobuf:
	cd ./pkg/gotfs && ./build_protobuf.sh
	cd ./pkg/gotgrpc && ./build_protobuf.sh

build: protobuf
	rm -r ./out/*
	GOOS=darwin GOARCH=amd64 ./etc/build_go_binary.sh out/got_darwin-amd64_$(TAG) ./cmd/got
	GOOS=linux GOARCH=amd64 ./etc/build_go_binary.sh out/got_linux-amd64_$(TAG) ./cmd/got
	GOOS=windows GOARCH=amd64 ./etc/build_go_binary.sh out/got_windows-amd64_$(TAG) ./cmd/got

add-replace:
	go mod edit -replace=github.com/brendoncarroll/go-state=../../brendoncarroll/go-state
	go mod edit -replace=github.com/brendoncarroll/go-p2p=../../brendoncarroll/go-p2p
	go mod edit -replace=github.com/inet256/inet256=../../inet256/inet256

drop-replace:
	go mod edit -dropreplace=github.com/brendoncarroll/go-state
	go mod edit -dropreplace=github.com/brendoncarroll/go-p2p
	go mod edit -dropreplace=github.com/inet256/inet256

precommit: drop-replace test
	go mod tidy
