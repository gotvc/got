
test: capnp
	go test ./...

testv: capnp
	go test -v -count=1 ./...

bench:
	go test -v -bench=. ./... -run Benchmark

install: capnp
	go install ./cmd/got

capnp:
	cd ./src/gotfs/gotfscnp && ./build_cnp.sh

build: capnp
	- rm -r ./build/out/*
	GOOS=darwin GOARCH=arm64 ./etc/build_go_binary.sh ./build/out/got_darwin-arm64 ./cmd/got
	GOOS=linux GOARCH=amd64 ./etc/build_go_binary.sh ./build/out/got_linux-amd64 ./cmd/got

docker:
	docker build -t got:local .

precommit: test
	go mod tidy
