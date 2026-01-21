
test: capnp
	go test ./...

testv: capnp
	go test -v -count=1 ./...

bench:
	go test -v -bench=. ./... -run Benchmark

install: build
    sudo cp ./build/out/got /usr/bin/got

capnp:
	cd ./src/gotfs/gotfscnp && ./build_cnp.sh

build: capnp
	rm ./build/out/*
	./build/go_exec.sh ./build/out/got ./cmd/got

build-amd64-linux: capnp
	mkdir -p ./build/out
	GOARCH=amd64 GOOS=linux ./build/go_exec.sh ./build/out/got_amd64-linux ./cmd/got

build-arm64-linux: capnp
    mkdir -p ./build/out
    GOARCH=arm64 GOOS=linux ./build/go_exec.sh ./build/out/got_arm64-linux ./cmd/got

build-arm64-darwin: capnp
	mkdir -p ./build/out
	GOARCH=arm64 GOOS=darwin ./build/go_exec.sh ./build/out/got_arm64-darwin ./cmd/got

build-exec: build-amd64-linux build-arm64-linux build-arm64-darwin

precommit: test
	go mod tidy
