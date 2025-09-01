
test: protobuf
	go test ./...

testv: protobuf
	go test -v -count=1 ./...

bench:
	go test -v -bench=. ./... -run Benchmark

install: protobuf
	go install ./cmd/got

protobuf:
	cd ./src/gotfs && ./build_protobuf.sh

build: protobuf
	rm -r ./out/*
	GOOS=darwin GOARCH=amd64 ./etc/build_go_binary.sh out/got_darwin-amd64_$(TAG) ./cmd/got
	GOOS=linux GOARCH=amd64 ./etc/build_go_binary.sh out/got_linux-amd64_$(TAG) ./cmd/got
	GOOS=windows GOARCH=amd64 ./etc/build_go_binary.sh out/got_windows-amd64_$(TAG) ./cmd/got

docker:
	docker build -t got:local .

precommit: test
	go mod tidy
