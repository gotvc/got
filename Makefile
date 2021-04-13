
.PHONY: test

test: protobuf
	go test -v ./...

install-capnp:
	brew install capnp
	go get -u -t zombiezen.com/go/capnproto2@v2.18.0
	go install zombiezen.com/go/capnproto2/capnpc-go

protobuf:
	cd ./pkg/ptree/ptreeproto && ./build.sh
