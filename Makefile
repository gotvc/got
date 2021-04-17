
.PHONY: test

test:
	go test -v ./...

install:
	go install ./cmd/got

