#!/bin/sh

GOPATH=$(go env GOPATH)

capnp compile -I$GOPATH/pkg/mod/zombiezen.com/go/capnproto2@v2.18.0+incompatible/std -ogo *.capnp
