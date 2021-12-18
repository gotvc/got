#!/bin/sh
set -ve

CGO_ENABLED=0 go build -trimpath -ldflags="-extldflags=-static" -o $1 $2
