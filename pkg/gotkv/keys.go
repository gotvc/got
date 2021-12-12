package gotkv

import "github.com/gotvc/got/pkg/gotkv/kvstreams"

func PrefixEnd(prefix []byte) []byte {
	return kvstreams.PrefixEnd(prefix)
}

func PrefixSpan(prefix []byte) Span {
	return kvstreams.PrefixSpan(prefix)
}

func KeyAfter(x []byte) []byte {
	return kvstreams.KeyAfter(x)
}

func TotalSpan() Span {
	return kvstreams.TotalSpan()
}

func SingleKeySpan(k []byte) Span {
	return kvstreams.SingleItemSpan(k)
}
