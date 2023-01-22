package gotkv

import (
	"github.com/gotvc/got/pkg/gotkv/kvstreams"
)

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

// AddPrefix returns a new version of root with the prefix prepended to all the keys
func AddPrefix(x Root, prefix []byte) Root {
	var first []byte
	first = append(first, prefix...)
	first = append(first, x.First...)
	y := Root{
		First: first,
		Ref:   x.Ref,
		Depth: x.Depth,
	}
	return y
}
