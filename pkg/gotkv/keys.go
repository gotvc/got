package gotkv

import (
	"github.com/gotvc/got/pkg/gotkv/ptree"
)

// PrefixSpan returns a Span that includes all keys with prefix p
func PrefixSpan(p []byte) Span {
	return Span{
		Start: []byte(p),
		End:   PrefixEnd([]byte(p)),
	}
}

// PrefixEnd return the key > all the keys with prefix p, but < any other key
func PrefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	var end []byte
	for i := len(prefix) - 1; i >= 0; i-- {
		c := prefix[i]
		if c < 0xff {
			end = make([]byte, i+1)
			copy(end, prefix)
			end[i] = c + 1
			break
		}
	}
	return end
}

func KeyAfter(x []byte) []byte {
	return ptree.KeyAfter(x)
}

func TotalSpan() Span {
	return ptree.TotalSpan()
}

func SingleKeySpan(k []byte) Span {
	return ptree.SingleItemSpan(k)
}
