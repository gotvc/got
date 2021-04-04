package gotkv

import (
	"github.com/brendoncarroll/got/pkg/ptree"
)

func PrefixSpan(p []byte) Span {
	return Span{
		First: []byte(p),
		Last:  PrefixEnd([]byte(p)),
	}
}

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
