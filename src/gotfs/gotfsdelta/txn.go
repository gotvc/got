package gotfsdelta

import "github.com/gotvc/got/src/gotkv/gotkvdelta"

type Txn struct {
	// Delta is the set of changes to be applied in the transaction
	Delta Delta
	// ReadSet is the set of items read during the transaction.
	ReadSet gotkvdelta.SpanSet
}
