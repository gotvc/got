package gotkvdelta

import "github.com/gotvc/got/src/gotkv"

type (
	Span  = gotkv.Span
	Entry = gotkv.Entry
	Edit  = gotkv.Edit
)

type Machine struct {
	kv *gotkv.Machine
}

func NewMachine(m *gotkv.Machine) Machine {
	return Machine{kv: m}
}
