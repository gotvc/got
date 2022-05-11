package goturl

type Snapish struct {
	CID    [32]byte
	Branch string
	Offset uint
}

func (s Snapish) String() string {
	panic("not implemented")
}
