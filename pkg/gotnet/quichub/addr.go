package quichub

import (
	"fmt"
	"strings"

	"github.com/inet256/inet256/pkg/inet256"
)

type Addr struct {
	ID   inet256.ID
	Addr string
}

func (a *Addr) String() string {
	return a.ID.String() + "@" + a.Addr
}

// ParseAddr parses an address of the form <id>@<host>:<port>
func ParseAddr(x string) (*Addr, error) {
	parts := strings.SplitN(x, "@", 2)
	if len(parts) < 2 {
		return nil, fmt.Errorf("missing ID")
	}
	id, err := inet256.ParseAddrB64([]byte(parts[0]))
	if err != nil {
		return nil, err
	}
	return &Addr{
		ID:   id,
		Addr: parts[0],
	}, nil
}
