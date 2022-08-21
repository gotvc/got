package pkecell

import (
	"testing"

	"github.com/gotvc/got/pkg/cells/pkecell/kem"
	"github.com/gotvc/got/pkg/cells/pkecell/sign"
)

func TestX448(t *testing.T) {
	kem.TestScheme(t, NewX448())
}

func TestKyber1024(t *testing.T) {
	kem.TestScheme(t, NewKyber1024())
}

func TestEd448(t *testing.T) {
	sign.TestScheme(t, NewEd448())
}

func TestDilithium(t *testing.T) {
	sign.TestScheme(t, NewDilithium5())
}

func TestX448Kyber1024(t *testing.T) {
	kem.TestScheme(t, kem.NewDualKEM(NewX448(), NewKyber1024()))
}

func TestV1Overhead(t *testing.T) {
	s := NewSchemeV1()
	t.Log(s.Overhead(1))
	t.Log(s.Overhead(10))
}
