package pkecell

import "golang.org/x/crypto/sha3"

type HandshakeState[Private, Public any] struct {
	kem KEMScheme[Private, Public]

	h sha3.ShakeHash
}

//func (hs *HandshakeState)
