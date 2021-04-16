package got

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/got/pkg/fs"
	"github.com/pkg/errors"
)

const pemTypePrivateKey = "PRIVATE KEY"

type PrivateKey = p2p.PrivateKey

func (r *Repo) GetID() p2p.PeerID {
	return p2p.NewPeerID(r.privateKey.Public())
}

func (r *Repo) GetPrivateKey() p2p.PrivateKey {
	return r.privateKey
}

func LoadPrivateKey(fsx fs.FS, p string) (p2p.PrivateKey, error) {
	data, err := fs.ReadFile(fsx, p)
	if err != nil {
		return nil, err
	}
	return parsePrivateKey(data)
}

func SavePrivateKey(fsx fs.FS, p string, privateKey p2p.PrivateKey) error {
	return fs.WriteIfNotExists(fsx, p, marshalPrivateKey(privateKey))
}

func marshalPEM(ty string, data []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  ty,
		Bytes: data,
	})
}

func parsePEM(ty string, pemData []byte) ([]byte, error) {
	b, _ := pem.Decode(pemData)
	if b == nil {
		return nil, errors.Errorf("no PEM block found")
	}
	if b.Type != pemTypePrivateKey {
		return nil, errors.Errorf("PEM block is wrong type HAVE: %s, WANT: %s", b.Type, ty)
	}
	return b.Bytes, nil
}

func marshalPrivateKey(x p2p.PrivateKey) []byte {
	data, err := x509.MarshalPKCS8PrivateKey(x)
	if err != nil {
		panic(err)
	}
	return marshalPEM(pemTypePrivateKey, data)
}

func parsePrivateKey(data []byte) (p2p.PrivateKey, error) {
	data, err := parsePEM(pemTypePrivateKey, data)
	if err != nil {
		return nil, err
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(data)
	if err != nil {
		return nil, err
	}
	return privateKey.(p2p.PrivateKey), nil
}

func generatePrivateKey() p2p.PrivateKey {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return priv
}
