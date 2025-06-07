package gotrepo

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"

	"go.brendoncarroll.net/state/posixfs"
	"go.inet256.org/inet256/pkg/inet256"
	"go.inet256.org/inet256/pkg/serde"
)

const pemTypePrivateKey = "PRIVATE KEY"

type PrivateKey = inet256.PrivateKey

func (r *Repo) GetID() inet256.Addr {
	return inet256.NewAddr(r.privateKey.Public())
}

func (r *Repo) GetPrivateKey() inet256.PrivateKey {
	return r.privateKey
}

func LoadPrivateKey(fsx posixfs.FS, p string) (inet256.PrivateKey, error) {
	data, err := posixfs.ReadFile(context.TODO(), fsx, p)
	if err != nil {
		return nil, err
	}
	return parsePrivateKey(data)
}

func SavePrivateKey(fsx posixfs.FS, p string, privateKey inet256.PrivateKey) error {
	data := marshalPrivateKey(privateKey)
	return writeIfNotExists(fsx, p, 0o600, bytes.NewReader(data))
}

func writeIfNotExists(fsx posixfs.FS, p string, mode posixfs.FileMode, r io.Reader) error {
	f, err := fsx.OpenFile(p, posixfs.O_EXCL|posixfs.O_CREATE|posixfs.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return err
	}
	return f.Close()
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
		return nil, fmt.Errorf("no PEM block found")
	}
	if b.Type != pemTypePrivateKey {
		return nil, fmt.Errorf("PEM block is wrong type HAVE: %s, WANT: %s", b.Type, ty)
	}
	return b.Bytes, nil
}

func marshalPrivateKey(x inet256.PrivateKey) []byte {
	return marshalPEM(pemTypePrivateKey, serde.MarshalPrivateKey(x))
}

func parsePrivateKey(data []byte) (inet256.PrivateKey, error) {
	data, err := parsePEM(pemTypePrivateKey, data)
	if err != nil {
		return nil, err
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(data)
	if err != nil {
		return nil, err
	}
	return inet256.PrivateKeyFromBuiltIn(privateKey.(crypto.Signer))
}

func generatePrivateKey() inet256.PrivateKey {
	_, priv, err := inet256.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	return priv
}
