package gotrepo

import (
	"bytes"
	"context"
	"io"

	"go.brendoncarroll.net/state/posixfs"
	"go.inet256.org/inet256/src/inet256"
)

func (r *Repo) GetID() inet256.ID {
	return inet256.NewID(r.privateKey.Public().(inet256.PublicKey))
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

func marshalPrivateKey(x inet256.PrivateKey) []byte {
	data, err := inet256.DefaultPKI.MarshalPrivateKey(nil, x)
	if err != nil {
		panic(err)
	}
	return data
}

func parsePrivateKey(data []byte) (inet256.PrivateKey, error) {
	return inet256.DefaultPKI.ParsePrivateKey(data)
}

func generatePrivateKey() inet256.PrivateKey {
	_, priv, err := inet256.GenerateKey()
	if err != nil {
		panic(err)
	}
	return priv
}
