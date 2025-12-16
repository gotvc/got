package gotrepo

import (
	"context"
	"fmt"
	"os"

	"github.com/gotvc/got/src/gotorg"
)

func (r *Repo) GotOrgClient() gotorg.Client {
	return gotorg.Client{
		Machine:   gotorg.New(),
		Blobcache: r.bc,
		ActAs:     r.leafPrivate,
	}
}

func (r *Repo) ActiveIdentity(ctx context.Context) (*gotorg.IdentityUnit, error) {
	idp, err := r.idenStore.Get("default")
	if err != nil {
		return nil, err
	}
	iden := idp.Public()
	return &iden, nil
}

func (r *Repo) IntroduceSelf(ctx context.Context) (gotorg.ChangeSet, error) {
	leaf, err := r.ActiveIdentity(ctx)
	if err != nil {
		return gotorg.ChangeSet{}, err
	}
	gnsc := r.GotOrgClient()
	return gnsc.IntroduceSelf(leaf.KEMPublicKey), nil
}

func marshalIden(idp gotorg.IdenPrivate) ([]byte, error) {
	sigPrivData, err := pki.MarshalPrivateKey(nil, idp.SigPrivateKey)
	if err != nil {
		return nil, err
	}
	kemPrivData := gotorg.MarshalKEMPrivateKey(nil, gotorg.KEM_MLKEM1024, idp.KEMPrivateKey)
	ret := fmt.Appendf(nil, "SIG %x\nKEM %x\n", sigPrivData, kemPrivData)
	return ret, nil
}

func parseIden(data []byte) (*gotorg.IdenPrivate, error) {
	x := string(data)
	var sigPrivData []byte
	var kemPrivData []byte
	_, err := fmt.Sscanf(x, "SIG %x\nKEM %x\n", &sigPrivData, &kemPrivData)
	if err != nil {
		return nil, err
	}
	sigPriv, err := pki.ParsePrivateKey(sigPrivData)
	if err != nil {
		return nil, err
	}
	kemPriv, err := gotorg.ParseKEMPrivateKey(kemPrivData)
	if err != nil {
		return nil, err
	}
	return &gotorg.IdenPrivate{
		SigPrivateKey: sigPriv,
		KEMPrivateKey: kemPriv,
	}, nil
}

// idenStore is a directory containing identity files
type idenStore struct {
	root *os.Root
}

func (s *idenStore) Put(name string, idp gotorg.IdenPrivate) error {
	if name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	data, err := marshalIden(idp)
	if err != nil {
		return err
	}
	return s.root.WriteFile(name, data, 0o644)
}

func (s *idenStore) Get(name string) (*gotorg.IdenPrivate, error) {
	data, err := s.root.ReadFile(name)
	if err != nil {
		return nil, err
	}
	return parseIden(data)
}

func (s *idenStore) GetOrCreate(name string) (*gotorg.IdenPrivate, error) {
	idp, err := s.Get(name)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if os.IsNotExist(err) {
		idp := gotorg.GenerateIden()
		if err := s.Put(name, idp); err != nil {
			return nil, err
		}
		return &idp, nil
	}
	return idp, nil
}

var pki = gotorg.PKI()
