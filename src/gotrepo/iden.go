package gotrepo

import (
	"context"
	"fmt"
	"os"

	"github.com/gotvc/got/src/gotorg"
	"go.inet256.org/inet256/src/inet256"
)

const DefaultIden = "default"

func (r *Repo) Identities(ctx context.Context) (map[string]gotorg.IdentityUnit, error) {
	s, err := r.openIdenStore()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	ret := make(map[string]gotorg.IdentityUnit)
	for name, id := range r.config.Identities {
		idp, err := s.Get(id)
		if err != nil {
			return nil, err
		}
		ret[name] = idp.Public()
	}
	return ret, nil
}

// Create
func (r *Repo) CreateIdentity(ctx context.Context, name string) (*inet256.ID, error) {
	if _, exists := r.config.Identities[name]; exists {
		return nil, fmt.Errorf("cannot create, there is already an identity called %v", name)
	}
	s, err := r.openIdenStore()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	idp := gotorg.GenerateIden()
	if _, err := s.Add(idp); err != nil {
		return nil, err
	}
	if err := EditConfig(r.root, func(x Config) Config {
		if x.Identities == nil {
			x.Identities = make(map[string]inet256.ID)
		}
		x.Identities[name] = idp.GetID()
		return x
	}); err != nil {
		return nil, err
	}
	idu := idp.Public()
	return &idu.ID, r.reloadConfig()
}

func (r *Repo) GetIdentity(ctx context.Context, name string) (*gotorg.IdentityUnit, error) {
	idp, err := r.getPrivate(name)
	if err != nil {
		return nil, err
	}
	idenUnit := idp.Public()
	return &idenUnit, nil
}

func (r *Repo) getPrivate(name string) (*gotorg.IdenPrivate, error) {
	id, exists := r.config.Identities[name]
	if !exists {
		return nil, fmt.Errorf("unknown identity %s", name)
	}
	s, err := r.openIdenStore()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	return s.Get(id)
}

func (r *Repo) openIdenStore() (*idenStore, error) {
	return openIdenStore(r.root)
}

func openIdenStore(repo *os.Root) (*idenStore, error) {
	root, err := repo.OpenRoot(idenPath)
	if err != nil {
		return nil, err
	}
	return &idenStore{
		root: root,
	}, nil
}

// idenStore is a directory containing identity files
type idenStore struct {
	root *os.Root
}

func (s *idenStore) Close() error {
	return s.root.Close()
}

func (s *idenStore) Add(idp gotorg.IdenPrivate) (inet256.ID, error) {
	data, err := marshalIden(idp)
	if err != nil {
		return inet256.ID{}, err
	}
	id := idp.GetID()
	p := id.Base64String()
	if err := s.root.WriteFile(p, data, 0o644); err != nil {
		return inet256.ID{}, err
	}
	return id, nil
}

func (s *idenStore) Get(id inet256.Addr) (*gotorg.IdenPrivate, error) {
	data, err := s.root.ReadFile(id.Base64String())
	if err != nil {
		return nil, err
	}
	return parseIden(data)
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

var pki = gotorg.PKI()
