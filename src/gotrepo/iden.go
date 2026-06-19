package gotrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"

	"github.com/gotvc/got/src/gotorg"
	"github.com/gotvc/got/src/gotrepo/internal/reposchema"
	"github.com/gotvc/got/src/internal/gotcfg"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
)

const DefaultIden = "default"

func (r *Repo) Identities(ctx context.Context) (map[string]gotorg.IdentityUnit, error) {
	if err := r.syncOldIdentities(ctx); err != nil {
		return nil, err
	}
	ret := make(map[string]gotorg.IdentityUnit)
	for name, id := range r.config.Identities {
		idp, err := r.repoc.GetIdentity(ctx, r.rootVol, id)
		if err != nil {
			return nil, err
		}
		ret[name] = idp.Public()
	}
	return ret, nil
}

func (r *Repo) CreateIdentity(ctx context.Context, name string) (*inet256.ID, error) {
	if err := r.syncOldIdentities(ctx); err != nil {
		return nil, err
	}
	if _, exists := r.config.Identities[name]; exists {
		return nil, fmt.Errorf("cannot create, there is already an identity called %v", name)
	}
	idp := gotorg.GenerateIden()
	id, err := r.repoc.PostIdentity(ctx, r.rootVol, idp)
	if err != nil {
		return nil, err
	}
	if err := r.repoc.EditConfig(ctx, r.rootVol, func(xData json.RawMessage) json.RawMessage {
		x, err := gotcfg.Parse[Config](xData)
		if err != nil {
			return nil
		}
		if x.Identities == nil {
			x.Identities = make(map[string]inet256.ID)
		}
		x.Identities[name] = id
		return gotcfg.Marshal(*x)
	}); err != nil {
		return nil, err
	}
	return &id, r.reloadConfig(ctx)
}

func (r *Repo) GetIdentity(ctx context.Context, name string) (*gotorg.IdentityUnit, error) {
	idp, err := r.getPrivate(ctx, name)
	if err != nil {
		return nil, err
	}
	idenUnit := idp.Public()
	return &idenUnit, nil
}

func (r *Repo) getPrivate(ctx context.Context, name string) (*gotorg.IdenPrivate, error) {
	if err := r.syncOldIdentities(ctx); err != nil {
		return nil, err
	}
	id, exists := r.config.Identities[name]
	if !exists {
		return nil, fmt.Errorf("unknown identity %s", name)
	}
	return r.repoc.GetIdentity(ctx, r.rootVol, id)
}

// syncOldIdentities copies identies from the filesystem identity store
// into the repo's Blobcache volume, but only if the repo is configured with a dir
func (r *Repo) syncOldIdentities(ctx context.Context) error {
	if r.dir != nil {
		s, err := r.openIdenStore()
		if err != nil {
			return err
		}
		defer s.Close()
		ids, err := s.List(ctx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			idp, err := s.Get(id)
			if err != nil {
				return err
			}
			if _, err := r.repoc.PostIdentity(ctx, r.rootVol, *idp); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Repo) openIdenStore() (*idenStore, error) {
	return openIdenStore(r.dir)
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
	data, err := reposchema.MarshalIden(idp)
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
	return reposchema.ParseIden(data)
}

func (s *idenStore) List(ctx context.Context) (ret []inet256.ID, _ error) {
	ents, err := fs.ReadDir(s.root.FS(), ".")
	if err != nil {
		return ret, nil
	}
	for _, ent := range ents {
		if ent.IsDir() {
			continue
		}
		var id inet256.ID
		if err := id.UnmarshalText([]byte(ent.Name())); err != nil {
			logctx.Error(ctx, "parsing iden filepath", zap.Error(err))
			continue
		}
		ret = append(ret, id)
	}
	return ret, nil
}
