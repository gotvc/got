package gotrepo

import (
	"context"
	"fmt"

	"github.com/cloudflare/circl/kem"
	"github.com/cloudflare/circl/kem/mlkem/mlkem1024"
	"github.com/cloudflare/circl/sign"
	"github.com/cloudflare/circl/sign/ed25519"
	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/internal/dbutil"
	"go.inet256.org/inet256/src/inet256"
)

func (r *Repo) GotNSClient() gotns.Client {
	return gotns.Client{
		Machine:   gotns.New(),
		Blobcache: r.bc,
		ActAs:     r.privateKey,
	}
}

func (r *Repo) ActiveIdentity(ctx context.Context) (gotns.IdentityLeaf, error) {
	return dbutil.DoTx1(ctx, r.db, getActiveIdentity)
}

func (r *Repo) IntroduceSelf(ctx context.Context) (gotns.Op_ChangeSet, error) {
	leaf, err := r.ActiveIdentity(ctx)
	if err != nil {
		return gotns.Op_ChangeSet{}, err
	}
	gnsc := r.GotNSClient()
	return gnsc.IntroduceSelf(leaf.KEMPublicKey), nil
}

// setupIdentity creates a new identity with a new key pair, only if it does not already exist.
func setupIdentity(conn *dbutil.Conn) error {
	var exists bool
	if err := dbutil.Get(conn, &exists, `SELECT EXISTS(SELECT 1 FROM idens)`); err != nil {
		return err
	}
	if exists {
		return nil
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}
	id := inet256.NewID(pub)
	sigPrivData, err := inet256.DefaultPKI.MarshalPrivateKey(nil, priv)
	if err != nil {
		return err
	}
	sigPubData, err := inet256.DefaultPKI.MarshalPublicKey(nil, pub)
	if err != nil {
		return err
	}
	kemPub, kemPriv, err := mlkem1024.GenerateKeyPair(nil)
	if err != nil {
		return err
	}
	kemPubData := gotns.MarshalKEMPublicKey(nil, gotns.KEM_MLKEM1024, kemPub)
	kemPrivData := gotns.MarshalKEMPrivateKey(nil, gotns.KEM_MLKEM1024, kemPriv)

	err = dbutil.Exec(conn, `INSERT INTO idens (id, sign_private_key, sign_public_key, kem_private_key, kem_public_key)
		VALUES (?, ?, ?, ?, ?)`, id[:], sigPrivData, sigPubData, kemPrivData, kemPubData)
	return err
}

func loadIdentity(conn *dbutil.Conn) (sign.PrivateKey, kem.PrivateKey, error) {
	stmt := conn.Prep(`SELECT id, sign_private_key, kem_private_key FROM idens`)
	defer stmt.Finalize()

	var row struct {
		ID          []byte
		SigPrivData []byte
		KemPrivData []byte
	}
	ok, err := stmt.Step()
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("no identity found")
	}
	for i, dst := range []*[]byte{&row.ID, &row.SigPrivData, &row.KemPrivData} {
		*dst = make([]byte, stmt.ColumnLen(i))
		n := stmt.ColumnBytes(i, *dst)
		if n != len(*dst) {
			return nil, nil, fmt.Errorf("read wrong number of bytes")
		}
	}

	sigPriv, err := inet256.DefaultPKI.ParsePrivateKey(row.SigPrivData)
	if err != nil {
		return nil, nil, err
	}
	kemPriv, err := gotns.ParseKEMPrivateKey(row.KemPrivData)
	if err != nil {
		return nil, nil, err
	}
	return sigPriv, kemPriv, nil
}

func getActiveIdentity(conn *dbutil.Conn) (gotns.IdentityLeaf, error) {
	sigPriv, kemPriv, err := loadIdentity(conn)
	if err != nil {
		return gotns.IdentityLeaf{}, err
	}
	return gotns.NewLeaf(sigPriv.Public().(sign.PublicKey), kemPriv.Public()), nil
}
