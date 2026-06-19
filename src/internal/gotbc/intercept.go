package gotbc

import (
	"context"
	"net"

	"blobcache.io/blobcache/src/bclocal"
	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/stdctx/logctx"
	"go.inet256.org/inet256/src/inet256"
	"go.uber.org/zap"
)

var _ blobcache.Service = &Local{}

type Local struct {
	svc    *bclocal.Service
	logger *zap.Logger
	pol    *bcPolicy
}

func (b *Local) LocalNode() blobcache.NodeID {
	return b.svc.LocalID()
}

func (b *Local) SetPolicy(canLook, canTouch []inet256.ID) {
	b.pol.Update(canLook, canTouch)
}

func (b *Local) Serve(ctx context.Context, pc net.PacketConn) error {
	return b.svc.Serve(b.ctx(ctx), pc)
}

func (b *Local) Close() error {
	return b.svc.Close()
}

func (b *Local) ctx(ctx context.Context) context.Context {
	return logctx.NewContext(ctx, b.logger)
}

func (b *Local) Endpoint(ctx context.Context) (blobcache.Endpoint, error) {
	return b.svc.Endpoint(b.ctx(ctx))
}

func (b *Local) Inspect(ctx context.Context, h blobcache.Handle) (blobcache.Info, error) {
	return b.svc.Inspect(b.ctx(ctx), h)
}

func (b *Local) Drop(ctx context.Context, h blobcache.Handle) error {
	return b.svc.Drop(b.ctx(ctx), h)
}

func (b *Local) KeepAlive(ctx context.Context, hs []blobcache.Handle) error {
	return b.svc.KeepAlive(b.ctx(ctx), hs)
}

func (b *Local) InspectHandle(ctx context.Context, h blobcache.Handle) (*blobcache.HandleInfo, error) {
	return b.svc.InspectHandle(b.ctx(ctx), h)
}

func (b *Local) ShareOut(ctx context.Context, h blobcache.Handle, to blobcache.NodeID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return b.svc.ShareOut(b.ctx(ctx), h, to, mask)
}

func (b *Local) ShareIn(ctx context.Context, host blobcache.NodeID, h blobcache.Handle) (blobcache.Handle, error) {
	return b.svc.ShareIn(ctx, host, h)
}

func (b *Local) CreateVolume(ctx context.Context, host *blobcache.Endpoint, vspec blobcache.VolumeSpec) (*blobcache.Handle, error) {
	return b.svc.CreateVolume(b.ctx(ctx), host, vspec)
}

func (b *Local) InspectVolume(ctx context.Context, h blobcache.Handle) (*blobcache.VolumeInfo, error) {
	return b.svc.InspectVolume(b.ctx(ctx), h)
}

func (b *Local) OpenFiat(ctx context.Context, x blobcache.OID, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return b.svc.OpenFiat(b.ctx(ctx), x, mask)
}

func (b *Local) OpenFrom(ctx context.Context, base blobcache.Handle, ltok blobcache.LinkToken, mask blobcache.ActionSet) (*blobcache.Handle, error) {
	return b.svc.OpenFrom(b.ctx(ctx), base, ltok, mask)
}

func (b *Local) BeginTx(ctx context.Context, volh blobcache.Handle, txp blobcache.TxParams) (*blobcache.Handle, error) {
	return b.svc.BeginTx(b.ctx(ctx), volh, txp)
}

func (b *Local) InspectTx(ctx context.Context, tx blobcache.Handle) (*blobcache.TxInfo, error) {
	return b.svc.InspectTx(b.ctx(ctx), tx)
}

func (b *Local) Commit(ctx context.Context, tx blobcache.Handle) error {
	return b.svc.Commit(b.ctx(ctx), tx)
}

func (b *Local) Abort(ctx context.Context, tx blobcache.Handle) error {
	return b.svc.Abort(b.ctx(ctx), tx)
}

func (b *Local) Load(ctx context.Context, tx blobcache.Handle, dst *[]byte) error {
	return b.svc.Load(b.ctx(ctx), tx, dst)
}

func (b *Local) Save(ctx context.Context, tx blobcache.Handle, src []byte) error {
	return b.svc.Save(b.ctx(ctx), tx, src)
}

func (b *Local) Post(ctx context.Context, tx blobcache.Handle, data []byte, opts blobcache.PostOpts) (blobcache.CID, error) {
	return b.svc.Post(b.ctx(ctx), tx, data, opts)
}

func (b *Local) Get(ctx context.Context, tx blobcache.Handle, cid blobcache.CID, buf []byte, opts blobcache.GetOpts) (int, error) {
	return b.svc.Get(b.ctx(ctx), tx, cid, buf, opts)
}

func (b *Local) Exists(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, dst *blobcache.BitMap) error {
	return b.svc.Exists(b.ctx(ctx), tx, cids, dst)
}

func (b *Local) Delete(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return b.svc.Delete(b.ctx(ctx), tx, cids)
}

func (b *Local) Copy(ctx context.Context, tx blobcache.Handle, srcTxns []blobcache.Handle, cids []blobcache.CID, success []bool) error {
	return b.svc.Copy(b.ctx(ctx), tx, srcTxns, cids, success)
}

func (b *Local) Visit(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID) error {
	return b.svc.Visit(b.ctx(ctx), tx, cids)
}

func (b *Local) IsVisited(ctx context.Context, tx blobcache.Handle, cids []blobcache.CID, yesVisited *blobcache.BitMap) error {
	return b.svc.IsVisited(b.ctx(ctx), tx, cids, yesVisited)
}

func (b *Local) Link(ctx context.Context, tx blobcache.Handle, target blobcache.Handle, mask blobcache.ActionSet) (*blobcache.LinkToken, error) {
	return b.svc.Link(b.ctx(ctx), tx, target, mask)
}

func (b *Local) Unlink(ctx context.Context, tx blobcache.Handle, ltoks []blobcache.LinkTokenID) error {
	return b.svc.Unlink(b.ctx(ctx), tx, ltoks)
}

func (b *Local) VisitLinks(ctx context.Context, tx blobcache.Handle, targets []blobcache.LinkTokenID) error {
	return b.svc.VisitLinks(b.ctx(ctx), tx, targets)
}

func (b *Local) CreateQueue(ctx context.Context, host *blobcache.Endpoint, qspec blobcache.QueueSpec) (*blobcache.Handle, error) {
	return b.svc.CreateQueue(b.ctx(ctx), host, qspec)
}

func (b *Local) InspectQueue(ctx context.Context, qh blobcache.Handle) (blobcache.QueueInfo, error) {
	return b.svc.InspectQueue(b.ctx(ctx), qh)
}

func (b *Local) Dequeue(ctx context.Context, q blobcache.Handle, buf []blobcache.Message, opts blobcache.DequeueOpts) (int, error) {
	return b.svc.Dequeue(b.ctx(ctx), q, buf, opts)
}

func (b *Local) Enqueue(ctx context.Context, q blobcache.Handle, msgs []blobcache.Message) (*blobcache.InsertResp, error) {
	return b.svc.Enqueue(b.ctx(ctx), q, msgs)
}

func (b *Local) SubToVolume(ctx context.Context, q blobcache.Handle, vol blobcache.Handle, spec blobcache.VolSubSpec) error {
	return b.svc.SubToVolume(b.ctx(ctx), q, vol, spec)
}
