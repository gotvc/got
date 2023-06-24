package gotkv

import (
	"bytes"
	"context"

	"github.com/brendoncarroll/go-exp/maybe"
	"github.com/brendoncarroll/go-exp/streams"
	"github.com/brendoncarroll/go-state/cadata"
)

// DEntry is the delta between 2 Entries
type DEntry struct {
	Key   []byte
	Left  maybe.Maybe[[]byte]
	Right maybe.Maybe[[]byte]
}

func (op *Operator) NewDiffer(s cadata.Getter, left, right Root, span Span) *Differ {
	return &Differ{
		left:  op.NewIterator(s, left, span),
		right: op.NewIterator(s, right, span),
	}
}

type Differ struct {
	left, right *Iterator

	haveLeft, haveRight bool
	le, re              Entry
}

func (d *Differ) Next(ctx context.Context, dst *DEntry) error {
	emitted := false
	for !emitted {
		if !d.haveLeft {
			if err := d.left.Next(ctx, &d.le); err != nil && !streams.IsEOS(err) {
				return err
			} else if err == nil {
				d.haveLeft = true
			}
		}
		if !d.haveRight {
			if err := d.right.Next(ctx, &d.re); err != nil && !streams.IsEOS(err) {
				return err
			} else if err == nil {
				d.haveRight = true
			}
		}
		switch {
		case d.haveLeft && !d.haveRight:
			emitted = d.emitLeft(dst)
		case !d.haveLeft && d.haveRight:
			emitted = d.emitRight(dst)
		case d.haveLeft && d.haveRight:
			switch bytes.Compare(d.le.Key, d.re.Key) {
			case -1:
				emitted = d.emitLeft(dst)
			case 0:
				emitted = d.emitBoth(dst)
			case 1:
				emitted = d.emitRight(dst)
			default:
				panic("bytes.Compare returned value out of range [-1, 0, 1]")
			}
		default:
			return streams.EOS()
		}
	}
	return nil
}

func (d *Differ) Seek(ctx context.Context, gteq []byte) error {
	if err := d.left.Seek(ctx, gteq); err != nil {
		return err
	}
	d.haveLeft = false
	if err := d.right.Seek(ctx, gteq); err != nil {
		return err
	}
	d.haveRight = false
	return nil
}

func (d *Differ) emitLeft(dst *DEntry) bool {
	setBytes(&dst.Key, d.le.Key)

	// left value
	setBytes(&dst.Left.X, d.le.Value)
	dst.Left.Ok = true

	// right value
	setBytes(&dst.Right.X, nil)
	dst.Right.Ok = false

	d.haveLeft = false
	return true
}

func (d *Differ) emitRight(dst *DEntry) bool {
	setBytes(&dst.Key, d.re.Key)
	// left value
	setBytes(&dst.Left.X, nil)
	dst.Left.Ok = false

	// right value
	setBytes(&dst.Right.X, d.re.Value)
	dst.Right.Ok = true

	d.haveRight = false
	return true
}

func (d *Differ) emitBoth(dst *DEntry) (ret bool) {
	setBytes(&dst.Key, d.le.Key)

	if !bytes.Equal(d.le.Value, d.re.Value) {
		// left value
		setBytes(&dst.Left.X, d.le.Value)
		dst.Left.Ok = true

		// right Value
		setBytes(&dst.Right.X, d.re.Value)
		dst.Right.Ok = true
		ret = true
	} else {
		ret = false
	}

	d.haveLeft = false
	d.haveRight = false
	return ret
}

func setBytes(dst *[]byte, src []byte) {
	*dst = append((*dst)[:0], src...)
}
