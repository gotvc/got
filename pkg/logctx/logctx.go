package logctx

import (
	"context"

	"github.com/sirupsen/logrus"
)

type ctxKey uint8

func WithLogger(ctx context.Context, x logrus.FieldLogger) context.Context {
	return context.WithValue(ctx, ctxKey(0), x)
}

func FromContext(ctx context.Context) logrus.FieldLogger {
	v := ctx.Value(ctxKey(0))
	if v == nil {
		return nil
	}
	return v.(logrus.FieldLogger)
}

func Debugf(ctx context.Context, fstr string, args ...any) {
	l := FromContext(ctx)
	if l == nil {
		return
	}
	l.Debugf(fstr, args...)
}

func Infof(ctx context.Context, fstr string, args ...any) {
	l := FromContext(ctx)
	if l == nil {
		return
	}
	l.Infof(fstr, args...)
}

func Warnf(ctx context.Context, fstr string, args ...any) {
	l := FromContext(ctx)
	if l == nil {
		return
	}
	l.Warnf(fstr, args...)
}

func Errorf(ctx context.Context, fstr string, args ...any) {
	l := FromContext(ctx)
	if l == nil {
		return
	}
	l.Errorf(fstr, args...)
}
