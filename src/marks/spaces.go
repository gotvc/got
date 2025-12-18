package marks

import (
	"context"
	"fmt"
	"regexp"

	"errors"
)

var (
	ErrNotExist = errors.New("mark does not exist")
	ErrExists   = errors.New("a mark already exists by that name")
)

func IsNotExist(err error) bool {
	return errors.Is(err, ErrNotExist)
}

func IsExists(err error) bool {
	return errors.Is(err, ErrNotExist)
}

var nameRegExp = regexp.MustCompile(`^[\w-/=_.]+$`)

const MaxNameLen = 1024

type ErrInvalidName struct {
	Name   string
	Reason string
}

func (e ErrInvalidName) Error() string {
	return fmt.Sprintf("invalid mark name: %q reason: %v", e.Name, e.Reason)
}

func CheckName(name string) error {
	if len(name) > MaxNameLen {
		return ErrInvalidName{
			Name:   name,
			Reason: "too long",
		}
	}
	if !nameRegExp.MatchString(name) {
		return ErrInvalidName{
			Name:   name,
			Reason: "contains invalid characters (must match " + nameRegExp.String() + " )",
		}
	}
	return nil
}

type Span struct {
	Begin string
	End   string
}

func TotalSpan() Span {
	return Span{}
}

func (s Span) Contains(x string) bool {
	return s.Begin <= x && (s.End == "" || s.End > x)
}

// A Space holds named marks.
type Space interface {
	Create(ctx context.Context, name string, cfg Params) (*Info, error)
	Inspect(ctx context.Context, name string) (*Info, error)
	Set(ctx context.Context, name string, cfg Params) error
	Delete(ctx context.Context, name string) error
	List(ctx context.Context, span Span, limit int) ([]string, error)

	// Open returns a volume for viewing and modifying the mark contents.
	Open(ctx context.Context, name string) (*Mark, error)
}

func CreateIfNotExists(ctx context.Context, r Space, k string, cfg Params) (*Info, error) {
	mark, err := r.Inspect(ctx, k)
	if err != nil {
		if IsNotExist(err) {
			return r.Create(ctx, k, cfg)
		}
		return nil, err
	}
	return mark, nil
}

// ForEach is a convenience function which uses Space.List to call fn with
// all the mark names contained in span.
func ForEach(ctx context.Context, s Space, span Span, fn func(string) error) (retErr error) {
	for {
		names, err := s.List(ctx, span, 0)
		if err != nil {
			retErr = err
		}
		if len(names) == 0 {
			break
		}
		for _, name := range names {
			if err := fn(name); err != nil {
				return err
			}
		}
		span.Begin = names[len(names)-1] + "\x00"
	}
	return retErr
}
