package gotrepo

import (
	"context"
	"encoding/json"
	"os"
	"regexp"

	"github.com/gotvc/got/src/internal/gotcfg"
	"go.inet256.org/inet256/src/inet256"
)

// Config contains runtime parameters for a Repo
type Config struct {
	// Identities are named identities stored in repo schema and referenced by ID.
	Identities map[string]inet256.ID `json:"identities"`
	// Spaces contain named mutable references (Bookmarks) to Commits
	// They are most similar to git remotes.
	Spaces map[string]SpaceSpec `json:"spaces"`
	Pull   []PullConfig         `json:"pull"`
	Push   []PushConfig         `json:"push"`
}

func (c *Config) Validate() error {
	return nil
}

func (c *Config) PutSpace(name string, spec SpaceSpec) *Config {
	c.Spaces[name] = spec
	return c
}

func (c *Config) AddPull(fc PullConfig) *Config {
	c.Pull = append(c.Pull, fc)
	return c
}

func (c *Config) AddPush(fc PushConfig) *Config {
	c.Push = append(c.Push, fc)
	return c
}

// PullConfig configures a pull task.
type PullConfig struct {
	// From is the name of the space to pull from.
	From string `json:"from"`
	// Filter is a regexp for which marks to fetch from the source space.
	Filter *regexp.Regexp `json:"filter,omitempty"`
	// CutPrefix is the prefix to remove from the name
	// The zero value does not change the name at all.
	CutPrefix string `json:"cut_prefix"`
	// AddPrefix is the prefix to add to the name
	// before inserting into the local space.
	// The zero value does not change the name at all.
	AddPrefix string `json:"add_prefix"`
}

// PushConfig configures a distribution task.
type PushConfig struct {
	// Filter is a regexp for which marks to fetch from the local space.
	// If this is nil, then all names are matched.
	// This is the first operation applied
	Filter *regexp.Regexp `json:"filter,omitempty"`
	// CutPrefix is the prefix to remove from the name
	// The zero value does not change the name at all.
	// This is the second operation applied
	CutPrefix string `json:"cut_prefix"`
	// AddPrefix is the prefix to add to the name
	// before inserting into the remote space.
	// The zero value does not change the name at all.
	// This is the third operation applied
	AddPrefix string `json:"add_prefix"`
	// To is the name of the space to write to.
	To string `json:"to"`
}

func DefaultConfig() Config {
	return Config{
		Spaces: map[string]SpaceSpec{},
	}
}

func LoadConfig(repo *os.Root) (*Config, error) {
	data, err := repo.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	return gotcfg.Parse[Config](data)
}

// editConfig applies fn to the config of the repo at repoPath
func editConfig(repo *os.Root, fn func(x Config) (Config, error)) error {
	return gotcfg.EditFile(repo, configPath, func(x Config) (Config, error) {
		// maps
		if x.Identities == nil {
			x.Identities = make(map[string]inet256.ID)
		}
		if x.Spaces == nil {
			x.Spaces = map[string]SpaceSpec{}
		}
		// slices
		if x.Pull == nil {
			x.Pull = []PullConfig{}
		}
		if x.Push == nil {
			x.Push = []PushConfig{}
		}
		return fn(x)
	})
}

func (r *Repo) Configure(ctx context.Context, fn func(x Config) (Config, error)) error {
	if r.dir != nil {
		if err := editConfig(r.dir, fn); err != nil {
			return err
		}
	} else {
		if err := r.repoc.EditConfig(ctx, r.rootVol, func(xData json.RawMessage) (json.RawMessage, error) {
			x, err := gotcfg.Parse[Config](xData)
			if err != nil {
				panic(err) // TODO: move Config into reposchema
			}
			y, err := fn(*x)
			return gotcfg.Marshal(y), nil
		}); err != nil {
			return err
		}
	}
	return r.reloadConfig(ctx)
}
