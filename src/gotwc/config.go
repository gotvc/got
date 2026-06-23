package gotwc

import (
	"os"
	"slices"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/gotbc"
	"github.com/gotvc/got/src/internal/gotcfg"
)

type Config struct {
	// Blobcache describes how to access to Blobcache
	Blobcache BlobcacheSpec `json:"blobcache"`
	// Repo is the OID of the volume that stores the repo's data.
	// This is different than the volume for the namespace.
	// This volume will have a link to the namespace volume.
	Repo blobcache.OID `json:"repo"`

	ID gotrepo.WorkingCopyID `json:"id"`
	// SaveTo is the name of the Mark to update when a new commit is made.
	// When it is the empty string, no marks will be updated on commit.
	SaveTo string `json:"save_to"`
	// Base are refs to the previous Commits
	// They will be the parents when the transaction is committed.
	Base []gdat.Ref `json:"base"`

	ActAs string `json:"act_as"`
	// Tracking is a list of tracked prefixes
	Tracking []string `json:"tracking"`
}

type BlobcacheSpec = gotbc.Config

func DefaultConfig() Config {
	return Config{
		Blobcache: gotbc.Config{
			InProcess: &gotbc.InProcessSpec{},
		},
		ID:       gotrepo.NewWorkingCopyID(),
		SaveTo:   nameMaster,
		ActAs:    gotrepo.DefaultIden,
		Tracking: []string{""},
	}
}

func SaveConfig(wcRoot *os.Root, cfg Config) error {
	if cfg.Tracking == nil {
		cfg.Tracking = []string{}
	}
	slices.Sort(cfg.Tracking)
	return gotcfg.CreateFile(wcRoot, configPath, cfg)
}

func LoadConfig(wcRoot *os.Root) (*Config, error) {
	data, err := wcRoot.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	return gotcfg.Parse[Config](data)
}

func EditConfig(wcRoot *os.Root, fn func(x Config) Config) error {
	return gotcfg.EditFile(wcRoot, configPath, func(x Config) (Config, error) {
		return fn(x), nil
	})
}
