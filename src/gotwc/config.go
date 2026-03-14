package gotwc

import (
	"os"
	"slices"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotrepo"
	"github.com/gotvc/got/src/internal/gotcfg"
)

type Config struct {
	ID gotrepo.WorkingCopyID `json:"id"`
	// SaveTo is the name of the Mark to update when a new commit is made.
	// When it is the empty string, no marks will be updated on commit.
	SaveTo string `json:"save_to"`
	// Base are refs to the previous Commits
	// They will be the parents when the transaction is committed.
	Base []gdat.Ref `json:"base,omitempty"`

	ActAs   string `json:"act_as"`
	RepoDir string `json:"repo"`
	// Tracking is a list of tracked prefixes
	Tracking []string `json:"tracking"`
}

func DefaultConfig() Config {
	return Config{
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
	return gotcfg.EditFile(wcRoot, configPath, fn)
}
