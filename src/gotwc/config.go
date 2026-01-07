package gotwc

import (
	"os"
	"slices"

	"github.com/gotvc/got/src/internal/gotcfg"
)

type Config struct {
	Head    string `json:"head"`
	ActAs   string `json:"act_as"`
	RepoDir string `json:"repo"`
	// Tracking is a list of tracked prefixes
	Tracking []string `json:"tracking"`
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
