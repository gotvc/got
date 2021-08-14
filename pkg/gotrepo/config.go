package gotrepo

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-state/fs"
)

type Config struct {
	Spaces []SpaceSpec `json:"realms"`
}

func DefaultConfig() Config {
	return Config{
		Spaces: []SpaceSpec{},
	}
}

func LoadConfig(fsx fs.FS, p string) (*Config, error) {
	data, err := fs.ReadFile(context.TODO(), fsx, p)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, err
	}
	return config, nil
}

func SaveConfig(fsx fs.FS, p string, c Config) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return fs.PutFile(context.TODO(), fsx, p, 0o644, bytes.NewReader(data))
}
