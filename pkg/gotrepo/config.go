package gotrepo

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/brendoncarroll/go-state/posixfs"
)

type Config struct {
	Spaces []SpaceSpec `json:"realms"`
}

func DefaultConfig() Config {
	return Config{
		Spaces: []SpaceSpec{},
	}
}

func LoadConfig(fsx posixfs.FS, p string) (*Config, error) {
	data, err := posixfs.ReadFile(context.TODO(), fsx, p)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, err
	}
	return config, nil
}

func SaveConfig(fsx posixfs.FS, p string, c Config) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return posixfs.PutFile(context.TODO(), fsx, p, 0o644, bytes.NewReader(data))
}
