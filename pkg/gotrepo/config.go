package gotrepo

import (
	"encoding/json"

	"github.com/brendoncarroll/got/pkg/fs"
)

type Config struct {
	Realms []RealmSpec `json:"realms"`
}

func DefaultConfig() Config {
	return Config{
		Realms: []RealmSpec{},
	}
}

func LoadConfig(fsx fs.FS, p string) (*Config, error) {
	data, err := fs.ReadFile(fsx, p)
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
	return fs.WriteFile(fsx, p, data)
}
