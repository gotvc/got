package gotrepo

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"

	"go.brendoncarroll.net/state/posixfs"
)

type Config struct {
	Spaces    MultiSpaceSpec `json:"spaces"`
	Blobcache BlobcacheSpec  `json:"blobcache"`
}

type BlobcacheSpec struct {
	// InProcess runs a local blobcache instance within the repository.
	InProcess *struct{} `json:"in_process,omitempty"`
	// HTTP connects to blobcache over HTTP
	HTTP *string `json:"http,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		Spaces: []SpaceLayerSpec{},
		Blobcache: BlobcacheSpec{
			InProcess: &struct{}{},
		},
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

// ConfigureRepo applies fn to the config of the repo at repoPath
func ConfigureRepo(repoPath string, fn func(x Config) Config) error {
	fs := posixfs.NewOSFS()
	p := filepath.Join(repoPath, filepath.FromSlash(configPath))
	configX, err := LoadConfig(fs, p)
	if err != nil {
		return err
	}
	configY := fn(*configX)
	return SaveConfig(fs, p, configY)
}
