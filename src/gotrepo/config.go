package gotrepo

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"

	"blobcache.io/blobcache/src/blobcache"
	"go.brendoncarroll.net/state/posixfs"
)

type Config struct {
	Spaces MultiSpaceSpec `json:"spaces"`
	// Blobcache configures access to a Blobcache service.
	// Got stores most of it's data in Blobcache.
	Blobcache BlobcacheSpec `json:"blobcache"`
	// RepoVolume is the OID of the volume that stores the repo's data.
	// This is different than the volume for the namespace.
	// This volume will have a link to the namespace volume.
	RepoVolume blobcache.OID `json:"repo_volume"`
}

func DefaultConfig() Config {
	return Config{
		Spaces: []SpaceLayerSpec{},
		Blobcache: BlobcacheSpec{
			InProcess: &struct{}{},
		},
		RepoVolume: blobcache.OID{},
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
