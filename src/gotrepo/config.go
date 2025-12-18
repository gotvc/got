package gotrepo

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/gotcfg"
	"go.inet256.org/inet256/src/inet256"
)

// Config contains runtime parameters for a Repo
type Config struct {
	// Spaces contain named mutable references to Snapshots
	// They are most similar to git remotes.
	Spaces []SpaceConfig `json:"spaces"`
	// Identities are named identities, which refer to files in the .got/iden directory
	Identities map[string]inet256.ID `json:"identities"`
	// Blobcache configures access to a Blobcache service.
	// Got stores most of it's data in Blobcache.
	Blobcache BlobcacheSpec `json:"blobcache"`
	// RepoVolume is the OID of the volume that stores the repo's data.
	// This is different than the volume for the namespace.
	// This volume will have a link to the namespace volume.
	RepoVolume blobcache.OID `json:"repo_volume"`
}

func (c *Config) Validate() error {
	slices.SortStableFunc(c.Spaces, func(a, b SpaceConfig) int {
		return strings.Compare(a.Name, b.Name)
	})
	for i := 1; i < len(c.Spaces); i++ {
		if c.Spaces[i-1].Name == c.Spaces[i].Name {
			return fmt.Errorf("config: spaces must have different names. %v, %v", c.Spaces[i-1], c.Spaces[i])
		}
	}
	return nil
}

// SpaceConfig is an element of a Config, which configures a named Space.
type SpaceConfig struct {
	Name string    `json:"name"`
	Spec SpaceSpec `json:"spec"`
}

func DefaultConfig() Config {
	return Config{
		Spaces: []SpaceConfig{},
		Blobcache: BlobcacheSpec{
			InProcess: &InProcessBlobcache{
				ActAs: DefaultIden,
			},
		},
		RepoVolume: blobcache.OID{},
	}
}

func LoadConfig(repo *os.Root) (*Config, error) {
	data, err := repo.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	return gotcfg.Parse[Config](data)
}

// EditConfig applies fn to the config of the repo at repoPath
func EditConfig(repo *os.Root, fn func(x Config) Config) error {
	return gotcfg.EditFile(repo, configPath, func(x Config) Config {
		if x.Identities == nil {
			x.Identities = make(map[string]inet256.ID)
		}
		return fn(x)
	})
}

func (r *Repo) Configure(fn func(x Config) Config) error {
	defer r.reloadConfig()
	return EditConfig(r.root, fn)
}
