package gotrepo

import (
	"os"
	"regexp"

	"blobcache.io/blobcache/src/blobcache"
	"github.com/gotvc/got/src/internal/gotcfg"
	"go.inet256.org/inet256/src/inet256"
)

// Config contains runtime parameters for a Repo
type Config struct {
	// Blobcache configures access to a Blobcache service.
	// Got stores most of it's data in Blobcache.
	Blobcache BlobcacheSpec `json:"blobcache"`
	// RepoVolume is the OID of the volume that stores the repo's data.
	// This is different than the volume for the namespace.
	// This volume will have a link to the namespace volume.
	RepoVolume blobcache.OID `json:"repo_volume"`

	// Identities are named identities, which refer to files in the .got/iden directory
	Identities map[string]inet256.ID `json:"identities"`
	// Spaces contain named mutable references to Snapshots
	// They are most similar to git remotes.
	Spaces map[string]SpaceSpec `json:"spaces"`
	Fetch  []FetchConfig        `json:"fetch"`
	Dist   []DistConfig         `json:"dist"`
}

func (c *Config) Validate() error {
	return nil
}

// FetchConfig configures a fetch task.
type FetchConfig struct {
	// From is the name of the space to pull from.
	// The destination space is always assumed to be the local space.
	From string `json:"from"`
	// Filter is a regexp for which marks to fetch from the source space.
	Filter *regexp.Regexp `json:"filter"`
	// CutPrefix is the prefix to remove from the name
	// The zero value does not change the name at all.
	CutPrefix string `json:"cut_prefix"`
	// AddPrefix is the prefix to add to the name
	// before inserting into the local space.
	// The zero value does not change the name at all.
	AddPrefix string `json:"add_prefix"`
	// Delete is the regexp for which marks to delete from the destination space.
	// Only names starting with AddPrefix in the destination space are considered.
	// The regexp should match the entire name including the prefix.
	Delete *regexp.Regexp `json:"delete"`
}

// DistConfig configures a distribution  task.
type DistConfig struct {
	// Filter is a regexp for which marks to fetch from the source space.
	// In the case of distribution, the is always the local space.
	Filter *regexp.Regexp `json:"filter"`
	// CutPrefix is the prefix to remove from the name
	// The zero value does not change the name at all.
	CutPrefix string `json:"cut_prefix"`
	// AddPrefix is the prefix to add to the name
	// before inserting into the local space.
	// The zero value does not change the name at all.
	AddPrefix string `json:"add_prefix"`
	// To is the name of the space to write to.
	To string `json:"to"`
	// Delete is the regexp for which marks to delete from the destination space.
	// Only names starting with AddPrefix in the destination space are considered.
	// The regexp should match the entire name including the prefix.
	Delete *regexp.Regexp `json:"delete"`
}

func DefaultConfig() Config {
	return Config{
		Spaces: map[string]SpaceSpec{},
		Blobcache: BlobcacheSpec{
			InProcess: &InProcessBlobcache{
				ActAs:    DefaultIden,
				CanLook:  []inet256.ID{},
				CanTouch: []inet256.ID{},
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
		// maps
		if x.Identities == nil {
			x.Identities = make(map[string]inet256.ID)
		}
		if x.Spaces == nil {
			x.Spaces = map[string]SpaceSpec{}
		}
		// slices
		if x.Fetch == nil {
			x.Fetch = []FetchConfig{}
		}
		if x.Dist == nil {
			x.Dist = []DistConfig{}
		}
		return fn(x)
	})
}

func (r *Repo) Configure(fn func(x Config) Config) error {
	if err := EditConfig(r.root, fn); err != nil {
		return err
	}
	return r.reloadConfig()
}
