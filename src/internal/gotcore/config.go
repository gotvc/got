package gotcore

import (
	"encoding/hex"
	"encoding/json"

	"github.com/gotvc/got/src/gdat"
	"github.com/gotvc/got/src/gotfs"
	"github.com/gotvc/got/src/gotvc"
)

// DSConfig holds all data structure parameters
type DSConfig struct {
	// Salt is a 32-byte salt used to derive the cryptographic keys for the mark.
	Salt Salt `json:"salt"`
	// GotFS contains all configuration for GotFS
	GotFS FSConfig `json:"fs"`
}

func (cfg DSConfig) Marshal(out []byte) []byte {
	data, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return append(out, data...)
}

func (cfg DSConfig) Hash() [32]byte {
	return gdat.Hash(cfg.Marshal(nil))
}

// Config contains all parameters.
type FSConfig struct {
	Data     ChunkingConfig    `json:"data_chunking"`
	Metadata Chunking_CDConfig `json:"metadata_chunking"`
}

// Salt is a 32-byte salt
type Salt [32]byte

func (s Salt) MarshalText() ([]byte, error) {
	return []byte(hex.EncodeToString(s[:])), nil
}

func (s *Salt) UnmarshalText(data []byte) error {
	_, err := hex.Decode(s[:], data)
	return err
}

func (s *Salt) String() string {
	return hex.EncodeToString(s[:])
}

type ChunkingConfig struct {
	CD  *Chunking_CDConfig `json:"cd,omitempty"`
	Max *Chunking_Fixed    `json:"fixed,omitempty"`
}

type Chunking_Fixed struct {
	Max uint32 `json:"max"`
	Min uint32 `json:"min"`
}

type Chunking_CDConfig struct {
	MeanSize int `json:"mean_size"`
	MaxSize  int `json:"max_size"`
}

func DefaultConfig(public bool) DSConfig {
	var salt Salt
	if !public {
		readRandom(salt[:])
	}
	return DSConfig{
		Salt: salt,
		GotFS: FSConfig{
			Data: ChunkingConfig{CD: &Chunking_CDConfig{
				MeanSize: gotfs.DefaultMeanBlobSizeData,
				MaxSize:  gotfs.DefaultMaxBlobSize,
			}},
			Metadata: Chunking_CDConfig{
				MeanSize: gotfs.DefaultMeanBlobSizeMetadata,
				MaxSize:  gotfs.DefaultMaxBlobSize,
			},
		},
	}
}

func GotFS(cfg DSConfig) *gotfs.Machine {
	return newGotFS(&cfg)
}

func GotVC(cfg DSConfig) *gotvc.Machine[Payload] {
	return newGotVC(&cfg)
}
