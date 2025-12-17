package gotcfg

import (
	"encoding/json"
	"os"
)

func Marshal(x any) []byte {
	data, err := json.MarshalIndent(x, "", "  ")
	if err != nil {
		panic(err)
	}
	return data
}

func Unmarshal[T any](data []byte, x *T) error {
	return json.Unmarshal(data, x)
}

func Parse[T any](data []byte) (*T, error) {
	var ret T
	if err := Unmarshal(data, &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}

func CreateFile[T any](root *os.Root, p string, cfg T) error {
	f, err := root.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(Marshal(cfg)); err != nil {
		return err
	}
	return f.Sync()
}

func LoadFile[T any](root *os.Root, p string) (*T, error) {
	data, err := root.ReadFile(p)
	if err != nil {
		return nil, err
	}
	return Parse[T](data)
}

func EditFile[T any](root *os.Root, p string, fn func(T) T) error {
	cfg, err := LoadFile[T](root, p)
	if err != nil {
		return err
	}
	cfg2 := fn(*cfg)
	// TODO: we should rename a new temp file into place.
	return root.WriteFile(p, Marshal(cfg2), 0o644)
}
