package gotbc

import (
	"fmt"

	"blobcache.io/blobcache/src/blobcache"
	"blobcache.io/blobcache/src/schema"
)

var registry map[blobcache.SchemaName]schema.Constructor

func AddSchema(name blobcache.SchemaName, c schema.Constructor) {
	if _, exists := registry[name]; exists {
		panic(name)
	}
	if registry == nil {
		registry = make(map[blobcache.SchemaName]schema.Constructor)
	}
	registry[name] = c
}

func MkSchema(spec blobcache.SchemaSpec) (schema.Schema, error) {
	if spec.Name == "" {
		return schema.None{}, nil
	}
	if c, exists := registry[spec.Name]; !exists {
		return nil, fmt.Errorf("unknown schema %q", spec.Name)
	} else {
		return c(spec.Params, MkSchema)
	}
}
