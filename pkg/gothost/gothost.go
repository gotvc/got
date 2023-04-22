// Package gothost provides tools for configuring access permissions on the host.
package gothost

import (
	"github.com/inet256/inet256/pkg/inet256"
)

const (
	// HostConfigKey is the branch key for the host config
	HostConfigKey = "__host__"

	IdentitiesPath = "IDENTITIES"
	PolicyPath     = "POLICY"
)

type PeerID = inet256.Addr
