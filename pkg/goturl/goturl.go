package goturl

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/gotvc/got/pkg/gotnet"
	"github.com/gotvc/got/pkg/gotnet/quichub"
)

const (
	// ProtocolNative is the native got protocol directly over INET256
	ProtocolNative = "got"
	// ProtocolQUIC is the got protocol over QUIC
	ProtocolQUIC = "quic"

	ProtocolGRPC = "grpc"
)

// URL points to a Got Space, Snapshot, or File
type URL struct {
	Protocol string
	Host     string
	Snap     *Snapish
	Path     string
}

func NewNativeSpace(id gotnet.PeerID) URL {
	return URL{
		Protocol: ProtocolNative,
		Host:     id.String(),
	}
}

func NewQUICSpace(addr quichub.Addr) URL {
	return URL{
		Protocol: ProtocolQUIC,
		Host:     addr.String(),
	}
}

func NewGRPCSpace(endpoint string) URL {
	return URL{
		Protocol: ProtocolGRPC,
		Host:     endpoint,
	}
}

var protocolRegexp = regexp.MustCompile(`([A-z0-9]+):\/\/(.*)`)

func ParseURL(x string) (*URL, error) {
	groups := protocolRegexp.FindStringSubmatch(x)
	if len(groups) < 2 {
		return nil, fmt.Errorf("url must start with protocol")
	}
	protocol := string(groups[1])
	hostport := groups[2]
	return &URL{
		Protocol: protocol,
		Host:     hostport,
	}, nil
}

func (u URL) IsSpace() bool {
	return !u.IsSnap() && !u.IsFile()
}

func (u URL) IsSnap() bool {
	return u.Snap != nil
}

func (u URL) IsFile() bool {
	return u.Path != "" && u.Snap != nil
}

func (u URL) String() string {
	sb := strings.Builder{}
	sb.WriteString(u.Protocol)
	sb.WriteString("://")
	sb.WriteString(u.Host)
	if u.Snap != nil {
		sb.WriteString("@")
		sb.WriteString(u.Snap.String())
	}
	if u.Path != "" {
		sb.WriteString(":")
		sb.WriteString(u.Path)
	}
	return sb.String()
}
