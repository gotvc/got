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
	// ProtocolGRPC is the gRPC based protocol in the gotgrpc package
	ProtocolGRPC = "grpc"
)

// URL points to a Got Space
type SpaceURL struct {
	Protocol    string
	Host        string
	SpacePrefix string
}

func NewNativeSpace(id gotnet.PeerID) SpaceURL {
	return SpaceURL{
		Protocol: ProtocolNative,
		Host:     id.String(),
	}
}

func NewQUICSpace(addr quichub.Addr) SpaceURL {
	return SpaceURL{
		Protocol: ProtocolQUIC,
		Host:     addr.String(),
	}
}

func NewGRPCSpace(endpoint string) SpaceURL {
	return SpaceURL{
		Protocol: ProtocolGRPC,
		Host:     endpoint,
	}
}

var protocolRegexp = regexp.MustCompile(`([A-z0-9]+):\/\/([^\/]+)(.*)`)

func ParseSpaceURL(x string) (*SpaceURL, error) {
	groups := protocolRegexp.FindStringSubmatch(x)
	if len(groups) < 2 {
		return nil, fmt.Errorf("url must start with protocol")
	}
	protocol := string(groups[1])
	hostport := groups[2]
	spacePrefix := strings.TrimLeft(groups[3], "/")
	return &SpaceURL{
		Protocol:    protocol,
		Host:        hostport,
		SpacePrefix: spacePrefix,
	}, nil
}

func (u SpaceURL) String() string {
	sb := strings.Builder{}
	sb.WriteString(u.Protocol)
	sb.WriteString("://")
	sb.WriteString(u.Host)
	if u.SpacePrefix != "" {
		sb.WriteString("/" + u.SpacePrefix)
	}
	return sb.String()
}
