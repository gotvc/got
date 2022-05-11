package gotgrpc

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/brendoncarroll/go-p2p"
	"github.com/brendoncarroll/go-p2p/s/swarmutil"
	"github.com/inet256/inet256/pkg/inet256"
	"google.golang.org/grpc/credentials"
)

type TransportCreds struct {
	privateKey   inet256.PrivateKey
	serverConfig *tls.Config
}

func NewClientCreds(privateKey inet256.PrivateKey) credentials.TransportCredentials {
	return TransportCreds{privateKey: privateKey}
}

// ClientHandshake does the authentication handshake specified by the
// corresponding authentication protocol on rawConn for clients. It returns
// the authenticated connection and the corresponding auth information
// about the connection.  The auth information should embed CommonAuthInfo
// to return additional information about the credentials. Implementations
// must use the provided context to implement timely cancellation.  gRPC
// will try to reconnect if the error returned is a temporary error
// (io.EOF, context.DeadlineExceeded or err.Temporary() == true).  If the
// returned error is a wrapper error, implementations should make sure that
// the error implements Temporary() to have the correct retry behaviors.
// Additionally, ClientHandshakeInfo data will be available via the context
// passed to this call.
//
// If the returned net.Conn is closed, it MUST close the net.Conn provided.
func (tc TransportCreds) ClientHandshake(ctx context.Context, endpoint string, x net.Conn) (net.Conn, credentials.AuthInfo, error) {
	config := generateClientTLS(tc.privateKey)
	config.ServerName = endpoint
	tconn := tls.Client(x, config)
	if err := tconn.HandshakeContext(ctx); err != nil {
		return nil, nil, err
	}
	id := inet256.NewAddr(tconn.ConnectionState().PeerCertificates[0].PublicKey)
	return tconn, AuthInfo{ID: id}, nil
}

// ServerHandshake does the authentication handshake for servers. It returns
// the authenticated connection and the corresponding auth information about
// the connection. The auth information should embed CommonAuthInfo to return additional information
// about the credentials.
//
// If the returned net.Conn is closed, it MUST close the net.Conn provided.
func (tc TransportCreds) ServerHandshake(x net.Conn) (net.Conn, credentials.AuthInfo, error) {
	tconn := tls.Server(x, tc.serverConfig)
	if err := tconn.Handshake(); err != nil {
		return nil, nil, err
	}
	id := inet256.NewAddr(tconn.ConnectionState().PeerCertificates[0].PublicKey)
	authInfo := newAuthInfo(id)
	return tconn, authInfo, nil
}

// Info provides the ProtocolInfo of this TransportCredentials.
func (tc TransportCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{}
}

// Clone makes a copy of this TransportCredentials.
func (tc TransportCreds) Clone() credentials.TransportCredentials {
	return tc
}

// OverrideServerName overrides the server name used to verify the hostname on the returned certificates from the server.
// gRPC internals also use it to override the virtual hosting name if it is set.
// It must be called before dialing. Currently, this is only used by grpclb.
func (tc TransportCreds) OverrideServerName(string) error {
	return nil
}

type AuthInfo struct {
	ID inet256.ID

	credentials.CommonAuthInfo
}

func newAuthInfo(id inet256.ID) AuthInfo {
	return AuthInfo{
		ID: id,
		CommonAuthInfo: credentials.CommonAuthInfo{
			SecurityLevel: credentials.PrivacyAndIntegrity,
		},
	}
}

func (ai AuthInfo) AuthType() string {
	return "INET256"
}

func generateClientTLS(privKey p2p.PrivateKey) *tls.Config {
	cert := swarmutil.GenerateSelfSigned(privKey)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAnyClientCert,
	}
}
