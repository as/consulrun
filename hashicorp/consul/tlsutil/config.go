package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/go-rootcerts"
)

type DCWrapper func(dc string, conn net.Conn) (net.Conn, error)

type Wrapper func(conn net.Conn) (net.Conn, error)

var TLSLookup = map[string]uint16{
	"tls10": tls.VersionTLS10,
	"tls11": tls.VersionTLS11,
	"tls12": tls.VersionTLS12,
}

type Config struct {
	VerifyIncoming bool

	VerifyOutgoing bool

	VerifyServerHostname bool

	UseTLS bool

	CAFile string

	CAPath string

	CertFile string

	KeyFile string

	NodeName string

	ServerName string

	Domain string

	TLSMinVersion string

	CipherSuites []uint16

	PreferServerCipherSuites bool
}

func (c *Config) AppendCA(pool *x509.CertPool) error {
	if c.CAFile == "" {
		return nil
	}

	data, err := ioutil.ReadFile(c.CAFile)
	if err != nil {
		return fmt.Errorf("Failed to read CA file: %v", err)
	}

	if !pool.AppendCertsFromPEM(data) {
		return fmt.Errorf("Failed to parse any CA certificates")
	}

	return nil
}

func (c *Config) KeyPair() (*tls.Certificate, error) {
	if c.CertFile == "" || c.KeyFile == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("Failed to load cert/key pair: %v", err)
	}
	return &cert, err
}

func (c *Config) OutgoingTLSConfig() (*tls.Config, error) {

	if c.VerifyServerHostname {
		c.VerifyOutgoing = true
	}
	if !c.UseTLS && !c.VerifyOutgoing {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		RootCAs:            x509.NewCertPool(),
		InsecureSkipVerify: true,
	}
	if c.ServerName != "" {
		tlsConfig.ServerName = c.ServerName
		tlsConfig.InsecureSkipVerify = false
	}
	if c.VerifyServerHostname {

		tlsConfig.ServerName = "VerifyServerHostname"
		tlsConfig.InsecureSkipVerify = false
	}
	if len(c.CipherSuites) != 0 {
		tlsConfig.CipherSuites = c.CipherSuites
	}
	if c.PreferServerCipherSuites {
		tlsConfig.PreferServerCipherSuites = true
	}

	if c.VerifyOutgoing && c.CAFile == "" && c.CAPath == "" {
		return nil, fmt.Errorf("VerifyOutgoing set, and no CA certificate provided!")
	}

	rootConfig := &rootcerts.Config{
		CAFile: c.CAFile,
		CAPath: c.CAPath,
	}
	if err := rootcerts.ConfigureTLS(tlsConfig, rootConfig); err != nil {
		return nil, err
	}

	cert, err := c.KeyPair()
	if err != nil {
		return nil, err
	} else if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	if c.TLSMinVersion != "" {
		tlsvers, ok := TLSLookup[c.TLSMinVersion]
		if !ok {
			return nil, fmt.Errorf("TLSMinVersion: value %s not supported, please specify one of [tls10,tls11,tls12]", c.TLSMinVersion)
		}
		tlsConfig.MinVersion = tlsvers
	}

	return tlsConfig, nil
}

func (c *Config) OutgoingTLSWrapper() (DCWrapper, error) {

	tlsConfig, err := c.OutgoingTLSConfig()
	if err != nil {
		return nil, err
	}

	if tlsConfig == nil {
		return nil, nil
	}

	domain := strings.TrimSuffix(c.Domain, ".")

	wrapper := func(dc string, c net.Conn) (net.Conn, error) {
		return WrapTLSClient(c, tlsConfig)
	}

	if c.VerifyServerHostname {
		wrapper = func(dc string, conn net.Conn) (net.Conn, error) {
			conf := tlsConfig.Clone()
			conf.ServerName = "server." + dc + "." + domain
			return WrapTLSClient(conn, conf)
		}
	}

	return wrapper, nil
}

func SpecificDC(dc string, tlsWrap DCWrapper) Wrapper {
	if tlsWrap == nil {
		return nil
	}
	return func(conn net.Conn) (net.Conn, error) {
		return tlsWrap(dc, conn)
	}
}

//
func WrapTLSClient(conn net.Conn, tlsConfig *tls.Config) (net.Conn, error) {
	var err error
	var tlsConn *tls.Conn

	tlsConn = tls.Client(conn, tlsConfig)

	if tlsConfig.InsecureSkipVerify == false {
		return tlsConn, nil
	}

	if err = tlsConn.Handshake(); err != nil {
		tlsConn.Close()
		return nil, err
	}

	opts := x509.VerifyOptions{
		Roots:         tlsConfig.RootCAs,
		CurrentTime:   time.Now(),
		DNSName:       "",
		Intermediates: x509.NewCertPool(),
	}

	certs := tlsConn.ConnectionState().PeerCertificates
	for i, cert := range certs {
		if i == 0 {
			continue
		}
		opts.Intermediates.AddCert(cert)
	}

	_, err = certs[0].Verify(opts)
	if err != nil {
		tlsConn.Close()
		return nil, err
	}

	return tlsConn, err
}

func (c *Config) IncomingTLSConfig() (*tls.Config, error) {

	tlsConfig := &tls.Config{
		ServerName: c.ServerName,
		ClientCAs:  x509.NewCertPool(),
		ClientAuth: tls.NoClientCert,
	}
	if tlsConfig.ServerName == "" {
		tlsConfig.ServerName = c.NodeName
	}

	if len(c.CipherSuites) != 0 {
		tlsConfig.CipherSuites = c.CipherSuites
	}
	if c.PreferServerCipherSuites {
		tlsConfig.PreferServerCipherSuites = true
	}

	if c.CAFile != "" {
		pool, err := rootcerts.LoadCAFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.ClientCAs = pool
	} else if c.CAPath != "" {
		pool, err := rootcerts.LoadCAPath(c.CAPath)
		if err != nil {
			return nil, err
		}
		tlsConfig.ClientCAs = pool
	}

	cert, err := c.KeyPair()
	if err != nil {
		return nil, err
	} else if cert != nil {
		tlsConfig.Certificates = []tls.Certificate{*cert}
	}

	if c.VerifyIncoming {
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		if c.CAFile == "" && c.CAPath == "" {
			return nil, fmt.Errorf("VerifyIncoming set, and no CA certificate provided!")
		}
		if cert == nil {
			return nil, fmt.Errorf("VerifyIncoming set, and no Cert/Key pair provided!")
		}
	}

	if c.TLSMinVersion != "" {
		tlsvers, ok := TLSLookup[c.TLSMinVersion]
		if !ok {
			return nil, fmt.Errorf("TLSMinVersion: value %s not supported, please specify one of [tls10,tls11,tls12]", c.TLSMinVersion)
		}
		tlsConfig.MinVersion = tlsvers
	}
	return tlsConfig, nil
}

func ParseCiphers(cipherStr string) ([]uint16, error) {
	suites := []uint16{}

	cipherStr = strings.TrimSpace(cipherStr)
	if cipherStr == "" {
		return []uint16{}, nil
	}
	ciphers := strings.Split(cipherStr, ",")

	cipherMap := map[string]uint16{
		"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305":    tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305":  tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
		"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":   tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384": tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256":   tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,
		"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA":      tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256": tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
		"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
		"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA":      tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
		"TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA":    tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
		"TLS_RSA_WITH_AES_128_GCM_SHA256":         tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
		"TLS_RSA_WITH_AES_256_GCM_SHA384":         tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
		"TLS_RSA_WITH_AES_128_CBC_SHA256":         tls.TLS_RSA_WITH_AES_128_CBC_SHA256,
		"TLS_RSA_WITH_AES_128_CBC_SHA":            tls.TLS_RSA_WITH_AES_128_CBC_SHA,
		"TLS_RSA_WITH_AES_256_CBC_SHA":            tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA":     tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
		"TLS_RSA_WITH_3DES_EDE_CBC_SHA":           tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
		"TLS_RSA_WITH_RC4_128_SHA":                tls.TLS_RSA_WITH_RC4_128_SHA,
		"TLS_ECDHE_RSA_WITH_RC4_128_SHA":          tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA,
		"TLS_ECDHE_ECDSA_WITH_RC4_128_SHA":        tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA,
	}
	for _, cipher := range ciphers {
		if v, ok := cipherMap[cipher]; ok {
			suites = append(suites, v)
		} else {
			return suites, fmt.Errorf("unsupported cipher %q", cipher)
		}
	}

	return suites, nil
}
