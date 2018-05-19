package rootcerts

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Config struct {
	CAFile string

	CAPath string
}

func ConfigureTLS(t *tls.Config, c *Config) error {
	if t == nil {
		return nil
	}
	pool, err := LoadCACerts(c)
	if err != nil {
		return err
	}
	t.RootCAs = pool
	return nil
}

func LoadCACerts(c *Config) (*x509.CertPool, error) {
	if c == nil {
		c = &Config{}
	}
	if c.CAFile != "" {
		return LoadCAFile(c.CAFile)
	}
	if c.CAPath != "" {
		return LoadCAPath(c.CAPath)
	}

	return LoadSystemCAs()
}

func LoadCAFile(caFile string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()

	pem, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("Error loading CA File: %s", err)
	}

	ok := pool.AppendCertsFromPEM(pem)
	if !ok {
		return nil, fmt.Errorf("Error loading CA File: Couldn't parse PEM in: %s", caFile)
	}

	return pool, nil
}

func LoadCAPath(caPath string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	walkFn := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		pem, err := ioutil.ReadFile(path)
		if err != nil {
			return fmt.Errorf("Error loading file from CAPath: %s", err)
		}

		ok := pool.AppendCertsFromPEM(pem)
		if !ok {
			return fmt.Errorf("Error loading CA Path: Couldn't parse PEM in: %s", path)
		}

		return nil
	}

	err := filepath.Walk(caPath, walkFn)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
