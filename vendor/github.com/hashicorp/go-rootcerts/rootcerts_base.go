// +build !darwin

package rootcerts

import "crypto/x509"

func LoadSystemCAs() (*x509.CertPool, error) {
	return nil, nil
}
