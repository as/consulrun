package dns

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
)

func (r *SMIMEA) Sign(usage, selector, matchingType int, cert *x509.Certificate) (err error) {
	r.Hdr.Rrtype = TypeSMIMEA
	r.Usage = uint8(usage)
	r.Selector = uint8(selector)
	r.MatchingType = uint8(matchingType)

	r.Certificate, err = CertificateToDANE(r.Selector, r.MatchingType, cert)
	if err != nil {
		return err
	}
	return nil
}

func (r *SMIMEA) Verify(cert *x509.Certificate) error {
	c, err := CertificateToDANE(r.Selector, r.MatchingType, cert)
	if err != nil {
		return err // Not also ErrSig?
	}
	if r.Certificate == c {
		return nil
	}
	return ErrSig // ErrSig, really?
}

func SMIMEAName(email, domain string) (string, error) {
	hasher := sha256.New()
	hasher.Write([]byte(email))

	return hex.EncodeToString(hasher.Sum(nil)[:28]) + "." + "_smimecert." + domain, nil
}
