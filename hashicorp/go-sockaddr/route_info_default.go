// +build android nacl plan9

package sockaddr

import "errors"

func getDefaultIfName() (string, error) {
	return "", errors.New("No default interface found (unsupported platform)")
}
