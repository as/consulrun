package memberlist

import (
	"bytes"
	"fmt"
	"sync"
)

type Keyring struct {
	keys [][]byte

	l sync.Mutex
}

func (k *Keyring) init() {
	k.keys = make([][]byte, 0)
}

//
//
//
func NewKeyring(keys [][]byte, primaryKey []byte) (*Keyring, error) {
	keyring := &Keyring{}
	keyring.init()

	if len(keys) > 0 || len(primaryKey) > 0 {
		if len(primaryKey) == 0 {
			return nil, fmt.Errorf("Empty primary key not allowed")
		}
		if err := keyring.AddKey(primaryKey); err != nil {
			return nil, err
		}
		for _, key := range keys {
			if err := keyring.AddKey(key); err != nil {
				return nil, err
			}
		}
	}

	return keyring, nil
}

//
func ValidateKey(key []byte) error {
	if l := len(key); l != 16 && l != 24 && l != 32 {
		return fmt.Errorf("key size must be 16, 24 or 32 bytes")
	}
	return nil
}

//
func (k *Keyring) AddKey(key []byte) error {
	if err := ValidateKey(key); err != nil {
		return err
	}

	for _, installedKey := range k.keys {
		if bytes.Equal(installedKey, key) {
			return nil
		}
	}

	keys := append(k.keys, key)
	primaryKey := k.GetPrimaryKey()
	if primaryKey == nil {
		primaryKey = key
	}
	k.installKeys(keys, primaryKey)
	return nil
}

func (k *Keyring) UseKey(key []byte) error {
	for _, installedKey := range k.keys {
		if bytes.Equal(key, installedKey) {
			k.installKeys(k.keys, key)
			return nil
		}
	}
	return fmt.Errorf("Requested key is not in the keyring")
}

func (k *Keyring) RemoveKey(key []byte) error {
	if bytes.Equal(key, k.keys[0]) {
		return fmt.Errorf("Removing the primary key is not allowed")
	}
	for i, installedKey := range k.keys {
		if bytes.Equal(key, installedKey) {
			keys := append(k.keys[:i], k.keys[i+1:]...)
			k.installKeys(keys, k.keys[0])
		}
	}
	return nil
}

func (k *Keyring) installKeys(keys [][]byte, primaryKey []byte) {
	k.l.Lock()
	defer k.l.Unlock()

	newKeys := [][]byte{primaryKey}
	for _, key := range keys {
		if !bytes.Equal(key, primaryKey) {
			newKeys = append(newKeys, key)
		}
	}
	k.keys = newKeys
}

func (k *Keyring) GetKeys() [][]byte {
	k.l.Lock()
	defer k.l.Unlock()

	return k.keys
}

func (k *Keyring) GetPrimaryKey() (key []byte) {
	k.l.Lock()
	defer k.l.Unlock()

	if len(k.keys) > 0 {
		key = k.keys[0]
	}
	return
}
