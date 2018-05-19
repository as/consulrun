package memberlist

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

/*

Encrypted messages are prefixed with an encryptionVersion byte
that is used for us to be able to properly encode/decode. We
currently support the following versions:

 0 - AES-GCM 128, using PKCS7 padding
 1 - AES-GCM 128, no padding. Padding not needed, caused bloat.

*/
type encryptionVersion uint8

const (
	minEncryptionVersion encryptionVersion = 0
	maxEncryptionVersion encryptionVersion = 1
)

const (
	versionSize    = 1
	nonceSize      = 12
	tagSize        = 16
	maxPadOverhead = 16
	blockSize      = aes.BlockSize
)

func pkcs7encode(buf *bytes.Buffer, ignore, blockSize int) {
	n := buf.Len() - ignore
	more := blockSize - (n % blockSize)
	for i := 0; i < more; i++ {
		buf.WriteByte(byte(more))
	}
}

func pkcs7decode(buf []byte, blockSize int) []byte {
	if len(buf) == 0 {
		panic("Cannot decode a PKCS7 buffer of zero length")
	}
	n := len(buf)
	last := buf[n-1]
	n -= int(last)
	return buf[:n]
}

func encryptOverhead(vsn encryptionVersion) int {
	switch vsn {
	case 0:
		return 45 // Version: 1, IV: 12, Padding: 16, Tag: 16
	case 1:
		return 29 // Version: 1, IV: 12, Tag: 16
	default:
		panic("unsupported version")
	}
}

func encryptedLength(vsn encryptionVersion, inp int) int {

	if vsn >= 1 {
		return versionSize + nonceSize + inp + tagSize
	}

	padding := blockSize - (inp % blockSize)

	return versionSize + nonceSize + inp + padding + tagSize
}

func encryptPayload(vsn encryptionVersion, key []byte, msg []byte, data []byte, dst *bytes.Buffer) error {

	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return err
	}

	gcm, err := cipher.NewGCM(aesBlock)
	if err != nil {
		return err
	}

	offset := dst.Len()
	dst.Grow(encryptedLength(vsn, len(msg)))

	dst.WriteByte(byte(vsn))

	io.CopyN(dst, rand.Reader, nonceSize)
	afterNonce := dst.Len()

	if vsn == 0 {
		io.Copy(dst, bytes.NewReader(msg))
		pkcs7encode(dst, offset+versionSize+nonceSize, aes.BlockSize)
	}

	slice := dst.Bytes()[offset:]
	nonce := slice[versionSize : versionSize+nonceSize]

	var src []byte
	if vsn == 0 {
		src = slice[versionSize+nonceSize:]
	} else {
		src = msg
	}
	out := gcm.Seal(nil, nonce, src, data)

	dst.Truncate(afterNonce)
	dst.Write(out)
	return nil
}

func decryptMessage(key, msg []byte, data []byte) ([]byte, error) {

	aesBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(aesBlock)
	if err != nil {
		return nil, err
	}

	nonce := msg[versionSize : versionSize+nonceSize]
	ciphertext := msg[versionSize+nonceSize:]
	plain, err := gcm.Open(nil, nonce, ciphertext, data)
	if err != nil {
		return nil, err
	}

	return plain, nil
}

func decryptPayload(keys [][]byte, msg []byte, data []byte) ([]byte, error) {

	if len(msg) == 0 {
		return nil, fmt.Errorf("Cannot decrypt empty payload")
	}

	vsn := encryptionVersion(msg[0])
	if vsn > maxEncryptionVersion {
		return nil, fmt.Errorf("Unsupported encryption version %d", msg[0])
	}

	if len(msg) < encryptedLength(vsn, 0) {
		return nil, fmt.Errorf("Payload is too small to decrypt: %d", len(msg))
	}

	for _, key := range keys {
		plain, err := decryptMessage(key, msg, data)
		if err == nil {

			if vsn == 0 {
				return pkcs7decode(plain, aes.BlockSize), nil
			} else {
				return plain, nil
			}
		}
	}

	return nil, fmt.Errorf("No installed keys could decrypt the message")
}
