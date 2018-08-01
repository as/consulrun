package serf

import (
	"encoding/base64"
	"fmt"
	"sync"
)

type KeyManager struct {
	serf *Serf

	l sync.RWMutex
}

type keyRequest struct {
	Key []byte
}

type KeyResponse struct {
	Messages map[string]string // Map of node name to response message
	NumNodes int               // Total nodes memberlist knows of
	NumResp  int               // Total responses received
	NumErr   int               // Total errors from request

	Keys map[string]int
}

type KeyRequestOptions struct {
	RelayFactor uint8
}

func (k *KeyManager) streamKeyResp(resp *KeyResponse, ch <-chan NodeResponse) {
	for r := range ch {
		var nodeResponse nodeKeyResponse

		resp.NumResp++

		if len(r.Payload) < 1 || messageType(r.Payload[0]) != messageKeyResponseType {
			resp.Messages[r.From] = fmt.Sprintf(
				"Invalid key query response type: %v", r.Payload)
			resp.NumErr++
			goto NEXT
		}
		if err := decodeMessage(r.Payload[1:], &nodeResponse); err != nil {
			resp.Messages[r.From] = fmt.Sprintf(
				"Failed to decode key query response: %v", r.Payload)
			resp.NumErr++
			goto NEXT
		}

		if !nodeResponse.Result {
			resp.Messages[r.From] = nodeResponse.Message
			resp.NumErr++
		}

		for _, key := range nodeResponse.Keys {
			if _, ok := resp.Keys[key]; !ok {
				resp.Keys[key] = 1
			} else {
				resp.Keys[key]++
			}
		}

	NEXT:

		if resp.NumResp == resp.NumNodes {
			return
		}
	}
}

func (k *KeyManager) handleKeyRequest(key, query string, opts *KeyRequestOptions) (*KeyResponse, error) {
	resp := &KeyResponse{
		Messages: make(map[string]string),
		Keys:     make(map[string]int),
	}
	qName := internalQueryName(query)

	rawKey, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return resp, err
	}

	req, err := encodeMessage(messageKeyRequestType, keyRequest{Key: rawKey})
	if err != nil {
		return resp, err
	}

	qParam := k.serf.DefaultQueryParams()
	if opts != nil {
		qParam.RelayFactor = opts.RelayFactor
	}
	queryResp, err := k.serf.Query(qName, req, qParam)
	if err != nil {
		return resp, err
	}

	resp.NumNodes = k.serf.memberlist.NumMembers()
	k.streamKeyResp(resp, queryResp.respCh)

	if resp.NumErr != 0 {
		return resp, fmt.Errorf("%d/%d nodes reported failure", resp.NumErr, resp.NumNodes)
	}
	if resp.NumResp != resp.NumNodes {
		return resp, fmt.Errorf("%d/%d nodes reported success", resp.NumResp, resp.NumNodes)
	}

	return resp, nil
}

func (k *KeyManager) InstallKey(key string) (*KeyResponse, error) {
	return k.InstallKeyWithOptions(key, nil)
}

func (k *KeyManager) InstallKeyWithOptions(key string, opts *KeyRequestOptions) (*KeyResponse, error) {
	k.l.Lock()
	defer k.l.Unlock()

	return k.handleKeyRequest(key, installKeyQuery, opts)
}

func (k *KeyManager) UseKey(key string) (*KeyResponse, error) {
	return k.UseKeyWithOptions(key, nil)
}

func (k *KeyManager) UseKeyWithOptions(key string, opts *KeyRequestOptions) (*KeyResponse, error) {
	k.l.Lock()
	defer k.l.Unlock()

	return k.handleKeyRequest(key, useKeyQuery, opts)
}

func (k *KeyManager) RemoveKey(key string) (*KeyResponse, error) {
	return k.RemoveKeyWithOptions(key, nil)
}

func (k *KeyManager) RemoveKeyWithOptions(key string, opts *KeyRequestOptions) (*KeyResponse, error) {
	k.l.Lock()
	defer k.l.Unlock()

	return k.handleKeyRequest(key, removeKeyQuery, opts)
}

func (k *KeyManager) ListKeys() (*KeyResponse, error) {
	return k.ListKeysWithOptions(nil)
}

func (k *KeyManager) ListKeysWithOptions(opts *KeyRequestOptions) (*KeyResponse, error) {
	k.l.RLock()
	defer k.l.RUnlock()

	return k.handleKeyRequest("", listKeysQuery, opts)
}
