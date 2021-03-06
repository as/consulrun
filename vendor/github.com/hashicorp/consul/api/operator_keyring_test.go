package api

import (
	"testing"

	"github.com/as/consulrun/hashicorp/consul/testutil"
)

func TestAPI_OperatorKeyringInstallListPutRemove(t *testing.T) {
	t.Parallel()
	oldKey := "d8wu8CSUrqgtjVsvcBPmhQ=="
	newKey := "qxycTi/SsePj/TZzCBmNXw=="
	c, s := makeClientWithConfig(t, nil, func(c *testutil.TestServerConfig) {
		c.Encrypt = oldKey
	})
	defer s.Stop()

	operator := c.Operator()
	if err := operator.KeyringInstall(newKey, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	listResponses, err := operator.KeyringList(nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}

	if len(listResponses) != 2 {
		t.Fatalf("bad: %v", len(listResponses))
	}
	for _, response := range listResponses {
		if len(response.Keys) != 2 {
			t.Fatalf("bad: %v", len(response.Keys))
		}
		if _, ok := response.Keys[oldKey]; !ok {
			t.Fatalf("bad: %v", ok)
		}
		if _, ok := response.Keys[newKey]; !ok {
			t.Fatalf("bad: %v", ok)
		}
	}

	if err := operator.KeyringUse(newKey, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	if err := operator.KeyringRemove(oldKey, nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	listResponses, err = operator.KeyringList(nil)
	if err != nil {
		t.Fatalf("err %v", err)
	}

	if len(listResponses) != 2 {
		t.Fatalf("bad: %v", len(listResponses))
	}
	for _, response := range listResponses {
		if len(response.Keys) != 1 {
			t.Fatalf("bad: %v", len(response.Keys))
		}
		if _, ok := response.Keys[oldKey]; ok {
			t.Fatalf("bad: %v", ok)
		}
		if _, ok := response.Keys[newKey]; !ok {
			t.Fatalf("bad: %v", ok)
		}
	}
}
