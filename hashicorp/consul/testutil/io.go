package testutil

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

//
//
var tmpdir = "/tmp/consul-test"

func init() {
	if err := os.MkdirAll(tmpdir, 0755); err != nil {
		fmt.Printf("Cannot create %s. Reverting to /tmp\n", tmpdir)
		tmpdir = "/tmp"
	}
}

func TempDir(t *testing.T, name string) string {
	if t != nil && t.Name() != "" {
		name = t.Name() + "-" + name
	}
	name = strings.Replace(name, "/", "_", -1)
	d, err := ioutil.TempDir(tmpdir, name)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	return d
}

func TempFile(t *testing.T, name string) *os.File {
	if t != nil && t.Name() != "" {
		name = t.Name() + "-" + name
	}
	f, err := ioutil.TempFile(tmpdir, name)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	return f
}
