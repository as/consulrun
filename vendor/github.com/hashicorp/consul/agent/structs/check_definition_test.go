package structs

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/gofuzz"
	"github.com/as/consulrun/hashicorp/consul/api"
	"github.com/mitchellh/reflectwalk"
	"github.com/pascaldekloe/goe/verify"
)

func TestCheckDefinition_Defaults(t *testing.T) {
	t.Parallel()
	def := CheckDefinition{}
	check := def.HealthCheck("node1")

	if check.Status != api.HealthCritical {
		t.Fatalf("bad: %v", check.Status)
	}
}

type walker struct {
	fields map[string]reflect.Value
}

func (w *walker) Struct(reflect.Value) error {
	return nil
}

func (w *walker) StructField(f reflect.StructField, v reflect.Value) error {
	w.fields[f.Name] = v
	return nil
}

func mapFields(obj interface{}) map[string]reflect.Value {
	w := &walker{make(map[string]reflect.Value)}
	if err := reflectwalk.Walk(obj, w); err != nil {
		panic(err)
	}
	return w.fields
}

func TestCheckDefinition_CheckType(t *testing.T) {
	t.Parallel()

	var def CheckDefinition
	fuzz.New().Fuzz(&def)
	orig := mapFields(def)

	orig["CheckID"] = orig["ID"]
	delete(orig, "ID")
	delete(orig, "ServiceID")
	delete(orig, "Token")

	chk := def.CheckType()
	copy := mapFields(chk)
	for f, vo := range orig {
		vc, ok := copy[f]
		if !ok {
			t.Fatalf("struct is missing field %q", f)
		}

		if !reflect.DeepEqual(vo.Interface(), vc.Interface()) {
			t.Fatalf("copy skipped field %q", f)
		}
	}
}

func TestCheckDefinitionToCheckType(t *testing.T) {
	t.Parallel()
	got := &CheckDefinition{
		ID:     "id",
		Name:   "name",
		Status: "green",
		Notes:  "notes",

		ServiceID:         "svcid",
		Token:             "tok",
		Script:            "/bin/foo",
		HTTP:              "someurl",
		TCP:               "host:port",
		Interval:          1 * time.Second,
		DockerContainerID: "abc123",
		Shell:             "/bin/ksh",
		TLSSkipVerify:     true,
		Timeout:           2 * time.Second,
		TTL:               3 * time.Second,
		DeregisterCriticalServiceAfter: 4 * time.Second,
	}
	want := &CheckType{
		CheckID: "id",
		Name:    "name",
		Status:  "green",
		Notes:   "notes",

		Script:            "/bin/foo",
		HTTP:              "someurl",
		TCP:               "host:port",
		Interval:          1 * time.Second,
		DockerContainerID: "abc123",
		Shell:             "/bin/ksh",
		TLSSkipVerify:     true,
		Timeout:           2 * time.Second,
		TTL:               3 * time.Second,
		DeregisterCriticalServiceAfter: 4 * time.Second,
	}
	verify.Values(t, "", got.CheckType(), want)
}
