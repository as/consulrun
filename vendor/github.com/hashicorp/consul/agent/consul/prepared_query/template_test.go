package prepared_query

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/types"
	"github.com/mitchellh/copystructure"
)

var (
	bigBench = &structs.PreparedQuery{
		Name: "hello",
		Template: structs.QueryTemplateOptions{
			Type:            structs.QueryTemplateTypeNamePrefixMatch,
			Regexp:          "^hello-(.*)-(.*)$",
			RemoveEmptyTags: true,
		},
		Service: structs.ServiceQuery{
			Service: "${name.full}",
			Failover: structs.QueryDatacenterOptions{
				Datacenters: []string{
					"${name.full}",
					"${name.prefix}",
					"${name.suffix}",
					"${match(0)}",
					"${match(1)}",
					"${match(2)}",
					"${agent.segment}",
				},
			},
			IgnoreCheckIDs: []types.CheckID{
				"${name.full}",
				"${name.prefix}",
				"${name.suffix}",
				"${match(0)}",
				"${match(1)}",
				"${match(2)}",
				"${agent.segment}",
			},
			Tags: []string{
				"${name.full}",
				"${name.prefix}",
				"${name.suffix}",
				"${match(0)}",
				"${match(1)}",
				"${match(2)}",
				"${agent.segment}",
			},
			NodeMeta: map[string]string{
				"foo": "${name.prefix}",
				"bar": "${match(0)}",
				"baz": "${match(1)}",
				"zoo": "${agent.segment}",
			},
		},
	}

	smallBench = &structs.PreparedQuery{
		Name: "",
		Template: structs.QueryTemplateOptions{
			Type: structs.QueryTemplateTypeNamePrefixMatch,
		},
		Service: structs.ServiceQuery{
			Service: "${name.full}",
			Failover: structs.QueryDatacenterOptions{
				Datacenters: []string{
					"dc1",
					"dc2",
					"dc3",
				},
			},
		},
	}
)

func compileBench(b *testing.B, query *structs.PreparedQuery) {
	for i := 0; i < b.N; i++ {
		_, err := Compile(query)
		if err != nil {
			b.Fatalf("err: %v", err)
		}
	}
}

func renderBench(b *testing.B, query *structs.PreparedQuery) {
	compiled, err := Compile(query)
	if err != nil {
		b.Fatalf("err: %v", err)
	}

	for i := 0; i < b.N; i++ {
		_, err := compiled.Render("hello-bench-mark", structs.QuerySource{})
		if err != nil {
			b.Fatalf("err: %v", err)
		}
	}
}

func BenchmarkTemplate_CompileSmall(b *testing.B) {
	compileBench(b, smallBench)
}

func BenchmarkTemplate_CompileBig(b *testing.B) {
	compileBench(b, bigBench)
}

func BenchmarkTemplate_RenderSmall(b *testing.B) {
	renderBench(b, smallBench)
}

func BenchmarkTemplate_RenderBig(b *testing.B) {
	renderBench(b, bigBench)
}

func TestTemplate_Compile(t *testing.T) {

	query := &structs.PreparedQuery{}
	_, err := Compile(query)
	if err == nil || !strings.Contains(err.Error(), "Bad Template") {
		t.Fatalf("bad: %v", err)
	}
	if IsTemplate(query) {
		t.Fatalf("should not be a template")
	}

	query.Template.Type = structs.QueryTemplateTypeNamePrefixMatch
	query.Template.Regexp = "^(hello)there$"
	query.Service.Service = "${name.full}"
	query.Service.IgnoreCheckIDs = []types.CheckID{"${match(1)}", "${agent.segment}"}
	query.Service.Tags = []string{"${match(1)}", "${agent.segment}"}
	backup, err := copystructure.Copy(query)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	ct, err := Compile(query)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !IsTemplate(query) {
		t.Fatalf("should be a template")
	}

	actual, err := ct.Render("hellothere", structs.QuerySource{Segment: "segment-foo"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	expected := &structs.PreparedQuery{
		Template: structs.QueryTemplateOptions{
			Type:   structs.QueryTemplateTypeNamePrefixMatch,
			Regexp: "^(hello)there$",
		},
		Service: structs.ServiceQuery{
			Service: "hellothere",
			IgnoreCheckIDs: []types.CheckID{
				"hello",
				"segment-foo",
			},
			Tags: []string{
				"hello",
				"segment-foo",
			},
		},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("bad: %#v", actual)
	}

	if !reflect.DeepEqual(query, backup.(*structs.PreparedQuery)) {
		t.Fatalf("bad: %#v", query)
	}

	query.Service.Service = "${name.full"
	_, err = Compile(query)
	if err == nil || !strings.Contains(err.Error(), "Bad format") {
		t.Fatalf("bad: %v", err)
	}

	query.Service.Service = "${name.nope}"
	_, err = Compile(query)
	if err == nil || !strings.Contains(err.Error(), "unknown variable") {
		t.Fatalf("bad: %v", err)
	}

	query.Template.Regexp = "^(nope$"
	query.Service.Service = "${name.full}"
	_, err = Compile(query)
	if err == nil || !strings.Contains(err.Error(), "Bad Regexp") {
		t.Fatalf("bad: %v", err)
	}
}

func TestTemplate_Render(t *testing.T) {

	{
		query := &structs.PreparedQuery{
			Template: structs.QueryTemplateOptions{
				Type: structs.QueryTemplateTypeNamePrefixMatch,
			},
			Service: structs.ServiceQuery{
				Service: "hellothere",
			},
		}
		ct, err := Compile(query)
		if err != nil {
			t.Fatalf("err: %v", err)
		}

		actual, err := ct.Render("unused", structs.QuerySource{})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if !reflect.DeepEqual(actual, query) {
			t.Fatalf("bad: %#v", actual)
		}
	}

	query := &structs.PreparedQuery{
		Name: "hello-",
		Template: structs.QueryTemplateOptions{
			Type:   structs.QueryTemplateTypeNamePrefixMatch,
			Regexp: "^(.*?)-(.*?)-(.*)$",
		},
		Service: structs.ServiceQuery{
			Service: "${name.prefix} xxx ${name.full} xxx ${name.suffix} xxx ${agent.segment}",
			Tags: []string{
				"${match(-1)}",
				"${match(0)}",
				"${match(1)}",
				"${match(2)}",
				"${match(3)}",
				"${match(4)}",
				"${40 + 2}",
			},
			NodeMeta: map[string]string{"foo": "${match(1)}"},
		},
	}
	ct, err := Compile(query)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		actual, err := ct.Render("hello-foo-bar-none", structs.QuerySource{Segment: "segment-bar"})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		expected := &structs.PreparedQuery{
			Name: "hello-",
			Template: structs.QueryTemplateOptions{
				Type:   structs.QueryTemplateTypeNamePrefixMatch,
				Regexp: "^(.*?)-(.*?)-(.*)$",
			},
			Service: structs.ServiceQuery{
				Service: "hello- xxx hello-foo-bar-none xxx foo-bar-none xxx segment-bar",
				Tags: []string{
					"",
					"hello-foo-bar-none",
					"hello",
					"foo",
					"bar-none",
					"",
					"42",
				},
				NodeMeta: map[string]string{"foo": "hello"},
			},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("bad: %#v", actual)
		}
	}

	{
		actual, err := ct.Render("hello-nope", structs.QuerySource{Segment: "segment-bar"})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		expected := &structs.PreparedQuery{
			Name: "hello-",
			Template: structs.QueryTemplateOptions{
				Type:   structs.QueryTemplateTypeNamePrefixMatch,
				Regexp: "^(.*?)-(.*?)-(.*)$",
			},
			Service: structs.ServiceQuery{
				Service: "hello- xxx hello-nope xxx nope xxx segment-bar",
				Tags: []string{
					"",
					"",
					"",
					"",
					"",
					"",
					"42",
				},
				NodeMeta: map[string]string{"foo": ""},
			},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("bad: %#v", actual)
		}
	}

	query = &structs.PreparedQuery{
		Name: "hello-",
		Template: structs.QueryTemplateOptions{
			Type:            structs.QueryTemplateTypeNamePrefixMatch,
			Regexp:          "^(.*?)-(.*?)-(.*)$",
			RemoveEmptyTags: true,
		},
		Service: structs.ServiceQuery{
			Service: "${name.prefix} xxx ${name.full} xxx ${name.suffix} xxx ${agent.segment}",
			Tags: []string{
				"${match(-1)}",
				"${match(0)}",
				"${match(1)}",
				"${match(2)}",
				"${match(3)}",
				"${match(4)}",
				"${40 + 2}",
			},
		},
	}
	ct, err = Compile(query)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	{
		actual, err := ct.Render("hello-foo-bar-none", structs.QuerySource{Segment: "segment-baz"})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		expected := &structs.PreparedQuery{
			Name: "hello-",
			Template: structs.QueryTemplateOptions{
				Type:            structs.QueryTemplateTypeNamePrefixMatch,
				Regexp:          "^(.*?)-(.*?)-(.*)$",
				RemoveEmptyTags: true,
			},
			Service: structs.ServiceQuery{
				Service: "hello- xxx hello-foo-bar-none xxx foo-bar-none xxx segment-baz",
				Tags: []string{
					"hello-foo-bar-none",
					"hello",
					"foo",
					"bar-none",
					"42",
				},
			},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("bad:\n%#v\nexpected:\n%#v\n", actual, expected)
		}
	}

	{
		actual, err := ct.Render("hello-nope", structs.QuerySource{Segment: "segment-baz"})
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		expected := &structs.PreparedQuery{
			Name: "hello-",
			Template: structs.QueryTemplateOptions{
				Type:            structs.QueryTemplateTypeNamePrefixMatch,
				Regexp:          "^(.*?)-(.*?)-(.*)$",
				RemoveEmptyTags: true,
			},
			Service: structs.ServiceQuery{
				Service: "hello- xxx hello-nope xxx nope xxx segment-baz",
				Tags: []string{
					"42",
				},
			},
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("bad: %#v", actual)
		}
	}
}
