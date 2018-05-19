//+build ignore

package main

import (
	"bytes"
	"fmt"
	"go/format"
	"go/importer"
	"go/types"
	"log"
	"os"
)

var packageHdr = `

package dns

`

func getTypeStruct(t types.Type, scope *types.Scope) (*types.Struct, bool) {
	st, ok := t.Underlying().(*types.Struct)
	if !ok {
		return nil, false
	}
	if st.Field(0).Type() == scope.Lookup("RR_Header").Type() {
		return st, false
	}
	if st.Field(0).Anonymous() {
		st, _ := getTypeStruct(st.Field(0).Type(), scope)
		return st, true
	}
	return nil, false
}

func main() {

	pkg, err := importer.Default().Import("github.com/miekg/dns")
	fatalIfErr(err)
	scope := pkg.Scope()

	var domainTypes []string  // Types that have a domain name in them (either compressible or not).
	var cdomainTypes []string // Types that have a compressible domain name in them (subset of domainType)
Names:
	for _, name := range scope.Names() {
		o := scope.Lookup(name)
		if o == nil || !o.Exported() {
			continue
		}
		st, _ := getTypeStruct(o.Type(), scope)
		if st == nil {
			continue
		}
		if name == "PrivateRR" {
			continue
		}

		if scope.Lookup("Type"+o.Name()) == nil && o.Name() != "RFC3597" {
			log.Fatalf("Constant Type%s does not exist.", o.Name())
		}

		for i := 1; i < st.NumFields(); i++ {
			if _, ok := st.Field(i).Type().(*types.Slice); ok {
				if st.Tag(i) == `dns:"domain-name"` {
					domainTypes = append(domainTypes, o.Name())
					continue Names
				}
				if st.Tag(i) == `dns:"cdomain-name"` {
					cdomainTypes = append(cdomainTypes, o.Name())
					domainTypes = append(domainTypes, o.Name())
					continue Names
				}
				continue
			}

			switch {
			case st.Tag(i) == `dns:"domain-name"`:
				domainTypes = append(domainTypes, o.Name())
				continue Names
			case st.Tag(i) == `dns:"cdomain-name"`:
				cdomainTypes = append(cdomainTypes, o.Name())
				domainTypes = append(domainTypes, o.Name())
				continue Names
			}
		}
	}

	b := &bytes.Buffer{}
	b.WriteString(packageHdr)

	fmt.Fprint(b, "func compressionLenHelperType(c map[string]int, r RR) {\n")
	fmt.Fprint(b, "switch x := r.(type) {\n")
	for _, name := range domainTypes {
		o := scope.Lookup(name)
		st, _ := getTypeStruct(o.Type(), scope)

		fmt.Fprintf(b, "case *%s:\n", name)
		for i := 1; i < st.NumFields(); i++ {
			out := func(s string) { fmt.Fprintf(b, "compressionLenHelper(c, x.%s)\n", st.Field(i).Name()) }

			if _, ok := st.Field(i).Type().(*types.Slice); ok {
				switch st.Tag(i) {
				case `dns:"domain-name"`:
					fallthrough
				case `dns:"cdomain-name"`:

					fmt.Fprintf(b, `for i := range x.%s {
						compressionLenHelper(c, x.%s[i])
					}
`, st.Field(i).Name(), st.Field(i).Name())
				}
				continue
			}

			switch {
			case st.Tag(i) == `dns:"cdomain-name"`:
				fallthrough
			case st.Tag(i) == `dns:"domain-name"`:
				out(st.Field(i).Name())
			}
		}
	}
	fmt.Fprintln(b, "}\n}\n\n")

	fmt.Fprint(b, "func compressionLenSearchType(c map[string]int, r RR) (int, bool) {\n")
	fmt.Fprint(b, "switch x := r.(type) {\n")
	for _, name := range cdomainTypes {
		o := scope.Lookup(name)
		st, _ := getTypeStruct(o.Type(), scope)

		fmt.Fprintf(b, "case *%s:\n", name)
		j := 1
		for i := 1; i < st.NumFields(); i++ {
			out := func(s string, j int) {
				fmt.Fprintf(b, "k%d, ok%d := compressionLenSearch(c, x.%s)\n", j, j, st.Field(i).Name())
			}

			switch {
			case st.Tag(i) == `dns:"cdomain-name"`:
				out(st.Field(i).Name(), j)
				j++
			}
		}
		k := "k1"
		ok := "ok1"
		for i := 2; i < j; i++ {
			k += fmt.Sprintf(" + k%d", i)
			ok += fmt.Sprintf(" && ok%d", i)
		}
		fmt.Fprintf(b, "return %s, %s\n", k, ok)
	}
	fmt.Fprintln(b, "}\nreturn 0, false\n}\n\n")

	res, err := format.Source(b.Bytes())
	if err != nil {
		b.WriteTo(os.Stderr)
		log.Fatal(err)
	}

	f, err := os.Create("zcompress.go")
	fatalIfErr(err)
	defer f.Close()
	f.Write(res)
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
