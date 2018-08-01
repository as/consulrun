package template

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/as/consulrun/hashicorp/errwrap"
	sockaddr "github.com/as/consulrun/hashicorp/go-sockaddr"
)

var (
	SourceFuncs template.FuncMap

	SortFuncs template.FuncMap

	FilterFuncs template.FuncMap

	HelperFuncs template.FuncMap
)

func init() {
	SourceFuncs = template.FuncMap{

		"GetAllInterfaces": sockaddr.GetAllInterfaces,

		"GetDefaultInterfaces": sockaddr.GetDefaultInterfaces,

		"GetPrivateInterfaces": sockaddr.GetPrivateInterfaces,

		"GetPublicInterfaces": sockaddr.GetPublicInterfaces,
	}

	SortFuncs = template.FuncMap{
		"sort": sockaddr.SortIfBy,
	}

	FilterFuncs = template.FuncMap{
		"exclude": sockaddr.ExcludeIfs,
		"include": sockaddr.IncludeIfs,
	}

	HelperFuncs = template.FuncMap{

		"attr":   Attr,
		"join":   sockaddr.JoinIfAddrs,
		"limit":  sockaddr.LimitIfAddrs,
		"offset": sockaddr.OffsetIfAddrs,
		"unique": sockaddr.UniqueIfAddrsBy,

		"math": sockaddr.IfAddrsMath,

		"GetPrivateIP": sockaddr.GetPrivateIP,

		"GetPrivateIPs": sockaddr.GetPrivateIPs,

		"GetPublicIP": sockaddr.GetPublicIP,

		"GetPublicIPs": sockaddr.GetPublicIPs,

		"GetInterfaceIP": sockaddr.GetInterfaceIP,

		"GetInterfaceIPs": sockaddr.GetInterfaceIPs,
	}
}

func Attr(selectorName string, ifAddrsRaw interface{}) (string, error) {
	switch v := ifAddrsRaw.(type) {
	case sockaddr.IfAddr:
		return sockaddr.IfAttr(selectorName, v)
	case sockaddr.IfAddrs:
		return sockaddr.IfAttrs(selectorName, v)
	default:
		return "", fmt.Errorf("unable to obtain attribute %s from type %T (%v)", selectorName, ifAddrsRaw, ifAddrsRaw)
	}
}

func Parse(input string) (string, error) {
	addrs, err := sockaddr.GetAllInterfaces()
	if err != nil {
		return "", errwrap.Wrapf("unable to query interface addresses: {{err}}", err)
	}

	return ParseIfAddrs(input, addrs)
}

func ParseIfAddrs(input string, ifAddrs sockaddr.IfAddrs) (string, error) {
	return ParseIfAddrsTemplate(input, ifAddrs, template.New("sockaddr.Parse"))
}

func ParseIfAddrsTemplate(input string, ifAddrs sockaddr.IfAddrs, tmplIn *template.Template) (string, error) {

	tmpl, err := tmplIn.Option("missingkey=error").
		Funcs(SourceFuncs).
		Funcs(SortFuncs).
		Funcs(FilterFuncs).
		Funcs(HelperFuncs).
		Parse(input)
	if err != nil {
		return "", errwrap.Wrapf(fmt.Sprintf("unable to parse template %+q: {{err}}", input), err)
	}

	var outWriter bytes.Buffer
	err = tmpl.Execute(&outWriter, ifAddrs)
	if err != nil {
		return "", errwrap.Wrapf(fmt.Sprintf("unable to execute sockaddr input %+q: {{err}}", input), err)
	}

	return outWriter.String(), nil
}
