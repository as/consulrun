package sockets

import (
	"net"
	"net/url"
	"os"
	"strings"

	"golang.org/x/net/proxy"
)

func GetProxyEnv(key string) string {
	proxyValue := os.Getenv(strings.ToUpper(key))
	if proxyValue == "" {
		return os.Getenv(strings.ToLower(key))
	}
	return proxyValue
}

func DialerFromEnvironment(direct *net.Dialer) (proxy.Dialer, error) {
	allProxy := GetProxyEnv("all_proxy")
	if len(allProxy) == 0 {
		return direct, nil
	}

	proxyURL, err := url.Parse(allProxy)
	if err != nil {
		return direct, err
	}

	proxyFromURL, err := proxy.FromURL(proxyURL, direct)
	if err != nil {
		return direct, err
	}

	noProxy := GetProxyEnv("no_proxy")
	if len(noProxy) == 0 {
		return proxyFromURL, nil
	}

	perHost := proxy.NewPerHost(proxyFromURL, direct)
	perHost.AddFromString(noProxy)

	return perHost, nil
}
