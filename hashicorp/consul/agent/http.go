package agent

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/NYTimes/gziphandler"
	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/acl"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/go-cleanhttp"
	"github.com/mitchellh/mapstructure"
)

type MethodNotAllowedError struct {
	Method string
	Allow  []string
}

func (e MethodNotAllowedError) Error() string {
	return fmt.Sprintf("method %s not allowed", e.Method)
}

type HTTPServer struct {
	*http.Server
	ln        net.Listener
	agent     *Agent
	blacklist *Blacklist

	proto string
}

type endpoint func(resp http.ResponseWriter, req *http.Request) (interface{}, error)

type unboundEndpoint func(s *HTTPServer, resp http.ResponseWriter, req *http.Request) (interface{}, error)

var endpoints map[string]unboundEndpoint

var allowedMethods map[string][]string

func registerEndpoint(pattern string, methods []string, fn unboundEndpoint) {
	if endpoints == nil {
		endpoints = make(map[string]unboundEndpoint)
	}
	if endpoints[pattern] != nil || allowedMethods[pattern] != nil {
		panic(fmt.Errorf("Pattern %q is already registered", pattern))
	}

	endpoints[pattern] = fn
	allowedMethods[pattern] = methods
}

type wrappedMux struct {
	mux     *http.ServeMux
	handler http.Handler
}

func (w *wrappedMux) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	w.handler.ServeHTTP(resp, req)
}

func (s *HTTPServer) handler(enableDebug bool) http.Handler {
	mux := http.NewServeMux()

	handleFuncMetrics := func(pattern string, handler http.HandlerFunc) {

		var parts []string
		for i, part := range strings.Split(pattern, "/") {
			if part == "" {
				if i == 0 {
					continue
				}
				part = "_"
			}
			parts = append(parts, part)
		}

		wrapper := func(resp http.ResponseWriter, req *http.Request) {
			start := time.Now()
			handler(resp, req)
			key := append([]string{"http", req.Method}, parts...)
			metrics.MeasureSince(append([]string{"consul"}, key...), start)
			metrics.MeasureSince(key, start)
		}

		gzipWrapper, _ := gziphandler.GzipHandlerWithOpts(gziphandler.MinSize(0))
		gzipHandler := gzipWrapper(http.HandlerFunc(wrapper))
		mux.Handle(pattern, gzipHandler)
	}

	mux.HandleFunc("/", s.Index)
	for pattern, fn := range endpoints {
		thisFn := fn
		methods, _ := allowedMethods[pattern]
		bound := func(resp http.ResponseWriter, req *http.Request) (interface{}, error) {
			return thisFn(s, resp, req)
		}
		handleFuncMetrics(pattern, s.wrap(bound, methods))
	}
	if enableDebug {
		handleFuncMetrics("/debug/pprof/", pprof.Index)
		handleFuncMetrics("/debug/pprof/cmdline", pprof.Cmdline)
		handleFuncMetrics("/debug/pprof/profile", pprof.Profile)
		handleFuncMetrics("/debug/pprof/symbol", pprof.Symbol)
	}

	if s.agent.config.UIDir != "" {
		mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(http.Dir(s.agent.config.UIDir))))
	} else if s.agent.config.EnableUI {
		mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServer(assetFS())))
	}

	return &wrappedMux{
		mux:     mux,
		handler: cleanhttp.PrintablePathCheckHandler(mux, nil),
	}
}

func (s *HTTPServer) nodeName() string {
	return s.agent.config.NodeName
}

//
//
//
//
var (
	aclEndpointRE = regexp.MustCompile("^(/v1/acl/[^/]+/)([^?]+)([?]?.*)$")
)

func (s *HTTPServer) wrap(handler endpoint, methods []string) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		setHeaders(resp, s.agent.config.HTTPResponseHeaders)
		setTranslateAddr(resp, s.agent.config.TranslateWANAddrs)

		formVals, err := url.ParseQuery(req.URL.RawQuery)
		if err != nil {
			s.agent.logger.Printf("[ERR] http: Failed to decode query: %s from=%s", err, req.RemoteAddr)
			resp.WriteHeader(http.StatusInternalServerError)
			return
		}
		logURL := req.URL.String()
		if tokens, ok := formVals["token"]; ok {
			for _, token := range tokens {
				if token == "" {
					logURL += "<hidden>"
					continue
				}
				logURL = strings.Replace(logURL, token, "<hidden>", -1)
			}
		}
		logURL = aclEndpointRE.ReplaceAllString(logURL, "$1<hidden>$3")

		if s.blacklist.Block(req.URL.Path) {
			errMsg := "Endpoint is blocked by agent configuration"
			s.agent.logger.Printf("[ERR] http: Request %s %v, error: %v from=%s", req.Method, logURL, err, req.RemoteAddr)
			resp.WriteHeader(http.StatusForbidden)
			fmt.Fprint(resp, errMsg)
			return
		}

		isMethodNotAllowed := func(err error) bool {
			_, ok := err.(MethodNotAllowedError)
			return ok
		}

		addAllowHeader := func(methods []string) {
			resp.Header().Add("Allow", strings.Join(methods, ","))
		}

		handleErr := func(err error) {
			s.agent.logger.Printf("[ERR] http: Request %s %v, error: %v from=%s", req.Method, logURL, err, req.RemoteAddr)
			switch {
			case acl.IsErrPermissionDenied(err) || acl.IsErrNotFound(err):
				resp.WriteHeader(http.StatusForbidden)
				fmt.Fprint(resp, err.Error())
			case structs.IsErrRPCRateExceeded(err):
				resp.WriteHeader(http.StatusTooManyRequests)
			case isMethodNotAllowed(err):

				addAllowHeader(err.(MethodNotAllowedError).Allow)
				resp.WriteHeader(http.StatusMethodNotAllowed) // 405
				fmt.Fprint(resp, err.Error())
			default:
				resp.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(resp, err.Error())
			}
		}

		start := time.Now()
		defer func() {
			s.agent.logger.Printf("[DEBUG] http: Request %s %v (%v) from=%s", req.Method, logURL, time.Since(start), req.RemoteAddr)
		}()

		var obj interface{}

		if req.Method == "OPTIONS" && len(methods) > 0 {
			addAllowHeader(append([]string{"OPTIONS"}, methods...))
			return
		}

		methodFound := len(methods) == 0
		for _, method := range methods {
			if method == req.Method {
				methodFound = true
				break
			}
		}

		if !methodFound {
			err = MethodNotAllowedError{req.Method, append([]string{"OPTIONS"}, methods...)}
		} else {

			obj, err = handler(resp, req)
		}

		if err != nil {
			handleErr(err)
			return
		}
		if obj == nil {
			return
		}

		buf, err := s.marshalJSON(req, obj)
		if err != nil {
			handleErr(err)
			return
		}
		resp.Header().Set("Content-Type", "application/json")
		resp.Write(buf)
	}
}

func (s *HTTPServer) marshalJSON(req *http.Request, obj interface{}) ([]byte, error) {
	if _, ok := req.URL.Query()["pretty"]; ok || s.agent.config.DevMode {
		buf, err := json.MarshalIndent(obj, "", "    ")
		if err != nil {
			return nil, err
		}
		buf = append(buf, "\n"...)
		return buf, nil
	}

	buf, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return buf, err
}

func (s *HTTPServer) IsUIEnabled() bool {
	return s.agent.config.UIDir != "" || s.agent.config.EnableUI
}

func (s *HTTPServer) Index(resp http.ResponseWriter, req *http.Request) {

	if req.URL.Path != "/" {
		resp.WriteHeader(http.StatusNotFound)
		return
	}

	if !s.IsUIEnabled() {
		fmt.Fprint(resp, "Consul Agent")
		return
	}

	http.Redirect(resp, req, "/ui/", http.StatusMovedPermanently) // 301
}

func decodeBody(req *http.Request, out interface{}, cb func(interface{}) error) error {
	var raw interface{}
	dec := json.NewDecoder(req.Body)
	if err := dec.Decode(&raw); err != nil {
		return err
	}

	if cb != nil {
		if err := cb(raw); err != nil {
			return err
		}
	}
	return mapstructure.Decode(raw, out)
}

func setTranslateAddr(resp http.ResponseWriter, active bool) {
	if active {
		resp.Header().Set("X-Consul-Translate-Addresses", "true")
	}
}

func setIndex(resp http.ResponseWriter, index uint64) {
	resp.Header().Set("X-Consul-Index", strconv.FormatUint(index, 10))
}

func setKnownLeader(resp http.ResponseWriter, known bool) {
	s := "true"
	if !known {
		s = "false"
	}
	resp.Header().Set("X-Consul-KnownLeader", s)
}

func setConsistency(resp http.ResponseWriter, consistency string) {
	if consistency != "" {
		resp.Header().Set("X-Consul-Effective-Consistency", consistency)
	}
}

func setLastContact(resp http.ResponseWriter, last time.Duration) {
	if last < 0 {
		last = 0
	}
	lastMsec := uint64(last / time.Millisecond)
	resp.Header().Set("X-Consul-LastContact", strconv.FormatUint(lastMsec, 10))
}

func setMeta(resp http.ResponseWriter, m *structs.QueryMeta) {
	setIndex(resp, m.Index)
	setLastContact(resp, m.LastContact)
	setKnownLeader(resp, m.KnownLeader)
	setConsistency(resp, m.ConsistencyLevel)
}

func setHeaders(resp http.ResponseWriter, headers map[string]string) {
	for field, value := range headers {
		resp.Header().Set(http.CanonicalHeaderKey(field), value)
	}
}

func parseWait(resp http.ResponseWriter, req *http.Request, b *structs.QueryOptions) bool {
	query := req.URL.Query()
	if wait := query.Get("wait"); wait != "" {
		dur, err := time.ParseDuration(wait)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(resp, "Invalid wait time")
			return true
		}
		b.MaxQueryTime = dur
	}
	if idx := query.Get("index"); idx != "" {
		index, err := strconv.ParseUint(idx, 10, 64)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(resp, "Invalid index")
			return true
		}
		b.MinQueryIndex = index
	}
	return false
}

func (s *HTTPServer) parseConsistency(resp http.ResponseWriter, req *http.Request, b *structs.QueryOptions) bool {
	query := req.URL.Query()
	defaults := true
	if _, ok := query["stale"]; ok {
		b.AllowStale = true
		defaults = false
	}
	if _, ok := query["consistent"]; ok {
		b.RequireConsistent = true
		defaults = false
	}
	if _, ok := query["leader"]; ok {
		defaults = false
	}
	if maxStale := query.Get("max_stale"); maxStale != "" {
		dur, err := time.ParseDuration(maxStale)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(resp, "Invalid max_stale value %q", maxStale)
			return true
		}
		b.MaxStaleDuration = dur
		if dur.Nanoseconds() > 0 {
			b.AllowStale = true
			defaults = false
		}
	}

	if defaults {
		path := req.URL.Path
		if strings.HasPrefix(path, "/v1/catalog") || strings.HasPrefix(path, "/v1/health") {
			if s.agent.config.DiscoveryMaxStale.Nanoseconds() > 0 {
				b.MaxStaleDuration = s.agent.config.DiscoveryMaxStale
				b.AllowStale = true
			}
		}
	}
	if b.AllowStale && b.RequireConsistent {
		resp.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(resp, "Cannot specify ?stale with ?consistent, conflicting semantics.")
		return true
	}
	return false
}

func (s *HTTPServer) parseDC(req *http.Request, dc *string) {
	if other := req.URL.Query().Get("dc"); other != "" {
		*dc = other
	} else if *dc == "" {
		*dc = s.agent.config.Datacenter
	}
}

func (s *HTTPServer) parseToken(req *http.Request, token *string) {
	if other := req.URL.Query().Get("token"); other != "" {
		*token = other
		return
	}

	if other := req.Header.Get("X-Consul-Token"); other != "" {
		*token = other
		return
	}

	*token = s.agent.tokens.UserToken()
}

func sourceAddrFromRequest(req *http.Request) string {
	xff := req.Header.Get("X-Forwarded-For")
	forwardHosts := strings.Split(xff, ",")
	if len(forwardHosts) > 0 {
		forwardIp := net.ParseIP(strings.TrimSpace(forwardHosts[0]))
		if forwardIp != nil {
			return forwardIp.String()
		}
	}

	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return ""
	}

	ip := net.ParseIP(host)
	if ip != nil {
		return ip.String()
	} else {
		return ""
	}
}

func (s *HTTPServer) parseSource(req *http.Request, source *structs.QuerySource) {
	s.parseDC(req, &source.Datacenter)
	source.Ip = sourceAddrFromRequest(req)
	if node := req.URL.Query().Get("near"); node != "" {
		if node == "_agent" {
			source.Node = s.agent.config.NodeName
		} else {
			source.Node = node
		}
	}
}

func (s *HTTPServer) parseMetaFilter(req *http.Request) map[string]string {
	if filterList, ok := req.URL.Query()["node-meta"]; ok {
		filters := make(map[string]string)
		for _, filter := range filterList {
			key, value := ParseMetaPair(filter)
			filters[key] = value
		}
		return filters
	}
	return nil
}

func (s *HTTPServer) parse(resp http.ResponseWriter, req *http.Request, dc *string, b *structs.QueryOptions) bool {
	s.parseDC(req, dc)
	s.parseToken(req, &b.Token)
	if s.parseConsistency(resp, req, b) {
		return true
	}
	return parseWait(resp, req, b)
}
