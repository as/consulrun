package agent

import (
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"regexp"

	"github.com/armon/go-metrics"
	"github.com/as/consulrun/hashicorp/consul/agent/config"
	"github.com/as/consulrun/hashicorp/consul/agent/consul"
	"github.com/as/consulrun/hashicorp/consul/agent/structs"
	"github.com/as/consulrun/hashicorp/consul/lib"
	"github.com/miekg/dns"
)

const (
	maxUDPAnswerLimit = 8
	maxRecurseRecords = 5

	staleCounterThreshold = 5 * time.Second

	defaultMaxUDPSize = 512

	MaxDNSLabelLength = 63
)

var InvalidDnsRe = regexp.MustCompile(`[^A-Za-z0-9\\-]+`)

type dnsConfig struct {
	AllowStale      bool
	Datacenter      string
	EnableTruncate  bool
	MaxStale        time.Duration
	NodeName        string
	NodeTTL         time.Duration
	OnlyPassing     bool
	RecursorTimeout time.Duration
	SegmentName     string
	ServiceTTL      map[string]time.Duration
	UDPAnswerLimit  int
	ARecordLimit    int
}

type DNSServer struct {
	*dns.Server
	agent     *Agent
	config    *dnsConfig
	domain    string
	recursors []string
	logger    *log.Logger

	disableCompression atomic.Value
}

func NewDNSServer(a *Agent) (*DNSServer, error) {
	var recursors []string
	for _, r := range a.config.DNSRecursors {
		ra, err := recursorAddr(r)
		if err != nil {
			return nil, fmt.Errorf("Invalid recursor address: %v", err)
		}
		recursors = append(recursors, ra)
	}

	domain := dns.Fqdn(strings.ToLower(a.config.DNSDomain))

	dnscfg := GetDNSConfig(a.config)
	srv := &DNSServer{
		agent:     a,
		config:    dnscfg,
		domain:    domain,
		logger:    a.logger,
		recursors: recursors,
	}
	srv.disableCompression.Store(a.config.DNSDisableCompression)

	return srv, nil
}

func GetDNSConfig(conf *config.RuntimeConfig) *dnsConfig {
	return &dnsConfig{
		AllowStale:      conf.DNSAllowStale,
		ARecordLimit:    conf.DNSARecordLimit,
		Datacenter:      conf.Datacenter,
		EnableTruncate:  conf.DNSEnableTruncate,
		MaxStale:        conf.DNSMaxStale,
		NodeName:        conf.NodeName,
		NodeTTL:         conf.DNSNodeTTL,
		OnlyPassing:     conf.DNSOnlyPassing,
		RecursorTimeout: conf.DNSRecursorTimeout,
		SegmentName:     conf.SegmentName,
		ServiceTTL:      conf.DNSServiceTTL,
		UDPAnswerLimit:  conf.DNSUDPAnswerLimit,
	}
}

func (d *DNSServer) ListenAndServe(network, addr string, notif func()) error {
	mux := dns.NewServeMux()
	mux.HandleFunc("arpa.", d.handlePtr)
	mux.HandleFunc(d.domain, d.handleQuery)
	if len(d.recursors) > 0 {
		mux.HandleFunc(".", d.handleRecurse)
	}

	d.Server = &dns.Server{
		Addr:              addr,
		Net:               network,
		Handler:           mux,
		NotifyStartedFunc: notif,
	}
	if network == "udp" {
		d.UDPSize = 65535
	}
	return d.Server.ListenAndServe()
}

func recursorAddr(recursor string) (string, error) {

START:
	_, _, err := net.SplitHostPort(recursor)
	if ae, ok := err.(*net.AddrError); ok && ae.Err == "missing port in address" {
		recursor = fmt.Sprintf("%s:%d", recursor, 53)
		goto START
	}
	if err != nil {
		return "", err
	}

	addr, err := net.ResolveTCPAddr("tcp", recursor)
	if err != nil {
		return "", err
	}

	return addr.String(), nil
}

func (d *DNSServer) handlePtr(resp dns.ResponseWriter, req *dns.Msg) {
	q := req.Question[0]
	defer func(s time.Time) {
		metrics.MeasureSinceWithLabels([]string{"consul", "dns", "ptr_query"}, s,
			[]metrics.Label{{Name: "node", Value: d.agent.config.NodeName}})
		metrics.MeasureSinceWithLabels([]string{"dns", "ptr_query"}, s,
			[]metrics.Label{{Name: "node", Value: d.agent.config.NodeName}})
		d.logger.Printf("[DEBUG] dns: request for %v (%v) from client %s (%s)",
			q, time.Since(s), resp.RemoteAddr().String(),
			resp.RemoteAddr().Network())
	}(time.Now())

	m := new(dns.Msg)
	m.SetReply(req)
	m.Compress = !d.disableCompression.Load().(bool)
	m.Authoritative = true
	m.RecursionAvailable = (len(d.recursors) > 0)

	if req.Question[0].Qtype == dns.TypeSOA {
		d.addSOA(m)
	}

	datacenter := d.agent.config.Datacenter

	qName := strings.ToLower(dns.Fqdn(req.Question[0].Name))

	args := structs.DCSpecificRequest{
		Datacenter: datacenter,
		QueryOptions: structs.QueryOptions{
			Token:      d.agent.tokens.UserToken(),
			AllowStale: d.config.AllowStale,
		},
	}
	var out structs.IndexedNodes

	if err := d.agent.RPC("Catalog.ListNodes", &args, &out); err == nil {
		for _, n := range out.Nodes {
			arpa, _ := dns.ReverseAddr(n.Address)
			if arpa == qName {
				ptr := &dns.PTR{
					Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypePTR, Class: dns.ClassINET, Ttl: 0},
					Ptr: fmt.Sprintf("%s.node.%s.%s", n.Node, datacenter, d.domain),
				}
				m.Answer = append(m.Answer, ptr)
				break
			}
		}
	}

	if len(m.Answer) == 0 {
		d.handleRecurse(resp, req)
		return
	}

	if edns := req.IsEdns0(); edns != nil {
		m.SetEdns0(edns.UDPSize(), false)
	}

	if err := resp.WriteMsg(m); err != nil {
		d.logger.Printf("[WARN] dns: failed to respond: %v", err)
	}
}

func (d *DNSServer) handleQuery(resp dns.ResponseWriter, req *dns.Msg) {
	q := req.Question[0]
	defer func(s time.Time) {
		metrics.MeasureSinceWithLabels([]string{"consul", "dns", "domain_query"}, s,
			[]metrics.Label{{Name: "node", Value: d.agent.config.NodeName}})
		metrics.MeasureSinceWithLabels([]string{"dns", "domain_query"}, s,
			[]metrics.Label{{Name: "node", Value: d.agent.config.NodeName}})
		d.logger.Printf("[DEBUG] dns: request for name %v type %v class %v (took %v) from client %s (%s)",
			q.Name, dns.Type(q.Qtype), dns.Class(q.Qclass), time.Since(s), resp.RemoteAddr().String(),
			resp.RemoteAddr().Network())
	}(time.Now())

	network := "udp"
	if _, ok := resp.RemoteAddr().(*net.TCPAddr); ok {
		network = "tcp"
	}

	m := new(dns.Msg)
	m.SetReply(req)
	m.Compress = !d.disableCompression.Load().(bool)
	m.Authoritative = true
	m.RecursionAvailable = (len(d.recursors) > 0)

	switch req.Question[0].Qtype {
	case dns.TypeSOA:
		ns, glue := d.nameservers(req.IsEdns0() != nil)
		m.Answer = append(m.Answer, d.soa())
		m.Ns = append(m.Ns, ns...)
		m.Extra = append(m.Extra, glue...)
		m.SetRcode(req, dns.RcodeSuccess)

	case dns.TypeNS:
		ns, glue := d.nameservers(req.IsEdns0() != nil)
		m.Answer = ns
		m.Extra = glue
		m.SetRcode(req, dns.RcodeSuccess)

	case dns.TypeAXFR:
		m.SetRcode(req, dns.RcodeNotImplemented)

	default:
		d.dispatch(network, resp.RemoteAddr(), req, m)
	}

	if edns := req.IsEdns0(); edns != nil {
		m.SetEdns0(edns.UDPSize(), false)
	}

	if err := resp.WriteMsg(m); err != nil {
		d.logger.Printf("[WARN] dns: failed to respond: %v", err)
	}
}

func (d *DNSServer) soa() *dns.SOA {
	return &dns.SOA{
		Hdr: dns.RR_Header{
			Name:   d.domain,
			Rrtype: dns.TypeSOA,
			Class:  dns.ClassINET,
			Ttl:    0,
		},
		Ns:     "ns." + d.domain,
		Serial: uint32(time.Now().Unix()),

		Mbox:    "hostmaster." + d.domain,
		Refresh: 3600,
		Retry:   600,
		Expire:  86400,
		Minttl:  0,
	}
}

func (d *DNSServer) addSOA(msg *dns.Msg) {
	msg.Ns = append(msg.Ns, d.soa())
}

func (d *DNSServer) nameservers(edns bool) (ns []dns.RR, extra []dns.RR) {
	out, err := d.lookupServiceNodes(d.agent.config.Datacenter, structs.ConsulServiceName, "")
	if err != nil {
		d.logger.Printf("[WARN] dns: Unable to get list of servers: %s", err)
		return nil, nil
	}

	if len(out.Nodes) == 0 {
		d.logger.Printf("[WARN] dns: no servers found")
		return
	}

	out.Nodes.Shuffle()

	for _, o := range out.Nodes {
		name, addr, dc := o.Node.Node, o.Node.Address, o.Node.Datacenter

		if InvalidDnsRe.MatchString(name) {
			d.logger.Printf("[WARN] dns: Skipping invalid node %q for NS records", name)
			continue
		}

		fqdn := name + ".node." + dc + "." + d.domain
		fqdn = dns.Fqdn(strings.ToLower(fqdn))

		nsrr := &dns.NS{
			Hdr: dns.RR_Header{
				Name:   d.domain,
				Rrtype: dns.TypeNS,
				Class:  dns.ClassINET,
				Ttl:    uint32(d.config.NodeTTL / time.Second),
			},
			Ns: fqdn,
		}
		ns = append(ns, nsrr)

		glue := d.formatNodeRecord(nil, addr, fqdn, dns.TypeANY, d.config.NodeTTL, edns)
		extra = append(extra, glue...)

		if len(ns) >= 3 {
			return
		}
	}

	return
}

func (d *DNSServer) dispatch(network string, remoteAddr net.Addr, req, resp *dns.Msg) {

	datacenter := d.agent.config.Datacenter

	qName := strings.ToLower(dns.Fqdn(req.Question[0].Name))
	qName = strings.TrimSuffix(qName, d.domain)

	labels := dns.SplitDomainName(qName)

	var dcParsed bool

PARSE:
	n := len(labels)
	if n == 0 {
		goto INVALID
	}

	if req.Question[0].Qtype == dns.TypeSRV && strings.HasPrefix(labels[n-1], "_") {
		labels = append(labels, "service")
		n = n + 1
	}

	switch labels[n-1] {
	case "service":
		if n == 1 {
			goto INVALID
		}

		if n == 3 && strings.HasPrefix(labels[n-2], "_") && strings.HasPrefix(labels[n-3], "_") {

			tag := labels[n-2][1:]

			if tag == "tcp" {
				tag = ""
			}

			d.serviceLookup(network, datacenter, labels[n-3][1:], tag, req, resp)

		} else {

			tag := ""
			if n >= 3 {
				tag = strings.Join(labels[:n-2], ".")
			}

			d.serviceLookup(network, datacenter, labels[n-2], tag, req, resp)
		}

	case "node":
		if n == 1 {
			goto INVALID
		}

		node := strings.Join(labels[:n-1], ".")
		d.nodeLookup(network, datacenter, node, req, resp)

	case "query":
		if n == 1 {
			goto INVALID
		}

		query := strings.Join(labels[:n-1], ".")
		d.preparedQueryLookup(network, datacenter, query, remoteAddr, req, resp)

	case "addr":
		if n != 2 {
			goto INVALID
		}

		switch len(labels[0]) / 2 {

		case 4:
			ip, err := hex.DecodeString(labels[0])
			if err != nil {
				goto INVALID
			}

			resp.Answer = append(resp.Answer, &dns.A{
				Hdr: dns.RR_Header{
					Name:   qName + d.domain,
					Rrtype: dns.TypeA,
					Class:  dns.ClassINET,
					Ttl:    uint32(d.config.NodeTTL / time.Second),
				},
				A: ip,
			})

		case 16:
			ip, err := hex.DecodeString(labels[0])
			if err != nil {
				goto INVALID
			}

			resp.Answer = append(resp.Answer, &dns.AAAA{
				Hdr: dns.RR_Header{
					Name:   qName + d.domain,
					Rrtype: dns.TypeAAAA,
					Class:  dns.ClassINET,
					Ttl:    uint32(d.config.NodeTTL / time.Second),
				},
				AAAA: ip,
			})
		}

	default:

		//

		//

		if dcParsed {
			goto INVALID
		}
		dcParsed = true

		datacenter = labels[n-1]
		labels = labels[:n-1]
		goto PARSE
	}
	return
INVALID:
	d.logger.Printf("[WARN] dns: QName invalid: %s", qName)
	d.addSOA(resp)
	resp.SetRcode(req, dns.RcodeNameError)
}

func (d *DNSServer) nodeLookup(network, datacenter, node string, req, resp *dns.Msg) {

	qType := req.Question[0].Qtype
	if qType != dns.TypeANY && qType != dns.TypeA && qType != dns.TypeAAAA && qType != dns.TypeTXT {
		return
	}

	args := structs.NodeSpecificRequest{
		Datacenter: datacenter,
		Node:       node,
		QueryOptions: structs.QueryOptions{
			Token:      d.agent.tokens.UserToken(),
			AllowStale: d.config.AllowStale,
		},
	}
	var out structs.IndexedNodeServices
RPC:
	if err := d.agent.RPC("Catalog.NodeServices", &args, &out); err != nil {
		d.logger.Printf("[ERR] dns: rpc error: %v", err)
		resp.SetRcode(req, dns.RcodeServerFailure)
		return
	}

	if args.AllowStale {
		if out.LastContact > d.config.MaxStale {
			args.AllowStale = false
			d.logger.Printf("[WARN] dns: Query results too stale, re-requesting")
			goto RPC
		} else if out.LastContact > staleCounterThreshold {
			metrics.IncrCounter([]string{"consul", "dns", "stale_queries"}, 1)
			metrics.IncrCounter([]string{"dns", "stale_queries"}, 1)
		}
	}

	if out.NodeServices == nil {
		d.addSOA(resp)
		resp.SetRcode(req, dns.RcodeNameError)
		return
	}

	n := out.NodeServices.Node
	edns := req.IsEdns0() != nil
	addr := d.agent.TranslateAddress(datacenter, n.Address, n.TaggedAddresses)
	records := d.formatNodeRecord(out.NodeServices.Node, addr, req.Question[0].Name, qType, d.config.NodeTTL, edns)
	if records != nil {
		resp.Answer = append(resp.Answer, records...)
	}
}

func encodeKVasRFC1464(key, value string) (txt string) {

	key = strings.Replace(key, "`", "``", -1)
	key = strings.Replace(key, "=", "`=", -1)

	leadingSpacesRE := regexp.MustCompile("^ +")
	numLeadingSpaces := len(leadingSpacesRE.FindString(key))
	key = leadingSpacesRE.ReplaceAllString(key, strings.Repeat("` ", numLeadingSpaces))

	trailingSpacesRE := regexp.MustCompile(" +$")
	numTrailingSpaces := len(trailingSpacesRE.FindString(key))
	key = trailingSpacesRE.ReplaceAllString(key, strings.Repeat("` ", numTrailingSpaces))

	value = strings.Replace(value, "`", "``", -1)

	return key + "=" + value
}

func (d *DNSServer) formatNodeRecord(node *structs.Node, addr, qName string, qType uint16, ttl time.Duration, edns bool) (records []dns.RR) {

	ip := net.ParseIP(addr)
	var ipv4 net.IP
	if ip != nil {
		ipv4 = ip.To4()
	}

	switch {
	case ipv4 != nil && (qType == dns.TypeANY || qType == dns.TypeA):
		records = append(records, &dns.A{
			Hdr: dns.RR_Header{
				Name:   qName,
				Rrtype: dns.TypeA,
				Class:  dns.ClassINET,
				Ttl:    uint32(ttl / time.Second),
			},
			A: ip,
		})

	case ip != nil && ipv4 == nil && (qType == dns.TypeANY || qType == dns.TypeAAAA):
		records = append(records, &dns.AAAA{
			Hdr: dns.RR_Header{
				Name:   qName,
				Rrtype: dns.TypeAAAA,
				Class:  dns.ClassINET,
				Ttl:    uint32(ttl / time.Second),
			},
			AAAA: ip,
		})

	case ip == nil && (qType == dns.TypeANY || qType == dns.TypeCNAME ||
		qType == dns.TypeA || qType == dns.TypeAAAA || qType == dns.TypeTXT):

		cnRec := &dns.CNAME{
			Hdr: dns.RR_Header{
				Name:   qName,
				Rrtype: dns.TypeCNAME,
				Class:  dns.ClassINET,
				Ttl:    uint32(ttl / time.Second),
			},
			Target: dns.Fqdn(addr),
		}
		records = append(records, cnRec)

		more := d.resolveCNAME(cnRec.Target)
		extra := 0
	MORE_REC:
		for _, rr := range more {
			switch rr.Header().Rrtype {
			case dns.TypeCNAME, dns.TypeA, dns.TypeAAAA, dns.TypeTXT:
				records = append(records, rr)
				extra++
				if extra == maxRecurseRecords && !edns {
					break MORE_REC
				}
			}
		}
	}

	if node != nil && (qType == dns.TypeANY || qType == dns.TypeTXT) {
		for key, value := range node.Meta {
			txt := value
			if !strings.HasPrefix(strings.ToLower(key), "rfc1035-") {
				txt = encodeKVasRFC1464(key, value)
			}
			records = append(records, &dns.TXT{
				Hdr: dns.RR_Header{
					Name:   qName,
					Rrtype: dns.TypeTXT,
					Class:  dns.ClassINET,
					Ttl:    uint32(ttl / time.Second),
				},
				Txt: []string{txt},
			})
		}
	}

	return records
}

func indexRRs(rrs []dns.RR, index map[string]dns.RR) {
	for _, rr := range rrs {
		name := strings.ToLower(rr.Header().Name)
		if _, ok := index[name]; !ok {
			index[name] = rr
		}
	}
}

func syncExtra(index map[string]dns.RR, resp *dns.Msg) {
	extra := make([]dns.RR, 0, len(resp.Answer))
	resolved := make(map[string]struct{}, len(resp.Answer))
	for _, ansRR := range resp.Answer {
		srv, ok := ansRR.(*dns.SRV)
		if !ok {
			continue
		}

		target := strings.ToLower(srv.Target)

	RESOLVE:
		if _, ok := resolved[target]; ok {
			continue
		}
		resolved[target] = struct{}{}

		extraRR, ok := index[target]
		if ok {
			extra = append(extra, extraRR)
			if cname, ok := extraRR.(*dns.CNAME); ok {
				target = strings.ToLower(cname.Target)
				goto RESOLVE
			}
		}
	}
	resp.Extra = extra
}

func dnsBinaryTruncate(resp *dns.Msg, maxSize int, index map[string]dns.RR, hasExtra bool) int {
	originalAnswser := resp.Answer
	startIndex := 0
	endIndex := len(resp.Answer) + 1
	for endIndex-startIndex > 1 {
		median := startIndex + (endIndex-startIndex)/2

		resp.Answer = originalAnswser[:median]
		if hasExtra {
			syncExtra(index, resp)
		}
		aLen := resp.Len()
		if aLen <= maxSize {
			if maxSize-aLen < 10 {

				return median
			}
			startIndex = median
		} else {
			endIndex = median
		}
	}
	return startIndex
}

func (d *DNSServer) trimTCPResponse(req, resp *dns.Msg) (trimmed bool) {
	hasExtra := len(resp.Extra) > 0

	maxSize := 65533 // 64k - 2 bytes

	compressed := resp.Compress
	resp.Compress = false

	var index map[string]dns.RR
	originalSize := resp.Len()
	originalNumRecords := len(resp.Answer)

	truncateAt := 2048
	if req.Question[0].Qtype == dns.TypeSRV {
		truncateAt = 640
	}
	if len(resp.Answer) > truncateAt {
		resp.Answer = resp.Answer[:truncateAt]
	}
	if hasExtra {
		index = make(map[string]dns.RR, len(resp.Extra))
		indexRRs(resp.Extra, index)
	}
	truncated := false

	for len(resp.Answer) > 0 && resp.Len() > maxSize {
		truncated = true

		if resp.Len()-maxSize > 100 {
			bestIndex := dnsBinaryTruncate(resp, maxSize, index, hasExtra)
			resp.Answer = resp.Answer[:bestIndex]
		} else {
			resp.Answer = resp.Answer[:len(resp.Answer)-1]
		}
		if hasExtra {
			syncExtra(index, resp)
		}
	}
	if truncated {
		d.logger.Printf("[DEBUG] dns: TCP answer to %v too large truncated recs:=%d/%d, size:=%d/%d",
			req.Question,
			len(resp.Answer), originalNumRecords, resp.Len(), originalSize)
	}

	resp.Compress = compressed
	return truncated
}

func trimUDPResponse(req, resp *dns.Msg, udpAnswerLimit int) (trimmed bool) {
	numAnswers := len(resp.Answer)
	hasExtra := len(resp.Extra) > 0
	maxSize := defaultMaxUDPSize

	if edns := req.IsEdns0(); edns != nil {
		if size := edns.UDPSize(); size > uint16(maxSize) {
			maxSize = int(size)
		}
	}

	var index map[string]dns.RR
	if hasExtra {
		index = make(map[string]dns.RR, len(resp.Extra))
		indexRRs(resp.Extra, index)
	}

	maxAnswers := lib.MinInt(maxUDPAnswerLimit, udpAnswerLimit)
	if maxSize == defaultMaxUDPSize && numAnswers > maxAnswers {
		resp.Answer = resp.Answer[:maxAnswers]
		if hasExtra {
			syncExtra(index, resp)
		}
	}

	compress := resp.Compress
	resp.Compress = false
	for len(resp.Answer) > 0 && resp.Len() > maxSize {

		if resp.Len()-maxSize > 100 {
			bestIndex := dnsBinaryTruncate(resp, maxSize, index, hasExtra)
			resp.Answer = resp.Answer[:bestIndex]
		} else {
			resp.Answer = resp.Answer[:len(resp.Answer)-1]
		}
		if hasExtra {
			syncExtra(index, resp)
		}
	}
	resp.Compress = compress

	return len(resp.Answer) < numAnswers
}

func (d *DNSServer) trimDNSResponse(network string, req, resp *dns.Msg) (trimmed bool) {
	if network != "tcp" {
		trimmed = trimUDPResponse(req, resp, d.config.UDPAnswerLimit)
	} else {
		trimmed = d.trimTCPResponse(req, resp)
	}

	if trimmed && d.config.EnableTruncate {
		resp.Truncated = true
	}
	return trimmed
}

func (d *DNSServer) lookupServiceNodes(datacenter, service, tag string) (structs.IndexedCheckServiceNodes, error) {
	args := structs.ServiceSpecificRequest{
		Datacenter:  datacenter,
		ServiceName: service,
		ServiceTag:  tag,
		TagFilter:   tag != "",
		QueryOptions: structs.QueryOptions{
			Token:      d.agent.tokens.UserToken(),
			AllowStale: d.config.AllowStale,
		},
	}

	var out structs.IndexedCheckServiceNodes
	if err := d.agent.RPC("Health.ServiceNodes", &args, &out); err != nil {
		return structs.IndexedCheckServiceNodes{}, err
	}

	if args.AllowStale && out.LastContact > staleCounterThreshold {
		metrics.IncrCounter([]string{"consul", "dns", "stale_queries"}, 1)
		metrics.IncrCounter([]string{"dns", "stale_queries"}, 1)
	}

	if args.AllowStale && out.LastContact > d.config.MaxStale {
		args.AllowStale = false
		d.logger.Printf("[WARN] dns: Query results too stale, re-requesting")

		if err := d.agent.RPC("Health.ServiceNodes", &args, &out); err != nil {
			return structs.IndexedCheckServiceNodes{}, err
		}
	}

	out.Nodes = out.Nodes.Filter(d.config.OnlyPassing)
	return out, nil
}

func (d *DNSServer) serviceLookup(network, datacenter, service, tag string, req, resp *dns.Msg) {
	out, err := d.lookupServiceNodes(datacenter, service, tag)
	if err != nil {
		d.logger.Printf("[ERR] dns: rpc error: %v", err)
		resp.SetRcode(req, dns.RcodeServerFailure)
		return
	}

	if len(out.Nodes) == 0 {
		d.addSOA(resp)
		resp.SetRcode(req, dns.RcodeNameError)
		return
	}

	out.Nodes.Shuffle()

	var ttl time.Duration
	if d.config.ServiceTTL != nil {
		var ok bool
		ttl, ok = d.config.ServiceTTL[service]
		if !ok {
			ttl = d.config.ServiceTTL["*"]
		}
	}

	qType := req.Question[0].Qtype
	if qType == dns.TypeSRV {
		d.serviceSRVRecords(datacenter, out.Nodes, req, resp, ttl)
	} else {
		d.serviceNodeRecords(datacenter, out.Nodes, req, resp, ttl)
	}

	d.trimDNSResponse(network, req, resp)

	if len(resp.Answer) == 0 && !resp.Truncated {
		d.addSOA(resp)
		return
	}
}

func ednsSubnetForRequest(req *dns.Msg) *dns.EDNS0_SUBNET {

	edns := req.IsEdns0()

	if edns == nil {
		return nil
	}

	for _, o := range edns.Option {
		if subnet, ok := o.(*dns.EDNS0_SUBNET); ok {
			return subnet
		}
	}

	return nil
}

func (d *DNSServer) preparedQueryLookup(network, datacenter, query string, remoteAddr net.Addr, req, resp *dns.Msg) {

	args := structs.PreparedQueryExecuteRequest{
		Datacenter:    datacenter,
		QueryIDOrName: query,
		QueryOptions: structs.QueryOptions{
			Token:      d.agent.tokens.UserToken(),
			AllowStale: d.config.AllowStale,
		},

		Agent: structs.QuerySource{
			Datacenter: d.agent.config.Datacenter,
			Segment:    d.agent.config.SegmentName,
			Node:       d.agent.config.NodeName,
		},
	}

	subnet := ednsSubnetForRequest(req)

	if subnet != nil {
		args.Source.Ip = subnet.Address.String()
	} else {
		switch v := remoteAddr.(type) {
		case *net.UDPAddr:
			args.Source.Ip = v.IP.String()
		case *net.TCPAddr:
			args.Source.Ip = v.IP.String()
		case *net.IPAddr:
			args.Source.Ip = v.IP.String()
		}
	}

	var out structs.PreparedQueryExecuteResponse
RPC:
	if err := d.agent.RPC("PreparedQuery.Execute", &args, &out); err != nil {

		if err.Error() == consul.ErrQueryNotFound.Error() {
			d.addSOA(resp)
			resp.SetRcode(req, dns.RcodeNameError)
			return
		}

		d.logger.Printf("[ERR] dns: rpc error: %v", err)
		resp.SetRcode(req, dns.RcodeServerFailure)
		return
	}

	if args.AllowStale {
		if out.LastContact > d.config.MaxStale {
			args.AllowStale = false
			d.logger.Printf("[WARN] dns: Query results too stale, re-requesting")
			goto RPC
		} else if out.LastContact > staleCounterThreshold {
			metrics.IncrCounter([]string{"consul", "dns", "stale_queries"}, 1)
			metrics.IncrCounter([]string{"dns", "stale_queries"}, 1)
		}
	}

	var ttl time.Duration
	if out.DNS.TTL != "" {
		var err error
		ttl, err = time.ParseDuration(out.DNS.TTL)
		if err != nil {
			d.logger.Printf("[WARN] dns: Failed to parse TTL '%s' for prepared query '%s', ignoring", out.DNS.TTL, query)
		}
	} else if d.config.ServiceTTL != nil {
		var ok bool
		ttl, ok = d.config.ServiceTTL[out.Service]
		if !ok {
			ttl = d.config.ServiceTTL["*"]
		}
	}

	if len(out.Nodes) == 0 {
		d.addSOA(resp)
		resp.SetRcode(req, dns.RcodeNameError)
		return
	}

	qType := req.Question[0].Qtype
	if qType == dns.TypeSRV {
		d.serviceSRVRecords(out.Datacenter, out.Nodes, req, resp, ttl)
	} else {
		d.serviceNodeRecords(out.Datacenter, out.Nodes, req, resp, ttl)
	}

	d.trimDNSResponse(network, req, resp)

	if len(resp.Answer) == 0 && !resp.Truncated {
		d.addSOA(resp)
		return
	}
}

func (d *DNSServer) serviceNodeRecords(dc string, nodes structs.CheckServiceNodes, req, resp *dns.Msg, ttl time.Duration) {
	qName := req.Question[0].Name
	qType := req.Question[0].Qtype
	handled := make(map[string]struct{})
	edns := req.IsEdns0() != nil

	count := 0
	for _, node := range nodes {

		addr := d.agent.TranslateAddress(dc, node.Node.Address, node.Node.TaggedAddresses)
		if node.Service.Address != "" {
			addr = node.Service.Address
		}

		if qName == strings.TrimSuffix(addr, ".")+"." {
			addr = node.Node.Address
		}

		if _, ok := handled[addr]; ok {
			continue
		}
		handled[addr] = struct{}{}

		records := d.formatNodeRecord(node.Node, addr, qName, qType, ttl, edns)
		if records != nil {
			resp.Answer = append(resp.Answer, records...)
			count++
			if count == d.config.ARecordLimit {

				return
			}
		}
	}
}

func (d *DNSServer) serviceSRVRecords(dc string, nodes structs.CheckServiceNodes, req, resp *dns.Msg, ttl time.Duration) {
	handled := make(map[string]struct{})
	edns := req.IsEdns0() != nil

	for _, node := range nodes {

		tuple := fmt.Sprintf("%s:%s:%d", node.Node.Node, node.Service.Address, node.Service.Port)
		if _, ok := handled[tuple]; ok {
			continue
		}
		handled[tuple] = struct{}{}

		srvRec := &dns.SRV{
			Hdr: dns.RR_Header{
				Name:   req.Question[0].Name,
				Rrtype: dns.TypeSRV,
				Class:  dns.ClassINET,
				Ttl:    uint32(ttl / time.Second),
			},
			Priority: 1,
			Weight:   1,
			Port:     uint16(node.Service.Port),
			Target:   fmt.Sprintf("%s.node.%s.%s", node.Node.Node, dc, d.domain),
		}
		resp.Answer = append(resp.Answer, srvRec)

		addr := d.agent.TranslateAddress(dc, node.Node.Address, node.Node.TaggedAddresses)
		if node.Service.Address != "" {
			addr = node.Service.Address
		}

		records := d.formatNodeRecord(node.Node, addr, srvRec.Target, dns.TypeANY, ttl, edns)
		if len(records) > 0 {

			if addr == node.Node.Address {
				resp.Extra = append(resp.Extra, records...)
			} else {

				switch record := records[0].(type) {

				case *dns.A:
					addr := hex.EncodeToString(record.A)

					srvRec.Target = fmt.Sprintf("%s.addr.%s.%s", addr[len(addr)-(net.IPv4len*2):], dc, d.domain)
					record.Hdr.Name = srvRec.Target
					resp.Extra = append(resp.Extra, record)

				case *dns.AAAA:
					srvRec.Target = fmt.Sprintf("%s.addr.%s.%s", hex.EncodeToString(record.AAAA), dc, d.domain)
					record.Hdr.Name = srvRec.Target
					resp.Extra = append(resp.Extra, record)

				default:
					resp.Extra = append(resp.Extra, records...)
				}
			}
		}
	}
}

func (d *DNSServer) handleRecurse(resp dns.ResponseWriter, req *dns.Msg) {
	q := req.Question[0]
	network := "udp"
	defer func(s time.Time) {
		d.logger.Printf("[DEBUG] dns: request for %v (%s) (%v) from client %s (%s)",
			q, network, time.Since(s), resp.RemoteAddr().String(),
			resp.RemoteAddr().Network())
	}(time.Now())

	if _, ok := resp.RemoteAddr().(*net.TCPAddr); ok {
		network = "tcp"
	}

	c := &dns.Client{Net: network, Timeout: d.config.RecursorTimeout}
	var r *dns.Msg
	var rtt time.Duration
	var err error
	for _, recursor := range d.recursors {
		r, rtt, err = c.Exchange(req, recursor)
		if err == nil || err == dns.ErrTruncated {

			r.Compress = !d.disableCompression.Load().(bool)

			d.logger.Printf("[DEBUG] dns: recurse RTT for %v (%v)", q, rtt)
			if err := resp.WriteMsg(r); err != nil {
				d.logger.Printf("[WARN] dns: failed to respond: %v", err)
			}
			return
		}
		d.logger.Printf("[ERR] dns: recurse failed: %v", err)
	}

	d.logger.Printf("[ERR] dns: all resolvers failed for %v from client %s (%s)",
		q, resp.RemoteAddr().String(), resp.RemoteAddr().Network())
	m := &dns.Msg{}
	m.SetReply(req)
	m.Compress = !d.disableCompression.Load().(bool)
	m.RecursionAvailable = true
	m.SetRcode(req, dns.RcodeServerFailure)
	if edns := req.IsEdns0(); edns != nil {
		m.SetEdns0(edns.UDPSize(), false)
	}
	resp.WriteMsg(m)
}

func (d *DNSServer) resolveCNAME(name string) []dns.RR {

	if strings.HasSuffix(strings.ToLower(name), "."+d.domain) {
		req := &dns.Msg{}
		resp := &dns.Msg{}

		req.SetQuestion(name, dns.TypeANY)
		d.dispatch("udp", nil, req, resp)

		return resp.Answer
	}

	if len(d.recursors) == 0 {
		return nil
	}

	m := new(dns.Msg)
	m.SetQuestion(name, dns.TypeA)

	c := &dns.Client{Net: "udp", Timeout: d.config.RecursorTimeout}
	var r *dns.Msg
	var rtt time.Duration
	var err error
	for _, recursor := range d.recursors {
		r, rtt, err = c.Exchange(m, recursor)
		if err == nil {
			d.logger.Printf("[DEBUG] dns: cname recurse RTT for %v (%v)", name, rtt)
			return r.Answer
		}
		d.logger.Printf("[ERR] dns: cname recurse failed for %v: %v", name, err)
	}
	d.logger.Printf("[ERR] dns: all resolvers failed for %v", name)
	return nil
}
