// checkpoint is a package for checking version information and alerts
package checkpoint

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	mrand "math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/as/consulrun/hashicorp/go-cleanhttp"
	uuid "github.com/as/consulrun/hashicorp/go-uuid"
)

var magicBytes [4]byte = [4]byte{0x35, 0x77, 0x69, 0xFB}

type ReportParams struct {

	//

	Signature     string `json:"signature"`
	SignatureFile string `json:"-"`

	StartTime     time.Time   `json:"start_time"`
	EndTime       time.Time   `json:"end_time"`
	Arch          string      `json:"arch"`
	OS            string      `json:"os"`
	Payload       interface{} `json:"payload,omitempty"`
	Product       string      `json:"product"`
	RunID         string      `json:"run_id"`
	SchemaVersion string      `json:"schema_version"`
	Version       string      `json:"version"`
}

func (i *ReportParams) signature() string {
	signature := i.Signature
	if i.Signature == "" && i.SignatureFile != "" {
		var err error
		signature, err = checkSignature(i.SignatureFile)
		if err != nil {
			return ""
		}
	}
	return signature
}

func Report(ctx context.Context, r *ReportParams) error {
	if disabled := os.Getenv("CHECKPOINT_DISABLE"); disabled != "" {
		return nil
	}

	req, err := ReportRequest(r)
	if err != nil {
		return err
	}

	client := cleanhttp.DefaultClient()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 {
		return fmt.Errorf("Unknown status: %d", resp.StatusCode)
	}

	return nil
}

func ReportRequest(r *ReportParams) (*http.Request, error) {

	if r.RunID == "" {
		uuid, err := uuid.GenerateUUID()
		if err != nil {
			return nil, err
		}
		r.RunID = uuid
	}
	if r.Arch == "" {
		r.Arch = runtime.GOARCH
	}
	if r.OS == "" {
		r.OS = runtime.GOOS
	}
	if r.Signature == "" {
		r.Signature = r.signature()
	}

	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	u := &url.URL{
		Scheme: "https",
		Host:   "checkpoint-api.hashicorp.com",
		Path:   fmt.Sprintf("/v1/telemetry/%s", r.Product),
	}

	req, err := http.NewRequest("POST", u.String(), bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("User-Agent", "HashiCorp/go-checkpoint")

	return req, nil
}

type CheckParams struct {
	Product string
	Version string

	Arch string
	OS   string

	//

	Signature     string
	SignatureFile string

	//

	CacheFile     string
	CacheDuration time.Duration

	Force bool
}

type CheckResponse struct {
	Product             string
	CurrentVersion      string `json:"current_version"`
	CurrentReleaseDate  int    `json:"current_release_date"`
	CurrentDownloadURL  string `json:"current_download_url"`
	CurrentChangelogURL string `json:"current_changelog_url"`
	ProjectWebsite      string `json:"project_website"`
	Outdated            bool   `json:"outdated"`
	Alerts              []*CheckAlert
}

//
type CheckAlert struct {
	ID      int
	Date    int
	Message string
	URL     string
	Level   string
}

func Check(p *CheckParams) (*CheckResponse, error) {
	if disabled := os.Getenv("CHECKPOINT_DISABLE"); disabled != "" && !p.Force {
		return &CheckResponse{}, nil
	}

	timeout := 3000
	if _, err := strconv.Atoi(os.Getenv("CHECKPOINT_TIMEOUT")); err == nil {
		timeout, _ = strconv.Atoi(os.Getenv("CHECKPOINT_TIMEOUT"))
	}

	if r, err := checkCache(p.Version, p.CacheFile, p.CacheDuration); err != nil {
		return nil, err
	} else if r != nil {
		defer r.Close()
		return checkResult(r)
	}

	var u url.URL

	if p.Arch == "" {
		p.Arch = runtime.GOARCH
	}
	if p.OS == "" {
		p.OS = runtime.GOOS
	}

	signature := p.Signature
	if p.Signature == "" && p.SignatureFile != "" {
		var err error
		signature, err = checkSignature(p.SignatureFile)
		if err != nil {
			return nil, err
		}
	}

	v := u.Query()
	v.Set("version", p.Version)
	v.Set("arch", p.Arch)
	v.Set("os", p.OS)
	v.Set("signature", signature)

	u.Scheme = "https"
	u.Host = "checkpoint-api.hashicorp.com"
	u.Path = fmt.Sprintf("/v1/check/%s", p.Product)
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("User-Agent", "HashiCorp/go-checkpoint")

	client := cleanhttp.DefaultClient()

	client.Timeout = time.Duration(timeout) * time.Millisecond

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("Unknown status: %d", resp.StatusCode)
	}

	var r io.Reader = resp.Body
	if p.CacheFile != "" {

		if err := os.MkdirAll(filepath.Dir(p.CacheFile), 0755); err != nil {
			return nil, err
		}

		f, err := os.Create(p.CacheFile)
		if err != nil {
			return nil, err
		}

		if err := writeCacheHeader(f, p.Version); err != nil {
			f.Close()
			os.Remove(p.CacheFile)
			return nil, err
		}

		defer f.Close()
		r = io.TeeReader(r, f)
	}

	return checkResult(r)
}

func CheckInterval(p *CheckParams, interval time.Duration, cb func(*CheckResponse, error)) chan struct{} {
	doneCh := make(chan struct{})

	if disabled := os.Getenv("CHECKPOINT_DISABLE"); disabled != "" {
		return doneCh
	}

	go func() {
		for {
			select {
			case <-time.After(randomStagger(interval)):
				resp, err := Check(p)
				cb(resp, err)
			case <-doneCh:
				return
			}
		}
	}()

	return doneCh
}

func randomStagger(interval time.Duration) time.Duration {
	stagger := time.Duration(mrand.Int63()) % (interval / 2)
	return 3*(interval/4) + stagger
}

func checkCache(current string, path string, d time.Duration) (io.ReadCloser, error) {
	fi, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {

			return nil, nil
		}

		return nil, err
	}

	if d == 0 {
		d = 48 * time.Hour
	}

	if fi.ModTime().Add(d).Before(time.Now()) {

		os.Remove(path)
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	var sig [4]byte
	if err := binary.Read(f, binary.LittleEndian, sig[:]); err != nil {
		f.Close()
		return nil, err
	}
	if !reflect.DeepEqual(sig, magicBytes) {

		f.Close()
		return nil, nil
	}

	var length uint32
	if err := binary.Read(f, binary.LittleEndian, &length); err != nil {
		f.Close()
		return nil, err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(f, data); err != nil {
		f.Close()
		return nil, err
	}
	if string(data) != current {

		f.Close()
		return nil, nil
	}

	return f, nil
}

func checkResult(r io.Reader) (*CheckResponse, error) {
	var result CheckResponse
	dec := json.NewDecoder(r)
	if err := dec.Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

func checkSignature(path string) (string, error) {
	_, err := os.Stat(path)
	if err == nil {

		sigBytes, err := ioutil.ReadFile(path)
		if err != nil {
			return "", err
		}

		lines := strings.SplitN(string(sigBytes), "\n", 2)
		if len(lines) > 0 {
			return strings.TrimSpace(lines[0]), nil
		}
	}

	if !os.IsNotExist(err) {
		return "", err
	}

	var b [16]byte
	n := 0
	for n < 16 {
		n2, err := rand.Read(b[n:])
		if err != nil {
			return "", err
		}

		n += n2
	}
	signature := fmt.Sprintf(
		"%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return "", err
	}

	if err := ioutil.WriteFile(path, []byte(signature+"\n\n"+userMessage+"\n"), 0644); err != nil {
		return "", err
	}

	return signature, nil
}

func writeCacheHeader(f io.Writer, v string) error {

	if err := binary.Write(f, binary.LittleEndian, magicBytes); err != nil {
		return err
	}

	var length uint32 = uint32(len(v))
	if err := binary.Write(f, binary.LittleEndian, length); err != nil {
		return err
	}

	_, err := f.Write([]byte(v))
	return err
}

var userMessage = `
This signature is a randomly generated UUID used to de-duplicate
alerts and version information. This signature is random, it is
not based on any personally identifiable information. To create
a new signature, you can simply delete this file at any time.
See the documentation for the software using Checkpoint for more
information on how to disable it.
`
