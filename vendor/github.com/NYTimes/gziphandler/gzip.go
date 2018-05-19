package gziphandler

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

const (
	vary            = "Vary"
	acceptEncoding  = "Accept-Encoding"
	contentEncoding = "Content-Encoding"
	contentType     = "Content-Type"
	contentLength   = "Content-Length"
)

type codings map[string]float64

const (
	DefaultQValue = 1.0

	DefaultMinSize = 1400
)

var gzipWriterPools [gzip.BestCompression - gzip.BestSpeed + 2]*sync.Pool

func init() {
	for i := gzip.BestSpeed; i <= gzip.BestCompression; i++ {
		addLevelPool(i)
	}
	addLevelPool(gzip.DefaultCompression)
}

func poolIndex(level int) int {

	if level == gzip.DefaultCompression {
		return gzip.BestCompression - gzip.BestSpeed + 1
	}
	return level - gzip.BestSpeed
}

func addLevelPool(level int) {
	gzipWriterPools[poolIndex(level)] = &sync.Pool{
		New: func() interface{} {

			w, _ := gzip.NewWriterLevel(nil, level)
			return w
		},
	}
}

type GzipResponseWriter struct {
	http.ResponseWriter
	index int // Index for gzipWriterPools.
	gw    *gzip.Writer

	code int // Saves the WriteHeader value.

	minSize int    // Specifed the minimum response size to gzip. If the response length is bigger than this value, it is compressed.
	buf     []byte // Holds the first part of the write before reaching the minSize or the end of the write.

	contentTypes []string // Only compress if the response is one of these content-types. All are accepted if empty.
}

type GzipResponseWriterWithCloseNotify struct {
	*GzipResponseWriter
}

func (w GzipResponseWriterWithCloseNotify) CloseNotify() <-chan bool {
	return w.ResponseWriter.(http.CloseNotifier).CloseNotify()
}

func (w *GzipResponseWriter) Write(b []byte) (int, error) {

	if _, ok := w.Header()[contentType]; !ok {

		w.Header().Set(contentType, http.DetectContentType(b))
	}

	if w.gw != nil {
		n, err := w.gw.Write(b)
		return n, err
	}

	w.buf = append(w.buf, b...)

	if len(w.buf) >= w.minSize && handleContentType(w.contentTypes, w) && w.Header().Get(contentEncoding) == "" {
		err := w.startGzip()
		if err != nil {
			return 0, err
		}
	}

	return len(b), nil
}

func (w *GzipResponseWriter) startGzip() error {

	w.Header().Set(contentEncoding, "gzip")

	w.Header().Del(contentLength)

	if w.code != 0 {
		w.ResponseWriter.WriteHeader(w.code)
	}

	w.init()

	n, err := w.gw.Write(w.buf)

	if err == nil && n < len(w.buf) {
		return io.ErrShortWrite
	}

	w.buf = nil
	return err
}

func (w *GzipResponseWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}

func (w *GzipResponseWriter) init() {

	gzw := gzipWriterPools[w.index].Get().(*gzip.Writer)
	gzw.Reset(w.ResponseWriter)
	w.gw = gzw
}

func (w *GzipResponseWriter) Close() error {
	if w.gw == nil {

		if w.code != 0 {
			w.ResponseWriter.WriteHeader(w.code)
		}
		if w.buf != nil {
			_, writeErr := w.ResponseWriter.Write(w.buf)

			if writeErr != nil {
				return fmt.Errorf("gziphandler: write to regular responseWriter at close gets error: %q", writeErr.Error())
			}
		}
		return nil
	}

	err := w.gw.Close()
	gzipWriterPools[w.index].Put(w.gw)
	w.gw = nil
	return err
}

func (w *GzipResponseWriter) Flush() {
	if w.gw == nil {

		//

		return
	}

	w.gw.Flush()

	if fw, ok := w.ResponseWriter.(http.Flusher); ok {
		fw.Flush()
	}
}

func (w *GzipResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, fmt.Errorf("http.Hijacker interface is not supported")
}

var _ http.Hijacker = &GzipResponseWriter{}

func MustNewGzipLevelHandler(level int) func(http.Handler) http.Handler {
	wrap, err := NewGzipLevelHandler(level)
	if err != nil {
		panic(err)
	}
	return wrap
}

func NewGzipLevelHandler(level int) (func(http.Handler) http.Handler, error) {
	return NewGzipLevelAndMinSize(level, DefaultMinSize)
}

func NewGzipLevelAndMinSize(level, minSize int) (func(http.Handler) http.Handler, error) {
	return GzipHandlerWithOpts(CompressionLevel(level), MinSize(minSize))
}

func GzipHandlerWithOpts(opts ...option) (func(http.Handler) http.Handler, error) {
	c := &config{
		level:   gzip.DefaultCompression,
		minSize: DefaultMinSize,
	}

	for _, o := range opts {
		o(c)
	}

	if err := c.validate(); err != nil {
		return nil, err
	}

	return func(h http.Handler) http.Handler {
		index := poolIndex(c.level)

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Add(vary, acceptEncoding)
			if acceptsGzip(r) {
				gw := &GzipResponseWriter{
					ResponseWriter: w,
					index:          index,
					minSize:        c.minSize,
					contentTypes:   c.contentTypes,
				}
				defer gw.Close()

				if _, ok := w.(http.CloseNotifier); ok {
					gwcn := GzipResponseWriterWithCloseNotify{gw}
					h.ServeHTTP(gwcn, r)
				} else {
					h.ServeHTTP(gw, r)
				}

			} else {
				h.ServeHTTP(w, r)
			}
		})
	}, nil
}

type config struct {
	minSize      int
	level        int
	contentTypes []string
}

func (c *config) validate() error {
	if c.level != gzip.DefaultCompression && (c.level < gzip.BestSpeed || c.level > gzip.BestCompression) {
		return fmt.Errorf("invalid compression level requested: %d", c.level)
	}

	if c.minSize < 0 {
		return fmt.Errorf("minimum size must be more than zero")
	}

	return nil
}

type option func(c *config)

func MinSize(size int) option {
	return func(c *config) {
		c.minSize = size
	}
}

func CompressionLevel(level int) option {
	return func(c *config) {
		c.level = level
	}
}

func ContentTypes(types []string) option {
	return func(c *config) {
		c.contentTypes = []string{}
		for _, v := range types {
			c.contentTypes = append(c.contentTypes, strings.ToLower(v))
		}
	}
}

func GzipHandler(h http.Handler) http.Handler {
	wrapper, _ := NewGzipLevelHandler(gzip.DefaultCompression)
	return wrapper(h)
}

func acceptsGzip(r *http.Request) bool {
	acceptedEncodings, _ := parseEncodings(r.Header.Get(acceptEncoding))
	return acceptedEncodings["gzip"] > 0.0
}

func handleContentType(contentTypes []string, w http.ResponseWriter) bool {

	if len(contentTypes) == 0 {
		return true
	}

	ct := strings.ToLower(w.Header().Get(contentType))
	for _, c := range contentTypes {
		if c == ct {
			return true
		}
	}

	return false
}

//
func parseEncodings(s string) (codings, error) {
	c := make(codings)
	var e []string

	for _, ss := range strings.Split(s, ",") {
		coding, qvalue, err := parseCoding(ss)

		if err != nil {
			e = append(e, err.Error())
		} else {
			c[coding] = qvalue
		}
	}

	if len(e) > 0 {
		return c, fmt.Errorf("errors while parsing encodings: %s", strings.Join(e, ", "))
	}

	return c, nil
}

func parseCoding(s string) (coding string, qvalue float64, err error) {
	for n, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		qvalue = DefaultQValue

		if n == 0 {
			coding = strings.ToLower(part)
		} else if strings.HasPrefix(part, "q=") {
			qvalue, err = strconv.ParseFloat(strings.TrimPrefix(part, "q="), 64)

			if qvalue < 0.0 {
				qvalue = 0.0
			} else if qvalue > 1.0 {
				qvalue = 1.0
			}
		}
	}

	if coding == "" {
		err = fmt.Errorf("empty content-coding")
	}

	return
}
