// Copyright 2014 The Go Authors. All rights reserved.

package http2

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"golang.org/x/net/http2/hpack"
	"golang.org/x/net/lex/httplex"
)

const frameHeaderLen = 9

var padZeros = make([]byte, 255) // zeros for padding

// A FrameType is a registered frame type as defined in
type FrameType uint8

const (
	FrameData         FrameType = 0x0
	FrameHeaders      FrameType = 0x1
	FramePriority     FrameType = 0x2
	FrameRSTStream    FrameType = 0x3
	FrameSettings     FrameType = 0x4
	FramePushPromise  FrameType = 0x5
	FramePing         FrameType = 0x6
	FrameGoAway       FrameType = 0x7
	FrameWindowUpdate FrameType = 0x8
	FrameContinuation FrameType = 0x9
)

var frameName = map[FrameType]string{
	FrameData:         "DATA",
	FrameHeaders:      "HEADERS",
	FramePriority:     "PRIORITY",
	FrameRSTStream:    "RST_STREAM",
	FrameSettings:     "SETTINGS",
	FramePushPromise:  "PUSH_PROMISE",
	FramePing:         "PING",
	FrameGoAway:       "GOAWAY",
	FrameWindowUpdate: "WINDOW_UPDATE",
	FrameContinuation: "CONTINUATION",
}

func (t FrameType) String() string {
	if s, ok := frameName[t]; ok {
		return s
	}
	return fmt.Sprintf("UNKNOWN_FRAME_TYPE_%d", uint8(t))
}

// The meaning of flags varies depending on the frame type.
type Flags uint8

func (f Flags) Has(v Flags) bool {
	return (f & v) == v
}

const (
	FlagDataEndStream Flags = 0x1
	FlagDataPadded    Flags = 0x8

	FlagHeadersEndStream  Flags = 0x1
	FlagHeadersEndHeaders Flags = 0x4
	FlagHeadersPadded     Flags = 0x8
	FlagHeadersPriority   Flags = 0x20

	FlagSettingsAck Flags = 0x1

	FlagPingAck Flags = 0x1

	FlagContinuationEndHeaders Flags = 0x4

	FlagPushPromiseEndHeaders Flags = 0x4
	FlagPushPromisePadded     Flags = 0x8
)

var flagName = map[FrameType]map[Flags]string{
	FrameData: {
		FlagDataEndStream: "END_STREAM",
		FlagDataPadded:    "PADDED",
	},
	FrameHeaders: {
		FlagHeadersEndStream:  "END_STREAM",
		FlagHeadersEndHeaders: "END_HEADERS",
		FlagHeadersPadded:     "PADDED",
		FlagHeadersPriority:   "PRIORITY",
	},
	FrameSettings: {
		FlagSettingsAck: "ACK",
	},
	FramePing: {
		FlagPingAck: "ACK",
	},
	FrameContinuation: {
		FlagContinuationEndHeaders: "END_HEADERS",
	},
	FramePushPromise: {
		FlagPushPromiseEndHeaders: "END_HEADERS",
		FlagPushPromisePadded:     "PADDED",
	},
}

// a frameParser parses a frame given its FrameHeader and payload
type frameParser func(fc *frameCache, fh FrameHeader, payload []byte) (Frame, error)

var frameParsers = map[FrameType]frameParser{
	FrameData:         parseDataFrame,
	FrameHeaders:      parseHeadersFrame,
	FramePriority:     parsePriorityFrame,
	FrameRSTStream:    parseRSTStreamFrame,
	FrameSettings:     parseSettingsFrame,
	FramePushPromise:  parsePushPromise,
	FramePing:         parsePingFrame,
	FrameGoAway:       parseGoAwayFrame,
	FrameWindowUpdate: parseWindowUpdateFrame,
	FrameContinuation: parseContinuationFrame,
}

func typeFrameParser(t FrameType) frameParser {
	if f := frameParsers[t]; f != nil {
		return f
	}
	return parseUnknownFrame
}

// A FrameHeader is the 9 byte header of all HTTP/2 frames.
//
type FrameHeader struct {
	valid bool // caller can access []byte fields in the Frame

	Type FrameType

	Flags Flags

	Length uint32

	StreamID uint32
}

// specific frame types and implement the Frame interface.
func (h FrameHeader) Header() FrameHeader { return h }

func (h FrameHeader) String() string {
	var buf bytes.Buffer
	buf.WriteString("[FrameHeader ")
	h.writeDebug(&buf)
	buf.WriteByte(']')
	return buf.String()
}

func (h FrameHeader) writeDebug(buf *bytes.Buffer) {
	buf.WriteString(h.Type.String())
	if h.Flags != 0 {
		buf.WriteString(" flags=")
		set := 0
		for i := uint8(0); i < 8; i++ {
			if h.Flags&(1<<i) == 0 {
				continue
			}
			set++
			if set > 1 {
				buf.WriteByte('|')
			}
			name := flagName[h.Type][Flags(1<<i)]
			if name != "" {
				buf.WriteString(name)
			} else {
				fmt.Fprintf(buf, "0x%x", 1<<i)
			}
		}
	}
	if h.StreamID != 0 {
		fmt.Fprintf(buf, " stream=%d", h.StreamID)
	}
	fmt.Fprintf(buf, " len=%d", h.Length)
}

func (h *FrameHeader) checkValid() {
	if !h.valid {
		panic("Frame accessor called on non-owned Frame")
	}
}

func (h *FrameHeader) invalidate() { h.valid = false }

// frame header bytes.
var fhBytes = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, frameHeaderLen)
		return &buf
	},
}

func ReadFrameHeader(r io.Reader) (FrameHeader, error) {
	bufp := fhBytes.Get().(*[]byte)
	defer fhBytes.Put(bufp)
	return readFrameHeader(*bufp, r)
}

func readFrameHeader(buf []byte, r io.Reader) (FrameHeader, error) {
	_, err := io.ReadFull(r, buf[:frameHeaderLen])
	if err != nil {
		return FrameHeader{}, err
	}
	return FrameHeader{
		Length:   (uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])),
		Type:     FrameType(buf[3]),
		Flags:    Flags(buf[4]),
		StreamID: binary.BigEndian.Uint32(buf[5:]) & (1<<31 - 1),
		valid:    true,
	}, nil
}

// A Frame is the base interface implemented by all frame types.
// Callers will generally type-assert the specific frame type:
//
type Frame interface {
	Header() FrameHeader

	invalidate()
}

type Framer struct {
	r         io.Reader
	lastFrame Frame
	errDetail error

	lastHeaderStream uint32

	maxReadSize uint32
	headerBuf   [frameHeaderLen]byte

	getReadBuf func(size uint32) []byte
	readBuf    []byte // cache for default getReadBuf

	maxWriteSize uint32 // zero means unlimited; TODO: implement

	w    io.Writer
	wbuf []byte

	AllowIllegalWrites bool

	AllowIllegalReads bool

	ReadMetaHeaders *hpack.Decoder

	MaxHeaderListSize uint32

	logReads, logWrites bool

	debugFramer       *Framer // only use for logging written writes
	debugFramerBuf    *bytes.Buffer
	debugReadLoggerf  func(string, ...interface{})
	debugWriteLoggerf func(string, ...interface{})

	frameCache *frameCache // nil if frames aren't reused (default)
}

func (fr *Framer) maxHeaderListSize() uint32 {
	if fr.MaxHeaderListSize == 0 {
		return 16 << 20 // sane default, per docs
	}
	return fr.MaxHeaderListSize
}

func (f *Framer) startWrite(ftype FrameType, flags Flags, streamID uint32) {

	f.wbuf = append(f.wbuf[:0],
		0, // 3 bytes of length, filled in in endWrite
		0,
		0,
		byte(ftype),
		byte(flags),
		byte(streamID>>24),
		byte(streamID>>16),
		byte(streamID>>8),
		byte(streamID))
}

func (f *Framer) endWrite() error {

	length := len(f.wbuf) - frameHeaderLen
	if length >= (1 << 24) {
		return ErrFrameTooLarge
	}
	_ = append(f.wbuf[:0],
		byte(length>>16),
		byte(length>>8),
		byte(length))
	if f.logWrites {
		f.logWrite()
	}

	n, err := f.w.Write(f.wbuf)
	if err == nil && n != len(f.wbuf) {
		err = io.ErrShortWrite
	}
	return err
}

func (f *Framer) logWrite() {
	if f.debugFramer == nil {
		f.debugFramerBuf = new(bytes.Buffer)
		f.debugFramer = NewFramer(nil, f.debugFramerBuf)
		f.debugFramer.logReads = false // we log it ourselves, saying "wrote" below

		f.debugFramer.AllowIllegalReads = true
	}
	f.debugFramerBuf.Write(f.wbuf)
	fr, err := f.debugFramer.ReadFrame()
	if err != nil {
		f.debugWriteLoggerf("http2: Framer %p: failed to decode just-written frame", f)
		return
	}
	f.debugWriteLoggerf("http2: Framer %p: wrote %v", f, summarizeFrame(fr))
}

func (f *Framer) writeByte(v byte)     { f.wbuf = append(f.wbuf, v) }
func (f *Framer) writeBytes(v []byte)  { f.wbuf = append(f.wbuf, v...) }
func (f *Framer) writeUint16(v uint16) { f.wbuf = append(f.wbuf, byte(v>>8), byte(v)) }
func (f *Framer) writeUint32(v uint32) {
	f.wbuf = append(f.wbuf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

const (
	minMaxFrameSize = 1 << 14
	maxFrameSize    = 1<<24 - 1
)

func (fr *Framer) SetReuseFrames() {
	if fr.frameCache != nil {
		return
	}
	fr.frameCache = &frameCache{}
}

type frameCache struct {
	dataFrame DataFrame
}

func (fc *frameCache) getDataFrame() *DataFrame {
	if fc == nil {
		return &DataFrame{}
	}
	return &fc.dataFrame
}

// NewFramer returns a Framer that writes frames to w and reads them from r.
func NewFramer(w io.Writer, r io.Reader) *Framer {
	fr := &Framer{
		w:                 w,
		r:                 r,
		logReads:          logFrameReads,
		logWrites:         logFrameWrites,
		debugReadLoggerf:  log.Printf,
		debugWriteLoggerf: log.Printf,
	}
	fr.getReadBuf = func(size uint32) []byte {
		if cap(fr.readBuf) >= int(size) {
			return fr.readBuf[:size]
		}
		fr.readBuf = make([]byte, size)
		return fr.readBuf
	}
	fr.SetMaxReadFrameSize(maxFrameSize)
	return fr
}

// SetMaxReadFrameSize sets the maximum size of a frame
// limit with a SETTINGS frame.
func (fr *Framer) SetMaxReadFrameSize(v uint32) {
	if v > maxFrameSize {
		v = maxFrameSize
	}
	fr.maxReadSize = v
}

func (fr *Framer) ErrorDetail() error {
	return fr.errDetail
}

// sends a frame that is larger than declared with SetMaxReadFrameSize.
var ErrFrameTooLarge = errors.New("http2: frame too large")

// error from ReadFrame and no other frames should be read.
func terminalReadFrameError(err error) bool {
	if _, ok := err.(StreamError); ok {
		return false
	}
	return err != nil
}

// ReadFrame reads a single frame. The returned Frame is only valid
//
// If the frame is larger than previously set with SetMaxReadFrameSize, the
func (fr *Framer) ReadFrame() (Frame, error) {
	fr.errDetail = nil
	if fr.lastFrame != nil {
		fr.lastFrame.invalidate()
	}
	fh, err := readFrameHeader(fr.headerBuf[:], fr.r)
	if err != nil {
		return nil, err
	}
	if fh.Length > fr.maxReadSize {
		return nil, ErrFrameTooLarge
	}
	payload := fr.getReadBuf(fh.Length)
	if _, err := io.ReadFull(fr.r, payload); err != nil {
		return nil, err
	}
	f, err := typeFrameParser(fh.Type)(fr.frameCache, fh, payload)
	if err != nil {
		if ce, ok := err.(connError); ok {
			return nil, fr.connError(ce.Code, ce.Reason)
		}
		return nil, err
	}
	if err := fr.checkFrameOrder(f); err != nil {
		return nil, err
	}
	if fr.logReads {
		fr.debugReadLoggerf("http2: Framer %p: read %v", fr, summarizeFrame(f))
	}
	if fh.Type == FrameHeaders && fr.ReadMetaHeaders != nil {
		return fr.readMetaFrame(f.(*HeadersFrame))
	}
	return f, nil
}

func (fr *Framer) connError(code ErrCode, reason string) error {
	fr.errDetail = errors.New(reason)
	return ConnectionError(code)
}

// checkFrameOrder reports an error if f is an invalid frame to return
// CONTINUATION frames are contiguous.
func (fr *Framer) checkFrameOrder(f Frame) error {
	last := fr.lastFrame
	fr.lastFrame = f
	if fr.AllowIllegalReads {
		return nil
	}

	fh := f.Header()
	if fr.lastHeaderStream != 0 {
		if fh.Type != FrameContinuation {
			return fr.connError(ErrCodeProtocol,
				fmt.Sprintf("got %s for stream %d; expected CONTINUATION following %s for stream %d",
					fh.Type, fh.StreamID,
					last.Header().Type, fr.lastHeaderStream))
		}
		if fh.StreamID != fr.lastHeaderStream {
			return fr.connError(ErrCodeProtocol,
				fmt.Sprintf("got CONTINUATION for stream %d; expected stream %d",
					fh.StreamID, fr.lastHeaderStream))
		}
	} else if fh.Type == FrameContinuation {
		return fr.connError(ErrCodeProtocol, fmt.Sprintf("unexpected CONTINUATION for stream %d", fh.StreamID))
	}

	switch fh.Type {
	case FrameHeaders, FrameContinuation:
		if fh.Flags.Has(FlagHeadersEndHeaders) {
			fr.lastHeaderStream = 0
		} else {
			fr.lastHeaderStream = fh.StreamID
		}
	}

	return nil
}

type DataFrame struct {
	FrameHeader
	data []byte
}

func (f *DataFrame) StreamEnded() bool {
	return f.FrameHeader.Flags.Has(FlagDataEndStream)
}

// Data returns the frame's data octets, not including any padding
func (f *DataFrame) Data() []byte {
	f.checkValid()
	return f.data
}

func parseDataFrame(fc *frameCache, fh FrameHeader, payload []byte) (Frame, error) {
	if fh.StreamID == 0 {

		return nil, connError{ErrCodeProtocol, "DATA frame with stream ID 0"}
	}
	f := fc.getDataFrame()
	f.FrameHeader = fh

	var padSize byte
	if fh.Flags.Has(FlagDataPadded) {
		var err error
		payload, padSize, err = readByte(payload)
		if err != nil {
			return nil, err
		}
	}
	if int(padSize) > len(payload) {

		return nil, connError{ErrCodeProtocol, "pad size larger than data payload"}
	}
	f.data = payload[:len(payload)-int(padSize)]
	return f, nil
}

var (
	errStreamID    = errors.New("invalid stream ID")
	errDepStreamID = errors.New("invalid dependent stream ID")
	errPadLength   = errors.New("pad length too large")
	errPadBytes    = errors.New("padding bytes must all be zeros unless AllowIllegalWrites is enabled")
)

func validStreamIDOrZero(streamID uint32) bool {
	return streamID&(1<<31) == 0
}

func validStreamID(streamID uint32) bool {
	return streamID != 0 && streamID&(1<<31) == 0
}

// WriteData writes a DATA frame.
//
// It is the caller's responsibility not to violate the maximum frame size
func (f *Framer) WriteData(streamID uint32, endStream bool, data []byte) error {
	return f.WriteDataPadded(streamID, endStream, data, nil)
}

// WriteData writes a DATA frame with optional padding.
//
//
// It is the caller's responsibility not to violate the maximum frame size
func (f *Framer) WriteDataPadded(streamID uint32, endStream bool, data, pad []byte) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	if len(pad) > 0 {
		if len(pad) > 255 {
			return errPadLength
		}
		if !f.AllowIllegalWrites {
			for _, b := range pad {
				if b != 0 {

					return errPadBytes
				}
			}
		}
	}
	var flags Flags
	if endStream {
		flags |= FlagDataEndStream
	}
	if pad != nil {
		flags |= FlagDataPadded
	}
	f.startWrite(FrameData, flags, streamID)
	if pad != nil {
		f.wbuf = append(f.wbuf, byte(len(pad)))
	}
	f.wbuf = append(f.wbuf, data...)
	f.wbuf = append(f.wbuf, pad...)
	return f.endWrite()
}

//
type SettingsFrame struct {
	FrameHeader
	p []byte
}

func parseSettingsFrame(_ *frameCache, fh FrameHeader, p []byte) (Frame, error) {
	if fh.Flags.Has(FlagSettingsAck) && fh.Length > 0 {

		return nil, ConnectionError(ErrCodeFrameSize)
	}
	if fh.StreamID != 0 {

		return nil, ConnectionError(ErrCodeProtocol)
	}
	if len(p)%6 != 0 {

		return nil, ConnectionError(ErrCodeFrameSize)
	}
	f := &SettingsFrame{FrameHeader: fh, p: p}
	if v, ok := f.Value(SettingInitialWindowSize); ok && v > (1<<31)-1 {

		return nil, ConnectionError(ErrCodeFlowControl)
	}
	return f, nil
}

func (f *SettingsFrame) IsAck() bool {
	return f.FrameHeader.Flags.Has(FlagSettingsAck)
}

func (f *SettingsFrame) Value(s SettingID) (v uint32, ok bool) {
	f.checkValid()
	buf := f.p
	for len(buf) > 0 {
		settingID := SettingID(binary.BigEndian.Uint16(buf[:2]))
		if settingID == s {
			return binary.BigEndian.Uint32(buf[2:6]), true
		}
		buf = buf[6:]
	}
	return 0, false
}

func (f *SettingsFrame) ForeachSetting(fn func(Setting) error) error {
	f.checkValid()
	buf := f.p
	for len(buf) > 0 {
		if err := fn(Setting{
			SettingID(binary.BigEndian.Uint16(buf[:2])),
			binary.BigEndian.Uint32(buf[2:6]),
		}); err != nil {
			return err
		}
		buf = buf[6:]
	}
	return nil
}

// WriteSettings writes a SETTINGS frame with zero or more settings
//
func (f *Framer) WriteSettings(settings ...Setting) error {
	f.startWrite(FrameSettings, 0, 0)
	for _, s := range settings {
		f.writeUint16(uint16(s.ID))
		f.writeUint32(s.Val)
	}
	return f.endWrite()
}

// WriteSettingsAck writes an empty SETTINGS frame with the ACK bit set.
//
func (f *Framer) WriteSettingsAck() error {
	f.startWrite(FrameSettings, FlagSettingsAck, 0)
	return f.endWrite()
}

type PingFrame struct {
	FrameHeader
	Data [8]byte
}

func (f *PingFrame) IsAck() bool { return f.Flags.Has(FlagPingAck) }

func parsePingFrame(_ *frameCache, fh FrameHeader, payload []byte) (Frame, error) {
	if len(payload) != 8 {
		return nil, ConnectionError(ErrCodeFrameSize)
	}
	if fh.StreamID != 0 {
		return nil, ConnectionError(ErrCodeProtocol)
	}
	f := &PingFrame{FrameHeader: fh}
	copy(f.Data[:], payload)
	return f, nil
}

func (f *Framer) WritePing(ack bool, data [8]byte) error {
	var flags Flags
	if ack {
		flags = FlagPingAck
	}
	f.startWrite(FramePing, flags, 0)
	f.writeBytes(data[:])
	return f.endWrite()
}

type GoAwayFrame struct {
	FrameHeader
	LastStreamID uint32
	ErrCode      ErrCode
	debugData    []byte
}

// DebugData returns any debug data in the GOAWAY frame. Its contents
func (f *GoAwayFrame) DebugData() []byte {
	f.checkValid()
	return f.debugData
}

func parseGoAwayFrame(_ *frameCache, fh FrameHeader, p []byte) (Frame, error) {
	if fh.StreamID != 0 {
		return nil, ConnectionError(ErrCodeProtocol)
	}
	if len(p) < 8 {
		return nil, ConnectionError(ErrCodeFrameSize)
	}
	return &GoAwayFrame{
		FrameHeader:  fh,
		LastStreamID: binary.BigEndian.Uint32(p[:4]) & (1<<31 - 1),
		ErrCode:      ErrCode(binary.BigEndian.Uint32(p[4:8])),
		debugData:    p[8:],
	}, nil
}

func (f *Framer) WriteGoAway(maxStreamID uint32, code ErrCode, debugData []byte) error {
	f.startWrite(FrameGoAway, 0, 0)
	f.writeUint32(maxStreamID & (1<<31 - 1))
	f.writeUint32(uint32(code))
	f.writeBytes(debugData)
	return f.endWrite()
}

// An UnknownFrame is the frame type returned when the frame type is unknown
// or no specific frame type parser exists.
type UnknownFrame struct {
	FrameHeader
	p []byte
}

// Payload returns the frame's payload (after the header).  It is not
// frame is read.
func (f *UnknownFrame) Payload() []byte {
	f.checkValid()
	return f.p
}

func parseUnknownFrame(_ *frameCache, fh FrameHeader, p []byte) (Frame, error) {
	return &UnknownFrame{fh, p}, nil
}

type WindowUpdateFrame struct {
	FrameHeader
	Increment uint32 // never read with high bit set
}

func parseWindowUpdateFrame(_ *frameCache, fh FrameHeader, p []byte) (Frame, error) {
	if len(p) != 4 {
		return nil, ConnectionError(ErrCodeFrameSize)
	}
	inc := binary.BigEndian.Uint32(p[:4]) & 0x7fffffff // mask off high reserved bit
	if inc == 0 {

		if fh.StreamID == 0 {
			return nil, ConnectionError(ErrCodeProtocol)
		}
		return nil, streamError(fh.StreamID, ErrCodeProtocol)
	}
	return &WindowUpdateFrame{
		FrameHeader: fh,
		Increment:   inc,
	}, nil
}

// WriteWindowUpdate writes a WINDOW_UPDATE frame.
func (f *Framer) WriteWindowUpdate(streamID, incr uint32) error {

	if (incr < 1 || incr > 2147483647) && !f.AllowIllegalWrites {
		return errors.New("illegal window increment value")
	}
	f.startWrite(FrameWindowUpdate, 0, streamID)
	f.writeUint32(incr)
	return f.endWrite()
}

type HeadersFrame struct {
	FrameHeader

	Priority PriorityParam

	headerFragBuf []byte // not owned
}

func (f *HeadersFrame) HeaderBlockFragment() []byte {
	f.checkValid()
	return f.headerFragBuf
}

func (f *HeadersFrame) HeadersEnded() bool {
	return f.FrameHeader.Flags.Has(FlagHeadersEndHeaders)
}

func (f *HeadersFrame) StreamEnded() bool {
	return f.FrameHeader.Flags.Has(FlagHeadersEndStream)
}

func (f *HeadersFrame) HasPriority() bool {
	return f.FrameHeader.Flags.Has(FlagHeadersPriority)
}

func parseHeadersFrame(_ *frameCache, fh FrameHeader, p []byte) (_ Frame, err error) {
	hf := &HeadersFrame{
		FrameHeader: fh,
	}
	if fh.StreamID == 0 {

		return nil, connError{ErrCodeProtocol, "HEADERS frame with stream ID 0"}
	}
	var padLength uint8
	if fh.Flags.Has(FlagHeadersPadded) {
		if p, padLength, err = readByte(p); err != nil {
			return
		}
	}
	if fh.Flags.Has(FlagHeadersPriority) {
		var v uint32
		p, v, err = readUint32(p)
		if err != nil {
			return nil, err
		}
		hf.Priority.StreamDep = v & 0x7fffffff
		hf.Priority.Exclusive = (v != hf.Priority.StreamDep) // high bit was set
		p, hf.Priority.Weight, err = readByte(p)
		if err != nil {
			return nil, err
		}
	}
	if len(p)-int(padLength) <= 0 {
		return nil, streamError(fh.StreamID, ErrCodeProtocol)
	}
	hf.headerFragBuf = p[:len(p)-int(padLength)]
	return hf, nil
}

// HeadersFrameParam are the parameters for writing a HEADERS frame.
type HeadersFrameParam struct {
	StreamID uint32

	BlockFragment []byte

	EndStream bool

	EndHeaders bool

	PadLength uint8

	Priority PriorityParam
}

// WriteHeaders writes a single HEADERS frame.
//
// splitting them into any necessary CONTINUATION frames is handled
//
func (f *Framer) WriteHeaders(p HeadersFrameParam) error {
	if !validStreamID(p.StreamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	var flags Flags
	if p.PadLength != 0 {
		flags |= FlagHeadersPadded
	}
	if p.EndStream {
		flags |= FlagHeadersEndStream
	}
	if p.EndHeaders {
		flags |= FlagHeadersEndHeaders
	}
	if !p.Priority.IsZero() {
		flags |= FlagHeadersPriority
	}
	f.startWrite(FrameHeaders, flags, p.StreamID)
	if p.PadLength != 0 {
		f.writeByte(p.PadLength)
	}
	if !p.Priority.IsZero() {
		v := p.Priority.StreamDep
		if !validStreamIDOrZero(v) && !f.AllowIllegalWrites {
			return errDepStreamID
		}
		if p.Priority.Exclusive {
			v |= 1 << 31
		}
		f.writeUint32(v)
		f.writeByte(p.Priority.Weight)
	}
	f.wbuf = append(f.wbuf, p.BlockFragment...)
	f.wbuf = append(f.wbuf, padZeros[:p.PadLength]...)
	return f.endWrite()
}

type PriorityFrame struct {
	FrameHeader
	PriorityParam
}

type PriorityParam struct {
	StreamDep uint32

	Exclusive bool

	Weight uint8
}

func (p PriorityParam) IsZero() bool {
	return p == PriorityParam{}
}

func parsePriorityFrame(_ *frameCache, fh FrameHeader, payload []byte) (Frame, error) {
	if fh.StreamID == 0 {
		return nil, connError{ErrCodeProtocol, "PRIORITY frame with stream ID 0"}
	}
	if len(payload) != 5 {
		return nil, connError{ErrCodeFrameSize, fmt.Sprintf("PRIORITY frame payload size was %d; want 5", len(payload))}
	}
	v := binary.BigEndian.Uint32(payload[:4])
	streamID := v & 0x7fffffff // mask off high bit
	return &PriorityFrame{
		FrameHeader: fh,
		PriorityParam: PriorityParam{
			Weight:    payload[4],
			StreamDep: streamID,
			Exclusive: streamID != v, // was high bit set?
		},
	}, nil
}

// WritePriority writes a PRIORITY frame.
//
func (f *Framer) WritePriority(streamID uint32, p PriorityParam) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	if !validStreamIDOrZero(p.StreamDep) {
		return errDepStreamID
	}
	f.startWrite(FramePriority, 0, streamID)
	v := p.StreamDep
	if p.Exclusive {
		v |= 1 << 31
	}
	f.writeUint32(v)
	f.writeByte(p.Weight)
	return f.endWrite()
}

type RSTStreamFrame struct {
	FrameHeader
	ErrCode ErrCode
}

func parseRSTStreamFrame(_ *frameCache, fh FrameHeader, p []byte) (Frame, error) {
	if len(p) != 4 {
		return nil, ConnectionError(ErrCodeFrameSize)
	}
	if fh.StreamID == 0 {
		return nil, ConnectionError(ErrCodeProtocol)
	}
	return &RSTStreamFrame{fh, ErrCode(binary.BigEndian.Uint32(p[:4]))}, nil
}

// WriteRSTStream writes a RST_STREAM frame.
//
func (f *Framer) WriteRSTStream(streamID uint32, code ErrCode) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	f.startWrite(FrameRSTStream, 0, streamID)
	f.writeUint32(uint32(code))
	return f.endWrite()
}

type ContinuationFrame struct {
	FrameHeader
	headerFragBuf []byte
}

func parseContinuationFrame(_ *frameCache, fh FrameHeader, p []byte) (Frame, error) {
	if fh.StreamID == 0 {
		return nil, connError{ErrCodeProtocol, "CONTINUATION frame with stream ID 0"}
	}
	return &ContinuationFrame{fh, p}, nil
}

func (f *ContinuationFrame) HeaderBlockFragment() []byte {
	f.checkValid()
	return f.headerFragBuf
}

func (f *ContinuationFrame) HeadersEnded() bool {
	return f.FrameHeader.Flags.Has(FlagContinuationEndHeaders)
}

// WriteContinuation writes a CONTINUATION frame.
//
func (f *Framer) WriteContinuation(streamID uint32, endHeaders bool, headerBlockFragment []byte) error {
	if !validStreamID(streamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	var flags Flags
	if endHeaders {
		flags |= FlagContinuationEndHeaders
	}
	f.startWrite(FrameContinuation, flags, streamID)
	f.wbuf = append(f.wbuf, headerBlockFragment...)
	return f.endWrite()
}

type PushPromiseFrame struct {
	FrameHeader
	PromiseID     uint32
	headerFragBuf []byte // not owned
}

func (f *PushPromiseFrame) HeaderBlockFragment() []byte {
	f.checkValid()
	return f.headerFragBuf
}

func (f *PushPromiseFrame) HeadersEnded() bool {
	return f.FrameHeader.Flags.Has(FlagPushPromiseEndHeaders)
}

func parsePushPromise(_ *frameCache, fh FrameHeader, p []byte) (_ Frame, err error) {
	pp := &PushPromiseFrame{
		FrameHeader: fh,
	}
	if pp.StreamID == 0 {

		return nil, ConnectionError(ErrCodeProtocol)
	}

	var padLength uint8
	if fh.Flags.Has(FlagPushPromisePadded) {
		if p, padLength, err = readByte(p); err != nil {
			return
		}
	}

	p, pp.PromiseID, err = readUint32(p)
	if err != nil {
		return
	}
	pp.PromiseID = pp.PromiseID & (1<<31 - 1)

	if int(padLength) > len(p) {

		return nil, ConnectionError(ErrCodeProtocol)
	}
	pp.headerFragBuf = p[:len(p)-int(padLength)]
	return pp, nil
}

// PushPromiseParam are the parameters for writing a PUSH_PROMISE frame.
type PushPromiseParam struct {
	StreamID uint32

	PromiseID uint32

	BlockFragment []byte

	EndHeaders bool

	PadLength uint8
}

//
// individual frames. Continuation frames are handled elsewhere.
//
func (f *Framer) WritePushPromise(p PushPromiseParam) error {
	if !validStreamID(p.StreamID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	var flags Flags
	if p.PadLength != 0 {
		flags |= FlagPushPromisePadded
	}
	if p.EndHeaders {
		flags |= FlagPushPromiseEndHeaders
	}
	f.startWrite(FramePushPromise, flags, p.StreamID)
	if p.PadLength != 0 {
		f.writeByte(p.PadLength)
	}
	if !validStreamID(p.PromiseID) && !f.AllowIllegalWrites {
		return errStreamID
	}
	f.writeUint32(p.PromiseID)
	f.wbuf = append(f.wbuf, p.BlockFragment...)
	f.wbuf = append(f.wbuf, padZeros[:p.PadLength]...)
	return f.endWrite()
}

// WriteRawFrame writes a raw frame. This can be used to write
// extension frames unknown to this package.
func (f *Framer) WriteRawFrame(t FrameType, flags Flags, streamID uint32, payload []byte) error {
	f.startWrite(t, flags, streamID)
	f.writeBytes(payload)
	return f.endWrite()
}

func readByte(p []byte) (remain []byte, b byte, err error) {
	if len(p) == 0 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	return p[1:], p[0], nil
}

func readUint32(p []byte) (remain []byte, v uint32, err error) {
	if len(p) < 4 {
		return nil, 0, io.ErrUnexpectedEOF
	}
	return p[4:], binary.BigEndian.Uint32(p[:4]), nil
}

type streamEnder interface {
	StreamEnded() bool
}

type headersEnder interface {
	HeadersEnded() bool
}

type headersOrContinuation interface {
	headersEnder
	HeaderBlockFragment() []byte
}

// A MetaHeadersFrame is the representation of one HEADERS frame and
// zero or more contiguous CONTINUATION frames and the decoding of
//
// This type of frame does not appear on the wire and is only returned
type MetaHeadersFrame struct {
	*HeadersFrame

	//

	Fields []hpack.HeaderField

	Truncated bool
}

func (mh *MetaHeadersFrame) PseudoValue(pseudo string) string {
	for _, hf := range mh.Fields {
		if !hf.IsPseudo() {
			return ""
		}
		if hf.Name[1:] == pseudo {
			return hf.Value
		}
	}
	return ""
}

func (mh *MetaHeadersFrame) RegularFields() []hpack.HeaderField {
	for i, hf := range mh.Fields {
		if !hf.IsPseudo() {
			return mh.Fields[i:]
		}
	}
	return nil
}

func (mh *MetaHeadersFrame) PseudoFields() []hpack.HeaderField {
	for i, hf := range mh.Fields {
		if !hf.IsPseudo() {
			return mh.Fields[:i]
		}
	}
	return mh.Fields
}

func (mh *MetaHeadersFrame) checkPseudos() error {
	var isRequest, isResponse bool
	pf := mh.PseudoFields()
	for i, hf := range pf {
		switch hf.Name {
		case ":method", ":path", ":scheme", ":authority":
			isRequest = true
		case ":status":
			isResponse = true
		default:
			return pseudoHeaderError(hf.Name)
		}

		for _, hf2 := range pf[:i] {
			if hf.Name == hf2.Name {
				return duplicatePseudoHeaderError(hf.Name)
			}
		}
	}
	if isRequest && isResponse {
		return errMixPseudoHeaderTypes
	}
	return nil
}

func (fr *Framer) maxHeaderStringLen() int {
	v := fr.maxHeaderListSize()
	if uint32(int(v)) == v {
		return int(v)
	}

	return 0
}

// readMetaFrame returns 0 or more CONTINUATION frames from fr and
func (fr *Framer) readMetaFrame(hf *HeadersFrame) (*MetaHeadersFrame, error) {
	if fr.AllowIllegalReads {
		return nil, errors.New("illegal use of AllowIllegalReads with ReadMetaHeaders")
	}
	mh := &MetaHeadersFrame{
		HeadersFrame: hf,
	}
	var remainSize = fr.maxHeaderListSize()
	var sawRegular bool

	var invalid error // pseudo header field errors
	hdec := fr.ReadMetaHeaders
	hdec.SetEmitEnabled(true)
	hdec.SetMaxStringLength(fr.maxHeaderStringLen())
	hdec.SetEmitFunc(func(hf hpack.HeaderField) {
		if VerboseLogs && fr.logReads {
			fr.debugReadLoggerf("http2: decoded hpack field %+v", hf)
		}
		if !httplex.ValidHeaderFieldValue(hf.Value) {
			invalid = headerFieldValueError(hf.Value)
		}
		isPseudo := strings.HasPrefix(hf.Name, ":")
		if isPseudo {
			if sawRegular {
				invalid = errPseudoAfterRegular
			}
		} else {
			sawRegular = true
			if !validWireHeaderFieldName(hf.Name) {
				invalid = headerFieldNameError(hf.Name)
			}
		}

		if invalid != nil {
			hdec.SetEmitEnabled(false)
			return
		}

		size := hf.Size()
		if size > remainSize {
			hdec.SetEmitEnabled(false)
			mh.Truncated = true
			return
		}
		remainSize -= size

		mh.Fields = append(mh.Fields, hf)
	})

	defer hdec.SetEmitFunc(func(hf hpack.HeaderField) {})

	var hc headersOrContinuation = hf
	for {
		frag := hc.HeaderBlockFragment()
		if _, err := hdec.Write(frag); err != nil {
			return nil, ConnectionError(ErrCodeCompression)
		}

		if hc.HeadersEnded() {
			break
		}
		if f, err := fr.ReadFrame(); err != nil {
			return nil, err
		} else {
			hc = f.(*ContinuationFrame) // guaranteed by checkFrameOrder
		}
	}

	mh.HeadersFrame.headerFragBuf = nil
	mh.HeadersFrame.invalidate()

	if err := hdec.Close(); err != nil {
		return nil, ConnectionError(ErrCodeCompression)
	}
	if invalid != nil {
		fr.errDetail = invalid
		if VerboseLogs {
			log.Printf("http2: invalid header: %v", invalid)
		}
		return nil, StreamError{mh.StreamID, ErrCodeProtocol, invalid}
	}
	if err := mh.checkPseudos(); err != nil {
		fr.errDetail = err
		if VerboseLogs {
			log.Printf("http2: invalid pseudo headers: %v", err)
		}
		return nil, StreamError{mh.StreamID, ErrCodeProtocol, err}
	}
	return mh, nil
}

func summarizeFrame(f Frame) string {
	var buf bytes.Buffer
	f.Header().writeDebug(&buf)
	switch f := f.(type) {
	case *SettingsFrame:
		n := 0
		f.ForeachSetting(func(s Setting) error {
			n++
			if n == 1 {
				buf.WriteString(", settings:")
			}
			fmt.Fprintf(&buf, " %v=%v,", s.ID, s.Val)
			return nil
		})
		if n > 0 {
			buf.Truncate(buf.Len() - 1) // remove trailing comma
		}
	case *DataFrame:
		data := f.Data()
		const max = 256
		if len(data) > max {
			data = data[:max]
		}
		fmt.Fprintf(&buf, " data=%q", data)
		if len(f.Data()) > max {
			fmt.Fprintf(&buf, " (%d bytes omitted)", len(f.Data())-max)
		}
	case *WindowUpdateFrame:
		if f.StreamID == 0 {
			buf.WriteString(" (conn)")
		}
		fmt.Fprintf(&buf, " incr=%v", f.Increment)
	case *PingFrame:
		fmt.Fprintf(&buf, " ping=%q", f.Data[:])
	case *GoAwayFrame:
		fmt.Fprintf(&buf, " LastStreamID=%v ErrCode=%v Debug=%q",
			f.LastStreamID, f.ErrCode, f.debugData)
	case *RSTStreamFrame:
		fmt.Fprintf(&buf, " ErrCode=%v", f.ErrCode)
	}
	return buf.String()
}
