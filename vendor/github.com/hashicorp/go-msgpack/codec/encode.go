// Copyright (c) 2012, 2013 Ugorji Nwoke. All rights reserved.

package codec

import (
	"io"
	"reflect"
)

const (
	msgTagEnc         = "codec.encoder"
	defEncByteBufSize = 1 << 6 // 4:16, 6:64, 8:256, 10:1024

)

type AsSymbolFlag uint8

const (
	AsSymbolDefault AsSymbolFlag = iota

	AsSymbolAll = 0xfe

	AsSymbolNone = 1 << iota

	AsSymbolMapStringKeysFlag

	AsSymbolStructFieldNameFlag
)

type encWriter interface {
	writeUint16(uint16)
	writeUint32(uint32)
	writeUint64(uint64)
	writeb([]byte)
	writestr(string)
	writen1(byte)
	writen2(byte, byte)
	atEndOfEncode()
}

type encDriver interface {
	isBuiltinType(rt uintptr) bool
	encodeBuiltin(rt uintptr, v interface{})
	encodeNil()
	encodeInt(i int64)
	encodeUint(i uint64)
	encodeBool(b bool)
	encodeFloat32(f float32)
	encodeFloat64(f float64)
	encodeExtPreamble(xtag byte, length int)
	encodeArrayPreamble(length int)
	encodeMapPreamble(length int)
	encodeString(c charEncoding, v string)
	encodeSymbol(v string)
	encodeStringBytes(c charEncoding, v []byte)
}

type ioEncWriterWriter interface {
	WriteByte(c byte) error
	WriteString(s string) (n int, err error)
	Write(p []byte) (n int, err error)
}

type ioEncStringWriter interface {
	WriteString(s string) (n int, err error)
}

type EncodeOptions struct {
	StructToArray bool

	//

	//

	//

	AsSymbols AsSymbolFlag
}

type simpleIoEncWriterWriter struct {
	w  io.Writer
	bw io.ByteWriter
	sw ioEncStringWriter
}

func (o *simpleIoEncWriterWriter) WriteByte(c byte) (err error) {
	if o.bw != nil {
		return o.bw.WriteByte(c)
	}
	_, err = o.w.Write([]byte{c})
	return
}

func (o *simpleIoEncWriterWriter) WriteString(s string) (n int, err error) {
	if o.sw != nil {
		return o.sw.WriteString(s)
	}
	return o.w.Write([]byte(s))
}

func (o *simpleIoEncWriterWriter) Write(p []byte) (n int, err error) {
	return o.w.Write(p)
}

type ioEncWriter struct {
	w ioEncWriterWriter
	x [8]byte // temp byte array re-used internally for efficiency
}

func (z *ioEncWriter) writeUint16(v uint16) {
	bigen.PutUint16(z.x[:2], v)
	z.writeb(z.x[:2])
}

func (z *ioEncWriter) writeUint32(v uint32) {
	bigen.PutUint32(z.x[:4], v)
	z.writeb(z.x[:4])
}

func (z *ioEncWriter) writeUint64(v uint64) {
	bigen.PutUint64(z.x[:8], v)
	z.writeb(z.x[:8])
}

func (z *ioEncWriter) writeb(bs []byte) {
	if len(bs) == 0 {
		return
	}
	n, err := z.w.Write(bs)
	if err != nil {
		panic(err)
	}
	if n != len(bs) {
		encErr("write: Incorrect num bytes written. Expecting: %v, Wrote: %v", len(bs), n)
	}
}

func (z *ioEncWriter) writestr(s string) {
	n, err := z.w.WriteString(s)
	if err != nil {
		panic(err)
	}
	if n != len(s) {
		encErr("write: Incorrect num bytes written. Expecting: %v, Wrote: %v", len(s), n)
	}
}

func (z *ioEncWriter) writen1(b byte) {
	if err := z.w.WriteByte(b); err != nil {
		panic(err)
	}
}

func (z *ioEncWriter) writen2(b1 byte, b2 byte) {
	z.writen1(b1)
	z.writen1(b2)
}

func (z *ioEncWriter) atEndOfEncode() {}

type bytesEncWriter struct {
	b   []byte
	c   int     // cursor
	out *[]byte // write out on atEndOfEncode
}

func (z *bytesEncWriter) writeUint16(v uint16) {
	c := z.grow(2)
	z.b[c] = byte(v >> 8)
	z.b[c+1] = byte(v)
}

func (z *bytesEncWriter) writeUint32(v uint32) {
	c := z.grow(4)
	z.b[c] = byte(v >> 24)
	z.b[c+1] = byte(v >> 16)
	z.b[c+2] = byte(v >> 8)
	z.b[c+3] = byte(v)
}

func (z *bytesEncWriter) writeUint64(v uint64) {
	c := z.grow(8)
	z.b[c] = byte(v >> 56)
	z.b[c+1] = byte(v >> 48)
	z.b[c+2] = byte(v >> 40)
	z.b[c+3] = byte(v >> 32)
	z.b[c+4] = byte(v >> 24)
	z.b[c+5] = byte(v >> 16)
	z.b[c+6] = byte(v >> 8)
	z.b[c+7] = byte(v)
}

func (z *bytesEncWriter) writeb(s []byte) {
	if len(s) == 0 {
		return
	}
	c := z.grow(len(s))
	copy(z.b[c:], s)
}

func (z *bytesEncWriter) writestr(s string) {
	c := z.grow(len(s))
	copy(z.b[c:], s)
}

func (z *bytesEncWriter) writen1(b1 byte) {
	c := z.grow(1)
	z.b[c] = b1
}

func (z *bytesEncWriter) writen2(b1 byte, b2 byte) {
	c := z.grow(2)
	z.b[c] = b1
	z.b[c+1] = b2
}

func (z *bytesEncWriter) atEndOfEncode() {
	*(z.out) = z.b[:z.c]
}

func (z *bytesEncWriter) grow(n int) (oldcursor int) {
	oldcursor = z.c
	z.c = oldcursor + n
	if z.c > cap(z.b) {

		bs := make([]byte, 2*cap(z.b)+n)
		copy(bs, z.b[:oldcursor])
		z.b = bs
	} else if z.c > len(z.b) {
		z.b = z.b[:cap(z.b)]
	}
	return
}

type encFnInfo struct {
	ti    *typeInfo
	e     *Encoder
	ee    encDriver
	xfFn  func(reflect.Value) ([]byte, error)
	xfTag byte
}

func (f *encFnInfo) builtin(rv reflect.Value) {
	f.ee.encodeBuiltin(f.ti.rtid, rv.Interface())
}

func (f *encFnInfo) rawExt(rv reflect.Value) {
	f.e.encRawExt(rv.Interface().(RawExt))
}

func (f *encFnInfo) ext(rv reflect.Value) {
	bs, fnerr := f.xfFn(rv)
	if fnerr != nil {
		panic(fnerr)
	}
	if bs == nil {
		f.ee.encodeNil()
		return
	}
	if f.e.hh.writeExt() {
		f.ee.encodeExtPreamble(f.xfTag, len(bs))
		f.e.w.writeb(bs)
	} else {
		f.ee.encodeStringBytes(c_RAW, bs)
	}

}

func (f *encFnInfo) binaryMarshal(rv reflect.Value) {
	var bm binaryMarshaler
	if f.ti.mIndir == 0 {
		bm = rv.Interface().(binaryMarshaler)
	} else if f.ti.mIndir == -1 {
		bm = rv.Addr().Interface().(binaryMarshaler)
	} else {
		for j, k := int8(0), f.ti.mIndir; j < k; j++ {
			if rv.IsNil() {
				f.ee.encodeNil()
				return
			}
			rv = rv.Elem()
		}
		bm = rv.Interface().(binaryMarshaler)
	}

	bs, fnerr := bm.MarshalBinary()
	if fnerr != nil {
		panic(fnerr)
	}
	if bs == nil {
		f.ee.encodeNil()
	} else {
		f.ee.encodeStringBytes(c_RAW, bs)
	}
}

func (f *encFnInfo) kBool(rv reflect.Value) {
	f.ee.encodeBool(rv.Bool())
}

func (f *encFnInfo) kString(rv reflect.Value) {
	f.ee.encodeString(c_UTF8, rv.String())
}

func (f *encFnInfo) kFloat64(rv reflect.Value) {
	f.ee.encodeFloat64(rv.Float())
}

func (f *encFnInfo) kFloat32(rv reflect.Value) {
	f.ee.encodeFloat32(float32(rv.Float()))
}

func (f *encFnInfo) kInt(rv reflect.Value) {
	f.ee.encodeInt(rv.Int())
}

func (f *encFnInfo) kUint(rv reflect.Value) {
	f.ee.encodeUint(rv.Uint())
}

func (f *encFnInfo) kInvalid(rv reflect.Value) {
	f.ee.encodeNil()
}

func (f *encFnInfo) kErr(rv reflect.Value) {
	encErr("Unsupported kind: %s, for: %#v", rv.Kind(), rv)
}

func (f *encFnInfo) kSlice(rv reflect.Value) {
	if rv.IsNil() {
		f.ee.encodeNil()
		return
	}

	if shortCircuitReflectToFastPath {
		switch f.ti.rtid {
		case intfSliceTypId:
			f.e.encSliceIntf(rv.Interface().([]interface{}))
			return
		case strSliceTypId:
			f.e.encSliceStr(rv.Interface().([]string))
			return
		case uint64SliceTypId:
			f.e.encSliceUint64(rv.Interface().([]uint64))
			return
		case int64SliceTypId:
			f.e.encSliceInt64(rv.Interface().([]int64))
			return
		}
	}

	if f.ti.rtid == uint8SliceTypId || f.ti.rt.Elem().Kind() == reflect.Uint8 {
		f.ee.encodeStringBytes(c_RAW, rv.Bytes())
		return
	}

	l := rv.Len()
	if f.ti.mbs {
		if l%2 == 1 {
			encErr("mapBySlice: invalid length (must be divisible by 2): %v", l)
		}
		f.ee.encodeMapPreamble(l / 2)
	} else {
		f.ee.encodeArrayPreamble(l)
	}
	if l == 0 {
		return
	}
	for j := 0; j < l; j++ {

		f.e.encodeValue(rv.Index(j))
	}
}

func (f *encFnInfo) kArray(rv reflect.Value) {

	l := rv.Len()

	if f.ti.rt.Elem().Kind() == reflect.Uint8 {
		if l == 0 {
			f.ee.encodeStringBytes(c_RAW, nil)
			return
		}
		var bs []byte
		if rv.CanAddr() {
			bs = rv.Slice(0, l).Bytes()
		} else {
			bs = make([]byte, l)
			for i := 0; i < l; i++ {
				bs[i] = byte(rv.Index(i).Uint())
			}
		}
		f.ee.encodeStringBytes(c_RAW, bs)
		return
	}

	if f.ti.mbs {
		if l%2 == 1 {
			encErr("mapBySlice: invalid length (must be divisible by 2): %v", l)
		}
		f.ee.encodeMapPreamble(l / 2)
	} else {
		f.ee.encodeArrayPreamble(l)
	}
	if l == 0 {
		return
	}
	for j := 0; j < l; j++ {

		f.e.encodeValue(rv.Index(j))
	}
}

func (f *encFnInfo) kStruct(rv reflect.Value) {
	fti := f.ti
	newlen := len(fti.sfi)
	rvals := make([]reflect.Value, newlen)
	var encnames []string
	e := f.e
	tisfi := fti.sfip
	toMap := !(fti.toArray || e.h.StructToArray)

	if toMap {
		tisfi = fti.sfi
		encnames = make([]string, newlen)
	}
	newlen = 0
	for _, si := range tisfi {
		if si.i != -1 {
			rvals[newlen] = rv.Field(int(si.i))
		} else {
			rvals[newlen] = rv.FieldByIndex(si.is)
		}
		if toMap {
			if si.omitEmpty && isEmptyValue(rvals[newlen]) {
				continue
			}
			encnames[newlen] = si.encName
		} else {
			if si.omitEmpty && isEmptyValue(rvals[newlen]) {
				rvals[newlen] = reflect.Value{} //encode as nil
			}
		}
		newlen++
	}

	if toMap {
		ee := f.ee //don't dereference everytime
		ee.encodeMapPreamble(newlen)

		asSymbols := e.h.AsSymbols == AsSymbolDefault || e.h.AsSymbols&AsSymbolStructFieldNameFlag != 0
		for j := 0; j < newlen; j++ {
			if asSymbols {
				ee.encodeSymbol(encnames[j])
			} else {
				ee.encodeString(c_UTF8, encnames[j])
			}
			e.encodeValue(rvals[j])
		}
	} else {
		f.ee.encodeArrayPreamble(newlen)
		for j := 0; j < newlen; j++ {
			e.encodeValue(rvals[j])
		}
	}
}

func (f *encFnInfo) kInterface(rv reflect.Value) {
	if rv.IsNil() {
		f.ee.encodeNil()
		return
	}
	f.e.encodeValue(rv.Elem())
}

func (f *encFnInfo) kMap(rv reflect.Value) {
	if rv.IsNil() {
		f.ee.encodeNil()
		return
	}

	if shortCircuitReflectToFastPath {
		switch f.ti.rtid {
		case mapIntfIntfTypId:
			f.e.encMapIntfIntf(rv.Interface().(map[interface{}]interface{}))
			return
		case mapStrIntfTypId:
			f.e.encMapStrIntf(rv.Interface().(map[string]interface{}))
			return
		case mapStrStrTypId:
			f.e.encMapStrStr(rv.Interface().(map[string]string))
			return
		case mapInt64IntfTypId:
			f.e.encMapInt64Intf(rv.Interface().(map[int64]interface{}))
			return
		case mapUint64IntfTypId:
			f.e.encMapUint64Intf(rv.Interface().(map[uint64]interface{}))
			return
		}
	}

	l := rv.Len()
	f.ee.encodeMapPreamble(l)
	if l == 0 {
		return
	}

	keyTypeIsString := f.ti.rt.Key() == stringTyp
	var asSymbols bool
	if keyTypeIsString {
		asSymbols = f.e.h.AsSymbols&AsSymbolMapStringKeysFlag != 0
	}
	mks := rv.MapKeys()

	for j := range mks {
		if keyTypeIsString {
			if asSymbols {
				f.ee.encodeSymbol(mks[j].String())
			} else {
				f.ee.encodeString(c_UTF8, mks[j].String())
			}
		} else {
			f.e.encodeValue(mks[j])
		}
		f.e.encodeValue(rv.MapIndex(mks[j]))
	}

}

type encFn struct {
	i *encFnInfo
	f func(*encFnInfo, reflect.Value)
}

type Encoder struct {
	w  encWriter
	e  encDriver
	h  *BasicHandle
	hh Handle
	f  map[uintptr]encFn
	x  []uintptr
	s  []encFn
}

//
func NewEncoder(w io.Writer, h Handle) *Encoder {
	ww, ok := w.(ioEncWriterWriter)
	if !ok {
		sww := simpleIoEncWriterWriter{w: w}
		sww.bw, _ = w.(io.ByteWriter)
		sww.sw, _ = w.(ioEncStringWriter)
		ww = &sww

	}
	z := ioEncWriter{
		w: ww,
	}
	return &Encoder{w: &z, hh: h, h: h.getBasicHandle(), e: h.newEncDriver(&z)}
}

//
func NewEncoderBytes(out *[]byte, h Handle) *Encoder {
	in := *out
	if in == nil {
		in = make([]byte, defEncByteBufSize)
	}
	z := bytesEncWriter{
		b:   in,
		out: out,
	}
	return &Encoder{w: &z, hh: h, h: h.getBasicHandle(), e: h.newEncDriver(&z)}
}

//
//
//
//
//
//
//
//
//
//
//
//
//
//
func (e *Encoder) Encode(v interface{}) (err error) {
	defer panicToErr(&err)
	e.encode(v)
	e.w.atEndOfEncode()
	return
}

func (e *Encoder) encode(iv interface{}) {
	switch v := iv.(type) {
	case nil:
		e.e.encodeNil()

	case reflect.Value:
		e.encodeValue(v)

	case string:
		e.e.encodeString(c_UTF8, v)
	case bool:
		e.e.encodeBool(v)
	case int:
		e.e.encodeInt(int64(v))
	case int8:
		e.e.encodeInt(int64(v))
	case int16:
		e.e.encodeInt(int64(v))
	case int32:
		e.e.encodeInt(int64(v))
	case int64:
		e.e.encodeInt(v)
	case uint:
		e.e.encodeUint(uint64(v))
	case uint8:
		e.e.encodeUint(uint64(v))
	case uint16:
		e.e.encodeUint(uint64(v))
	case uint32:
		e.e.encodeUint(uint64(v))
	case uint64:
		e.e.encodeUint(v)
	case float32:
		e.e.encodeFloat32(v)
	case float64:
		e.e.encodeFloat64(v)

	case []interface{}:
		e.encSliceIntf(v)
	case []string:
		e.encSliceStr(v)
	case []int64:
		e.encSliceInt64(v)
	case []uint64:
		e.encSliceUint64(v)
	case []uint8:
		e.e.encodeStringBytes(c_RAW, v)

	case map[interface{}]interface{}:
		e.encMapIntfIntf(v)
	case map[string]interface{}:
		e.encMapStrIntf(v)
	case map[string]string:
		e.encMapStrStr(v)
	case map[int64]interface{}:
		e.encMapInt64Intf(v)
	case map[uint64]interface{}:
		e.encMapUint64Intf(v)

	case *string:
		e.e.encodeString(c_UTF8, *v)
	case *bool:
		e.e.encodeBool(*v)
	case *int:
		e.e.encodeInt(int64(*v))
	case *int8:
		e.e.encodeInt(int64(*v))
	case *int16:
		e.e.encodeInt(int64(*v))
	case *int32:
		e.e.encodeInt(int64(*v))
	case *int64:
		e.e.encodeInt(*v)
	case *uint:
		e.e.encodeUint(uint64(*v))
	case *uint8:
		e.e.encodeUint(uint64(*v))
	case *uint16:
		e.e.encodeUint(uint64(*v))
	case *uint32:
		e.e.encodeUint(uint64(*v))
	case *uint64:
		e.e.encodeUint(*v)
	case *float32:
		e.e.encodeFloat32(*v)
	case *float64:
		e.e.encodeFloat64(*v)

	case *[]interface{}:
		e.encSliceIntf(*v)
	case *[]string:
		e.encSliceStr(*v)
	case *[]int64:
		e.encSliceInt64(*v)
	case *[]uint64:
		e.encSliceUint64(*v)
	case *[]uint8:
		e.e.encodeStringBytes(c_RAW, *v)

	case *map[interface{}]interface{}:
		e.encMapIntfIntf(*v)
	case *map[string]interface{}:
		e.encMapStrIntf(*v)
	case *map[string]string:
		e.encMapStrStr(*v)
	case *map[int64]interface{}:
		e.encMapInt64Intf(*v)
	case *map[uint64]interface{}:
		e.encMapUint64Intf(*v)

	default:
		e.encodeValue(reflect.ValueOf(iv))
	}
}

func (e *Encoder) encodeValue(rv reflect.Value) {
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			e.e.encodeNil()
			return
		}
		rv = rv.Elem()
	}

	rt := rv.Type()
	rtid := reflect.ValueOf(rt).Pointer()

	var fn encFn
	var ok bool
	if useMapForCodecCache {
		fn, ok = e.f[rtid]
	} else {
		for i, v := range e.x {
			if v == rtid {
				fn, ok = e.s[i], true
				break
			}
		}
	}
	if !ok {

		fi := encFnInfo{ti: getTypeInfo(rtid, rt), e: e, ee: e.e}
		fn.i = &fi
		if rtid == rawExtTypId {
			fn.f = (*encFnInfo).rawExt
		} else if e.e.isBuiltinType(rtid) {
			fn.f = (*encFnInfo).builtin
		} else if xfTag, xfFn := e.h.getEncodeExt(rtid); xfFn != nil {
			fi.xfTag, fi.xfFn = xfTag, xfFn
			fn.f = (*encFnInfo).ext
		} else if supportBinaryMarshal && fi.ti.m {
			fn.f = (*encFnInfo).binaryMarshal
		} else {
			switch rk := rt.Kind(); rk {
			case reflect.Bool:
				fn.f = (*encFnInfo).kBool
			case reflect.String:
				fn.f = (*encFnInfo).kString
			case reflect.Float64:
				fn.f = (*encFnInfo).kFloat64
			case reflect.Float32:
				fn.f = (*encFnInfo).kFloat32
			case reflect.Int, reflect.Int8, reflect.Int64, reflect.Int32, reflect.Int16:
				fn.f = (*encFnInfo).kInt
			case reflect.Uint8, reflect.Uint64, reflect.Uint, reflect.Uint32, reflect.Uint16:
				fn.f = (*encFnInfo).kUint
			case reflect.Invalid:
				fn.f = (*encFnInfo).kInvalid
			case reflect.Slice:
				fn.f = (*encFnInfo).kSlice
			case reflect.Array:
				fn.f = (*encFnInfo).kArray
			case reflect.Struct:
				fn.f = (*encFnInfo).kStruct

			case reflect.Interface:
				fn.f = (*encFnInfo).kInterface
			case reflect.Map:
				fn.f = (*encFnInfo).kMap
			default:
				fn.f = (*encFnInfo).kErr
			}
		}
		if useMapForCodecCache {
			if e.f == nil {
				e.f = make(map[uintptr]encFn, 16)
			}
			e.f[rtid] = fn
		} else {
			e.s = append(e.s, fn)
			e.x = append(e.x, rtid)
		}
	}

	fn.f(fn.i, rv)

}

func (e *Encoder) encRawExt(re RawExt) {
	if re.Data == nil {
		e.e.encodeNil()
		return
	}
	if e.hh.writeExt() {
		e.e.encodeExtPreamble(re.Tag, len(re.Data))
		e.w.writeb(re.Data)
	} else {
		e.e.encodeStringBytes(c_RAW, re.Data)
	}
}

func (e *Encoder) encSliceIntf(v []interface{}) {
	e.e.encodeArrayPreamble(len(v))
	for _, v2 := range v {
		e.encode(v2)
	}
}

func (e *Encoder) encSliceStr(v []string) {
	e.e.encodeArrayPreamble(len(v))
	for _, v2 := range v {
		e.e.encodeString(c_UTF8, v2)
	}
}

func (e *Encoder) encSliceInt64(v []int64) {
	e.e.encodeArrayPreamble(len(v))
	for _, v2 := range v {
		e.e.encodeInt(v2)
	}
}

func (e *Encoder) encSliceUint64(v []uint64) {
	e.e.encodeArrayPreamble(len(v))
	for _, v2 := range v {
		e.e.encodeUint(v2)
	}
}

func (e *Encoder) encMapStrStr(v map[string]string) {
	e.e.encodeMapPreamble(len(v))
	asSymbols := e.h.AsSymbols&AsSymbolMapStringKeysFlag != 0
	for k2, v2 := range v {
		if asSymbols {
			e.e.encodeSymbol(k2)
		} else {
			e.e.encodeString(c_UTF8, k2)
		}
		e.e.encodeString(c_UTF8, v2)
	}
}

func (e *Encoder) encMapStrIntf(v map[string]interface{}) {
	e.e.encodeMapPreamble(len(v))
	asSymbols := e.h.AsSymbols&AsSymbolMapStringKeysFlag != 0
	for k2, v2 := range v {
		if asSymbols {
			e.e.encodeSymbol(k2)
		} else {
			e.e.encodeString(c_UTF8, k2)
		}
		e.encode(v2)
	}
}

func (e *Encoder) encMapInt64Intf(v map[int64]interface{}) {
	e.e.encodeMapPreamble(len(v))
	for k2, v2 := range v {
		e.e.encodeInt(k2)
		e.encode(v2)
	}
}

func (e *Encoder) encMapUint64Intf(v map[uint64]interface{}) {
	e.e.encodeMapPreamble(len(v))
	for k2, v2 := range v {
		e.e.encodeUint(uint64(k2))
		e.encode(v2)
	}
}

func (e *Encoder) encMapIntfIntf(v map[interface{}]interface{}) {
	e.e.encodeMapPreamble(len(v))
	for k2, v2 := range v {
		e.encode(k2)
		e.encode(v2)
	}
}

func encErr(format string, params ...interface{}) {
	doPanic(msgTagEnc, format, params...)
}
