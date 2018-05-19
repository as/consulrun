// Copyright (c) 2012, 2013 Ugorji Nwoke. All rights reserved.

package codec

import (
	"io"
	"reflect"
)

const (
	msgTagDec             = "codec.decoder"
	msgBadDesc            = "Unrecognized descriptor byte"
	msgDecCannotExpandArr = "cannot expand go array from %v to stream length: %v"
)

type decReader interface {
	readn(n int) []byte
	readb([]byte)
	readn1() uint8
	readUint16() uint16
	readUint32() uint32
	readUint64() uint64
}

type decDriver interface {
	initReadNext()
	tryDecodeAsNil() bool
	currentEncodedType() valueType
	isBuiltinType(rt uintptr) bool
	decodeBuiltin(rt uintptr, v interface{})

	decodeNaked() (v interface{}, vt valueType, decodeFurther bool)
	decodeInt(bitsize uint8) (i int64)
	decodeUint(bitsize uint8) (ui uint64)
	decodeFloat(chkOverflow32 bool) (f float64)
	decodeBool() (b bool)

	decodeString() (s string)
	decodeBytes(bs []byte) (bsOut []byte, changed bool)
	decodeExt(verifyTag bool, tag byte) (xtag byte, xbs []byte)
	readMapLen() int
	readArrayLen() int
}

type DecodeOptions struct {
	MapType reflect.Type

	SliceType reflect.Type

	ErrorIfNoField bool
}

type ioDecReader struct {
	r  io.Reader
	br io.ByteReader
	x  [8]byte //temp byte array re-used internally for efficiency
}

func (z *ioDecReader) readn(n int) (bs []byte) {
	if n <= 0 {
		return
	}
	bs = make([]byte, n)
	if _, err := io.ReadAtLeast(z.r, bs, n); err != nil {
		panic(err)
	}
	return
}

func (z *ioDecReader) readb(bs []byte) {
	if _, err := io.ReadAtLeast(z.r, bs, len(bs)); err != nil {
		panic(err)
	}
}

func (z *ioDecReader) readn1() uint8 {
	if z.br != nil {
		b, err := z.br.ReadByte()
		if err != nil {
			panic(err)
		}
		return b
	}
	z.readb(z.x[:1])
	return z.x[0]
}

func (z *ioDecReader) readUint16() uint16 {
	z.readb(z.x[:2])
	return bigen.Uint16(z.x[:2])
}

func (z *ioDecReader) readUint32() uint32 {
	z.readb(z.x[:4])
	return bigen.Uint32(z.x[:4])
}

func (z *ioDecReader) readUint64() uint64 {
	z.readb(z.x[:8])
	return bigen.Uint64(z.x[:8])
}

type bytesDecReader struct {
	b []byte // data
	c int    // cursor
	a int    // available
}

func (z *bytesDecReader) consume(n int) (oldcursor int) {
	if z.a == 0 {
		panic(io.EOF)
	}
	if n > z.a {
		decErr("Trying to read %v bytes. Only %v available", n, z.a)
	}

	oldcursor = z.c
	z.c = oldcursor + n
	z.a = z.a - n
	return
}

func (z *bytesDecReader) readn(n int) (bs []byte) {
	if n <= 0 {
		return
	}
	c0 := z.consume(n)
	bs = z.b[c0:z.c]
	return
}

func (z *bytesDecReader) readb(bs []byte) {
	copy(bs, z.readn(len(bs)))
}

func (z *bytesDecReader) readn1() uint8 {
	c0 := z.consume(1)
	return z.b[c0]
}

func (z *bytesDecReader) readUint16() uint16 {
	c0 := z.consume(2)
	return uint16(z.b[c0+1]) | uint16(z.b[c0])<<8
}

func (z *bytesDecReader) readUint32() uint32 {
	c0 := z.consume(4)
	return bigen.Uint32(z.b[c0:z.c])
}

func (z *bytesDecReader) readUint64() uint64 {
	c0 := z.consume(8)
	return bigen.Uint64(z.b[c0:z.c])
}

type decFnInfo struct {
	ti    *typeInfo
	d     *Decoder
	dd    decDriver
	xfFn  func(reflect.Value, []byte) error
	xfTag byte
	array bool
}

func (f *decFnInfo) builtin(rv reflect.Value) {
	f.dd.decodeBuiltin(f.ti.rtid, rv.Addr().Interface())
}

func (f *decFnInfo) rawExt(rv reflect.Value) {
	xtag, xbs := f.dd.decodeExt(false, 0)
	rv.Field(0).SetUint(uint64(xtag))
	rv.Field(1).SetBytes(xbs)
}

func (f *decFnInfo) ext(rv reflect.Value) {
	_, xbs := f.dd.decodeExt(true, f.xfTag)
	if fnerr := f.xfFn(rv, xbs); fnerr != nil {
		panic(fnerr)
	}
}

func (f *decFnInfo) binaryMarshal(rv reflect.Value) {
	var bm binaryUnmarshaler
	if f.ti.unmIndir == -1 {
		bm = rv.Addr().Interface().(binaryUnmarshaler)
	} else if f.ti.unmIndir == 0 {
		bm = rv.Interface().(binaryUnmarshaler)
	} else {
		for j, k := int8(0), f.ti.unmIndir; j < k; j++ {
			if rv.IsNil() {
				rv.Set(reflect.New(rv.Type().Elem()))
			}
			rv = rv.Elem()
		}
		bm = rv.Interface().(binaryUnmarshaler)
	}
	xbs, _ := f.dd.decodeBytes(nil)
	if fnerr := bm.UnmarshalBinary(xbs); fnerr != nil {
		panic(fnerr)
	}
}

func (f *decFnInfo) kErr(rv reflect.Value) {
	decErr("Unhandled value for kind: %v: %s", rv.Kind(), msgBadDesc)
}

func (f *decFnInfo) kString(rv reflect.Value) {
	rv.SetString(f.dd.decodeString())
}

func (f *decFnInfo) kBool(rv reflect.Value) {
	rv.SetBool(f.dd.decodeBool())
}

func (f *decFnInfo) kInt(rv reflect.Value) {
	rv.SetInt(f.dd.decodeInt(intBitsize))
}

func (f *decFnInfo) kInt64(rv reflect.Value) {
	rv.SetInt(f.dd.decodeInt(64))
}

func (f *decFnInfo) kInt32(rv reflect.Value) {
	rv.SetInt(f.dd.decodeInt(32))
}

func (f *decFnInfo) kInt8(rv reflect.Value) {
	rv.SetInt(f.dd.decodeInt(8))
}

func (f *decFnInfo) kInt16(rv reflect.Value) {
	rv.SetInt(f.dd.decodeInt(16))
}

func (f *decFnInfo) kFloat32(rv reflect.Value) {
	rv.SetFloat(f.dd.decodeFloat(true))
}

func (f *decFnInfo) kFloat64(rv reflect.Value) {
	rv.SetFloat(f.dd.decodeFloat(false))
}

func (f *decFnInfo) kUint8(rv reflect.Value) {
	rv.SetUint(f.dd.decodeUint(8))
}

func (f *decFnInfo) kUint64(rv reflect.Value) {
	rv.SetUint(f.dd.decodeUint(64))
}

func (f *decFnInfo) kUint(rv reflect.Value) {
	rv.SetUint(f.dd.decodeUint(uintBitsize))
}

func (f *decFnInfo) kUint32(rv reflect.Value) {
	rv.SetUint(f.dd.decodeUint(32))
}

func (f *decFnInfo) kUint16(rv reflect.Value) {
	rv.SetUint(f.dd.decodeUint(16))
}

func (f *decFnInfo) kInterface(rv reflect.Value) {

	if !rv.IsNil() {
		f.d.decodeValue(rv.Elem())
		return
	}

	v, vt, decodeFurther := f.dd.decodeNaked()
	if vt == valueTypeNil {
		return
	}

	if num := f.ti.rt.NumMethod(); num > 0 {
		decErr("decodeValue: Cannot decode non-nil codec value into nil %v (%v methods)",
			f.ti.rt, num)
	}
	var rvn reflect.Value
	var useRvn bool
	switch vt {
	case valueTypeMap:
		if f.d.h.MapType == nil {
			var m2 map[interface{}]interface{}
			v = &m2
		} else {
			rvn = reflect.New(f.d.h.MapType).Elem()
			useRvn = true
		}
	case valueTypeArray:
		if f.d.h.SliceType == nil {
			var m2 []interface{}
			v = &m2
		} else {
			rvn = reflect.New(f.d.h.SliceType).Elem()
			useRvn = true
		}
	case valueTypeExt:
		re := v.(*RawExt)
		var bfn func(reflect.Value, []byte) error
		rvn, bfn = f.d.h.getDecodeExtForTag(re.Tag)
		if bfn == nil {
			rvn = reflect.ValueOf(*re)
		} else if fnerr := bfn(rvn, re.Data); fnerr != nil {
			panic(fnerr)
		}
		rv.Set(rvn)
		return
	}
	if decodeFurther {
		if useRvn {
			f.d.decodeValue(rvn)
		} else if v != nil {

			f.d.decode(v)
			rvn = reflect.ValueOf(v).Elem()
			useRvn = true
		}
	}
	if useRvn {
		rv.Set(rvn)
	} else if v != nil {
		rv.Set(reflect.ValueOf(v))
	}
}

func (f *decFnInfo) kStruct(rv reflect.Value) {
	fti := f.ti
	if currEncodedType := f.dd.currentEncodedType(); currEncodedType == valueTypeMap {
		containerLen := f.dd.readMapLen()
		if containerLen == 0 {
			return
		}
		tisfi := fti.sfi
		for j := 0; j < containerLen; j++ {

			f.dd.initReadNext()
			rvkencname := f.dd.decodeString()

			if k := fti.indexForEncName(rvkencname); k > -1 {
				sfik := tisfi[k]
				if sfik.i != -1 {
					f.d.decodeValue(rv.Field(int(sfik.i)))
				} else {
					f.d.decEmbeddedField(rv, sfik.is)
				}

			} else {
				if f.d.h.ErrorIfNoField {
					decErr("No matching struct field found when decoding stream map with key: %v",
						rvkencname)
				} else {
					var nilintf0 interface{}
					f.d.decodeValue(reflect.ValueOf(&nilintf0).Elem())
				}
			}
		}
	} else if currEncodedType == valueTypeArray {
		containerLen := f.dd.readArrayLen()
		if containerLen == 0 {
			return
		}
		for j, si := range fti.sfip {
			if j == containerLen {
				break
			}
			if si.i != -1 {
				f.d.decodeValue(rv.Field(int(si.i)))
			} else {
				f.d.decEmbeddedField(rv, si.is)
			}
		}
		if containerLen > len(fti.sfip) {

			for j := len(fti.sfip); j < containerLen; j++ {
				var nilintf0 interface{}
				f.d.decodeValue(reflect.ValueOf(&nilintf0).Elem())
			}
		}
	} else {
		decErr("Only encoded map or array can be decoded into a struct. (valueType: %x)",
			currEncodedType)
	}
}

func (f *decFnInfo) kSlice(rv reflect.Value) {

	currEncodedType := f.dd.currentEncodedType()

	switch currEncodedType {
	case valueTypeBytes, valueTypeString:
		if f.ti.rtid == uint8SliceTypId || f.ti.rt.Elem().Kind() == reflect.Uint8 {
			if bs2, changed2 := f.dd.decodeBytes(rv.Bytes()); changed2 {
				rv.SetBytes(bs2)
			}
			return
		}
	}

	if shortCircuitReflectToFastPath && rv.CanAddr() {
		switch f.ti.rtid {
		case intfSliceTypId:
			f.d.decSliceIntf(rv.Addr().Interface().(*[]interface{}), currEncodedType, f.array)
			return
		case uint64SliceTypId:
			f.d.decSliceUint64(rv.Addr().Interface().(*[]uint64), currEncodedType, f.array)
			return
		case int64SliceTypId:
			f.d.decSliceInt64(rv.Addr().Interface().(*[]int64), currEncodedType, f.array)
			return
		case strSliceTypId:
			f.d.decSliceStr(rv.Addr().Interface().(*[]string), currEncodedType, f.array)
			return
		}
	}

	containerLen, containerLenS := decContLens(f.dd, currEncodedType)

	if rv.IsNil() {
		rv.Set(reflect.MakeSlice(f.ti.rt, containerLenS, containerLenS))
	}

	if containerLen == 0 {
		return
	}

	if rvcap, rvlen := rv.Len(), rv.Cap(); containerLenS > rvcap {
		if f.array { // !rv.CanSet()
			decErr(msgDecCannotExpandArr, rvcap, containerLenS)
		}
		rvn := reflect.MakeSlice(f.ti.rt, containerLenS, containerLenS)
		if rvlen > 0 {
			reflect.Copy(rvn, rv)
		}
		rv.Set(rvn)
	} else if containerLenS > rvlen {
		rv.SetLen(containerLenS)
	}

	for j := 0; j < containerLenS; j++ {
		f.d.decodeValue(rv.Index(j))
	}
}

func (f *decFnInfo) kArray(rv reflect.Value) {

	f.kSlice(rv.Slice(0, rv.Len()))
}

func (f *decFnInfo) kMap(rv reflect.Value) {
	if shortCircuitReflectToFastPath && rv.CanAddr() {
		switch f.ti.rtid {
		case mapStrIntfTypId:
			f.d.decMapStrIntf(rv.Addr().Interface().(*map[string]interface{}))
			return
		case mapIntfIntfTypId:
			f.d.decMapIntfIntf(rv.Addr().Interface().(*map[interface{}]interface{}))
			return
		case mapInt64IntfTypId:
			f.d.decMapInt64Intf(rv.Addr().Interface().(*map[int64]interface{}))
			return
		case mapUint64IntfTypId:
			f.d.decMapUint64Intf(rv.Addr().Interface().(*map[uint64]interface{}))
			return
		}
	}

	containerLen := f.dd.readMapLen()

	if rv.IsNil() {
		rv.Set(reflect.MakeMap(f.ti.rt))
	}

	if containerLen == 0 {
		return
	}

	ktype, vtype := f.ti.rt.Key(), f.ti.rt.Elem()
	ktypeId := reflect.ValueOf(ktype).Pointer()
	for j := 0; j < containerLen; j++ {
		rvk := reflect.New(ktype).Elem()
		f.d.decodeValue(rvk)

		if ktypeId == intfTypId {
			rvk = rvk.Elem()
			if rvk.Type() == uint8SliceTyp {
				rvk = reflect.ValueOf(string(rvk.Bytes()))
			}
		}
		rvv := rv.MapIndex(rvk)
		if !rvv.IsValid() {
			rvv = reflect.New(vtype).Elem()
		}

		f.d.decodeValue(rvv)
		rv.SetMapIndex(rvk, rvv)
	}
}

type decFn struct {
	i *decFnInfo
	f func(*decFnInfo, reflect.Value)
}

type Decoder struct {
	r decReader
	d decDriver
	h *BasicHandle
	f map[uintptr]decFn
	x []uintptr
	s []decFn
}

//
func NewDecoder(r io.Reader, h Handle) *Decoder {
	z := ioDecReader{
		r: r,
	}
	z.br, _ = r.(io.ByteReader)
	return &Decoder{r: &z, d: h.newDecDriver(&z), h: h.getBasicHandle()}
}

func NewDecoderBytes(in []byte, h Handle) *Decoder {
	z := bytesDecReader{
		b: in,
		a: len(in),
	}
	return &Decoder{r: &z, d: h.newDecDriver(&z), h: h.getBasicHandle()}
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
func (d *Decoder) Decode(v interface{}) (err error) {
	defer panicToErr(&err)
	d.decode(v)
	return
}

func (d *Decoder) decode(iv interface{}) {
	d.d.initReadNext()

	switch v := iv.(type) {
	case nil:
		decErr("Cannot decode into nil.")

	case reflect.Value:
		d.chkPtrValue(v)
		d.decodeValue(v.Elem())

	case *string:
		*v = d.d.decodeString()
	case *bool:
		*v = d.d.decodeBool()
	case *int:
		*v = int(d.d.decodeInt(intBitsize))
	case *int8:
		*v = int8(d.d.decodeInt(8))
	case *int16:
		*v = int16(d.d.decodeInt(16))
	case *int32:
		*v = int32(d.d.decodeInt(32))
	case *int64:
		*v = d.d.decodeInt(64)
	case *uint:
		*v = uint(d.d.decodeUint(uintBitsize))
	case *uint8:
		*v = uint8(d.d.decodeUint(8))
	case *uint16:
		*v = uint16(d.d.decodeUint(16))
	case *uint32:
		*v = uint32(d.d.decodeUint(32))
	case *uint64:
		*v = d.d.decodeUint(64)
	case *float32:
		*v = float32(d.d.decodeFloat(true))
	case *float64:
		*v = d.d.decodeFloat(false)
	case *[]byte:
		*v, _ = d.d.decodeBytes(*v)

	case *[]interface{}:
		d.decSliceIntf(v, valueTypeInvalid, false)
	case *[]uint64:
		d.decSliceUint64(v, valueTypeInvalid, false)
	case *[]int64:
		d.decSliceInt64(v, valueTypeInvalid, false)
	case *[]string:
		d.decSliceStr(v, valueTypeInvalid, false)
	case *map[string]interface{}:
		d.decMapStrIntf(v)
	case *map[interface{}]interface{}:
		d.decMapIntfIntf(v)
	case *map[uint64]interface{}:
		d.decMapUint64Intf(v)
	case *map[int64]interface{}:
		d.decMapInt64Intf(v)

	case *interface{}:
		d.decodeValue(reflect.ValueOf(iv).Elem())

	default:
		rv := reflect.ValueOf(iv)
		d.chkPtrValue(rv)
		d.decodeValue(rv.Elem())
	}
}

func (d *Decoder) decodeValue(rv reflect.Value) {
	d.d.initReadNext()

	if d.d.tryDecodeAsNil() {

		if rv.Kind() == reflect.Ptr {
			if !rv.IsNil() {
				rv.Set(reflect.Zero(rv.Type()))
			}
			return
		}

		if rv.IsValid() { // rv.CanSet() // always settable, except it's invalid
			rv.Set(reflect.Zero(rv.Type()))
		}
		return
	}

	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}

	rt := rv.Type()
	rtid := reflect.ValueOf(rt).Pointer()

	var fn decFn
	var ok bool
	if useMapForCodecCache {
		fn, ok = d.f[rtid]
	} else {
		for i, v := range d.x {
			if v == rtid {
				fn, ok = d.s[i], true
				break
			}
		}
	}
	if !ok {

		fi := decFnInfo{ti: getTypeInfo(rtid, rt), d: d, dd: d.d}
		fn.i = &fi

		//

		//

		if rtid == rawExtTypId {
			fn.f = (*decFnInfo).rawExt
		} else if d.d.isBuiltinType(rtid) {
			fn.f = (*decFnInfo).builtin
		} else if xfTag, xfFn := d.h.getDecodeExt(rtid); xfFn != nil {
			fi.xfTag, fi.xfFn = xfTag, xfFn
			fn.f = (*decFnInfo).ext
		} else if supportBinaryMarshal && fi.ti.unm {
			fn.f = (*decFnInfo).binaryMarshal
		} else {
			switch rk := rt.Kind(); rk {
			case reflect.String:
				fn.f = (*decFnInfo).kString
			case reflect.Bool:
				fn.f = (*decFnInfo).kBool
			case reflect.Int:
				fn.f = (*decFnInfo).kInt
			case reflect.Int64:
				fn.f = (*decFnInfo).kInt64
			case reflect.Int32:
				fn.f = (*decFnInfo).kInt32
			case reflect.Int8:
				fn.f = (*decFnInfo).kInt8
			case reflect.Int16:
				fn.f = (*decFnInfo).kInt16
			case reflect.Float32:
				fn.f = (*decFnInfo).kFloat32
			case reflect.Float64:
				fn.f = (*decFnInfo).kFloat64
			case reflect.Uint8:
				fn.f = (*decFnInfo).kUint8
			case reflect.Uint64:
				fn.f = (*decFnInfo).kUint64
			case reflect.Uint:
				fn.f = (*decFnInfo).kUint
			case reflect.Uint32:
				fn.f = (*decFnInfo).kUint32
			case reflect.Uint16:
				fn.f = (*decFnInfo).kUint16

			case reflect.Interface:
				fn.f = (*decFnInfo).kInterface
			case reflect.Struct:
				fn.f = (*decFnInfo).kStruct
			case reflect.Slice:
				fn.f = (*decFnInfo).kSlice
			case reflect.Array:
				fi.array = true
				fn.f = (*decFnInfo).kArray
			case reflect.Map:
				fn.f = (*decFnInfo).kMap
			default:
				fn.f = (*decFnInfo).kErr
			}
		}
		if useMapForCodecCache {
			if d.f == nil {
				d.f = make(map[uintptr]decFn, 16)
			}
			d.f[rtid] = fn
		} else {
			d.s = append(d.s, fn)
			d.x = append(d.x, rtid)
		}
	}

	fn.f(fn.i, rv)

	return
}

func (d *Decoder) chkPtrValue(rv reflect.Value) {

	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		return
	}
	if !rv.IsValid() {
		decErr("Cannot decode into a zero (ie invalid) reflect.Value")
	}
	if !rv.CanInterface() {
		decErr("Cannot decode into a value without an interface: %v", rv)
	}
	rvi := rv.Interface()
	decErr("Cannot decode into non-pointer or nil pointer. Got: %v, %T, %v",
		rv.Kind(), rvi, rvi)
}

func (d *Decoder) decEmbeddedField(rv reflect.Value, index []int) {

	for _, j := range index {
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				rv.Set(reflect.New(rv.Type().Elem()))
			}

			rv = rv.Elem()
		}
		rv = rv.Field(j)
	}
	d.decodeValue(rv)
}

func (d *Decoder) decSliceIntf(v *[]interface{}, currEncodedType valueType, doNotReset bool) {
	_, containerLenS := decContLens(d.d, currEncodedType)
	s := *v
	if s == nil {
		s = make([]interface{}, containerLenS, containerLenS)
	} else if containerLenS > cap(s) {
		if doNotReset {
			decErr(msgDecCannotExpandArr, cap(s), containerLenS)
		}
		s = make([]interface{}, containerLenS, containerLenS)
		copy(s, *v)
	} else if containerLenS > len(s) {
		s = s[:containerLenS]
	}
	for j := 0; j < containerLenS; j++ {
		d.decode(&s[j])
	}
	*v = s
}

func (d *Decoder) decSliceInt64(v *[]int64, currEncodedType valueType, doNotReset bool) {
	_, containerLenS := decContLens(d.d, currEncodedType)
	s := *v
	if s == nil {
		s = make([]int64, containerLenS, containerLenS)
	} else if containerLenS > cap(s) {
		if doNotReset {
			decErr(msgDecCannotExpandArr, cap(s), containerLenS)
		}
		s = make([]int64, containerLenS, containerLenS)
		copy(s, *v)
	} else if containerLenS > len(s) {
		s = s[:containerLenS]
	}
	for j := 0; j < containerLenS; j++ {

		d.d.initReadNext()
		s[j] = d.d.decodeInt(intBitsize)
	}
	*v = s
}

func (d *Decoder) decSliceUint64(v *[]uint64, currEncodedType valueType, doNotReset bool) {
	_, containerLenS := decContLens(d.d, currEncodedType)
	s := *v
	if s == nil {
		s = make([]uint64, containerLenS, containerLenS)
	} else if containerLenS > cap(s) {
		if doNotReset {
			decErr(msgDecCannotExpandArr, cap(s), containerLenS)
		}
		s = make([]uint64, containerLenS, containerLenS)
		copy(s, *v)
	} else if containerLenS > len(s) {
		s = s[:containerLenS]
	}
	for j := 0; j < containerLenS; j++ {

		d.d.initReadNext()
		s[j] = d.d.decodeUint(intBitsize)
	}
	*v = s
}

func (d *Decoder) decSliceStr(v *[]string, currEncodedType valueType, doNotReset bool) {
	_, containerLenS := decContLens(d.d, currEncodedType)
	s := *v
	if s == nil {
		s = make([]string, containerLenS, containerLenS)
	} else if containerLenS > cap(s) {
		if doNotReset {
			decErr(msgDecCannotExpandArr, cap(s), containerLenS)
		}
		s = make([]string, containerLenS, containerLenS)
		copy(s, *v)
	} else if containerLenS > len(s) {
		s = s[:containerLenS]
	}
	for j := 0; j < containerLenS; j++ {

		d.d.initReadNext()
		s[j] = d.d.decodeString()
	}
	*v = s
}

func (d *Decoder) decMapIntfIntf(v *map[interface{}]interface{}) {
	containerLen := d.d.readMapLen()
	m := *v
	if m == nil {
		m = make(map[interface{}]interface{}, containerLen)
		*v = m
	}
	for j := 0; j < containerLen; j++ {
		var mk interface{}
		d.decode(&mk)

		if bv, bok := mk.([]byte); bok {
			mk = string(bv)
		}
		mv := m[mk]
		d.decode(&mv)
		m[mk] = mv
	}
}

func (d *Decoder) decMapInt64Intf(v *map[int64]interface{}) {
	containerLen := d.d.readMapLen()
	m := *v
	if m == nil {
		m = make(map[int64]interface{}, containerLen)
		*v = m
	}
	for j := 0; j < containerLen; j++ {
		d.d.initReadNext()
		mk := d.d.decodeInt(intBitsize)
		mv := m[mk]
		d.decode(&mv)
		m[mk] = mv
	}
}

func (d *Decoder) decMapUint64Intf(v *map[uint64]interface{}) {
	containerLen := d.d.readMapLen()
	m := *v
	if m == nil {
		m = make(map[uint64]interface{}, containerLen)
		*v = m
	}
	for j := 0; j < containerLen; j++ {
		d.d.initReadNext()
		mk := d.d.decodeUint(intBitsize)
		mv := m[mk]
		d.decode(&mv)
		m[mk] = mv
	}
}

func (d *Decoder) decMapStrIntf(v *map[string]interface{}) {
	containerLen := d.d.readMapLen()
	m := *v
	if m == nil {
		m = make(map[string]interface{}, containerLen)
		*v = m
	}
	for j := 0; j < containerLen; j++ {
		d.d.initReadNext()
		mk := d.d.decodeString()
		mv := m[mk]
		d.decode(&mv)
		m[mk] = mv
	}
}

func decContLens(dd decDriver, currEncodedType valueType) (containerLen, containerLenS int) {
	if currEncodedType == valueTypeInvalid {
		currEncodedType = dd.currentEncodedType()
	}
	switch currEncodedType {
	case valueTypeArray:
		containerLen = dd.readArrayLen()
		containerLenS = containerLen
	case valueTypeMap:
		containerLen = dd.readMapLen()
		containerLenS = containerLen * 2
	default:
		decErr("Only encoded map or array can be decoded into a slice. (valueType: %0x)",
			currEncodedType)
	}
	return
}

func decErr(format string, params ...interface{}) {
	doPanic(msgTagDec, format, params...)
}
