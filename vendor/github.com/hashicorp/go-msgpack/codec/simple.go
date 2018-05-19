// Copyright (c) 2012, 2013 Ugorji Nwoke. All rights reserved.

package codec

import "math"

const (
	_               uint8 = iota
	simpleVdNil           = 1
	simpleVdFalse         = 2
	simpleVdTrue          = 3
	simpleVdFloat32       = 4
	simpleVdFloat64       = 5

	simpleVdPosInt = 8
	simpleVdNegInt = 12

	simpleVdString    = 216
	simpleVdByteArray = 224
	simpleVdArray     = 232
	simpleVdMap       = 240
	simpleVdExt       = 248
)

type simpleEncDriver struct {
	h *SimpleHandle
	w encWriter
}

func (e *simpleEncDriver) isBuiltinType(rt uintptr) bool {
	return false
}

func (e *simpleEncDriver) encodeBuiltin(rt uintptr, v interface{}) {
}

func (e *simpleEncDriver) encodeNil() {
	e.w.writen1(simpleVdNil)
}

func (e *simpleEncDriver) encodeBool(b bool) {
	if b {
		e.w.writen1(simpleVdTrue)
	} else {
		e.w.writen1(simpleVdFalse)
	}
}

func (e *simpleEncDriver) encodeFloat32(f float32) {
	e.w.writen1(simpleVdFloat32)
	e.w.writeUint32(math.Float32bits(f))
}

func (e *simpleEncDriver) encodeFloat64(f float64) {
	e.w.writen1(simpleVdFloat64)
	e.w.writeUint64(math.Float64bits(f))
}

func (e *simpleEncDriver) encodeInt(v int64) {
	if v < 0 {
		e.encUint(uint64(-v), simpleVdNegInt)
	} else {
		e.encUint(uint64(v), simpleVdPosInt)
	}
}

func (e *simpleEncDriver) encodeUint(v uint64) {
	e.encUint(v, simpleVdPosInt)
}

func (e *simpleEncDriver) encUint(v uint64, bd uint8) {
	switch {
	case v <= math.MaxUint8:
		e.w.writen2(bd, uint8(v))
	case v <= math.MaxUint16:
		e.w.writen1(bd + 1)
		e.w.writeUint16(uint16(v))
	case v <= math.MaxUint32:
		e.w.writen1(bd + 2)
		e.w.writeUint32(uint32(v))
	case v <= math.MaxUint64:
		e.w.writen1(bd + 3)
		e.w.writeUint64(v)
	}
}

func (e *simpleEncDriver) encLen(bd byte, length int) {
	switch {
	case length == 0:
		e.w.writen1(bd)
	case length <= math.MaxUint8:
		e.w.writen1(bd + 1)
		e.w.writen1(uint8(length))
	case length <= math.MaxUint16:
		e.w.writen1(bd + 2)
		e.w.writeUint16(uint16(length))
	case int64(length) <= math.MaxUint32:
		e.w.writen1(bd + 3)
		e.w.writeUint32(uint32(length))
	default:
		e.w.writen1(bd + 4)
		e.w.writeUint64(uint64(length))
	}
}

func (e *simpleEncDriver) encodeExtPreamble(xtag byte, length int) {
	e.encLen(simpleVdExt, length)
	e.w.writen1(xtag)
}

func (e *simpleEncDriver) encodeArrayPreamble(length int) {
	e.encLen(simpleVdArray, length)
}

func (e *simpleEncDriver) encodeMapPreamble(length int) {
	e.encLen(simpleVdMap, length)
}

func (e *simpleEncDriver) encodeString(c charEncoding, v string) {
	e.encLen(simpleVdString, len(v))
	e.w.writestr(v)
}

func (e *simpleEncDriver) encodeSymbol(v string) {
	e.encodeString(c_UTF8, v)
}

func (e *simpleEncDriver) encodeStringBytes(c charEncoding, v []byte) {
	e.encLen(simpleVdByteArray, len(v))
	e.w.writeb(v)
}

type simpleDecDriver struct {
	h      *SimpleHandle
	r      decReader
	bdRead bool
	bdType valueType
	bd     byte
}

func (d *simpleDecDriver) initReadNext() {
	if d.bdRead {
		return
	}
	d.bd = d.r.readn1()
	d.bdRead = true
	d.bdType = valueTypeUnset
}

func (d *simpleDecDriver) currentEncodedType() valueType {
	if d.bdType == valueTypeUnset {
		switch d.bd {
		case simpleVdNil:
			d.bdType = valueTypeNil
		case simpleVdTrue, simpleVdFalse:
			d.bdType = valueTypeBool
		case simpleVdPosInt, simpleVdPosInt + 1, simpleVdPosInt + 2, simpleVdPosInt + 3:
			d.bdType = valueTypeUint
		case simpleVdNegInt, simpleVdNegInt + 1, simpleVdNegInt + 2, simpleVdNegInt + 3:
			d.bdType = valueTypeInt
		case simpleVdFloat32, simpleVdFloat64:
			d.bdType = valueTypeFloat
		case simpleVdString, simpleVdString + 1, simpleVdString + 2, simpleVdString + 3, simpleVdString + 4:
			d.bdType = valueTypeString
		case simpleVdByteArray, simpleVdByteArray + 1, simpleVdByteArray + 2, simpleVdByteArray + 3, simpleVdByteArray + 4:
			d.bdType = valueTypeBytes
		case simpleVdExt, simpleVdExt + 1, simpleVdExt + 2, simpleVdExt + 3, simpleVdExt + 4:
			d.bdType = valueTypeExt
		case simpleVdArray, simpleVdArray + 1, simpleVdArray + 2, simpleVdArray + 3, simpleVdArray + 4:
			d.bdType = valueTypeArray
		case simpleVdMap, simpleVdMap + 1, simpleVdMap + 2, simpleVdMap + 3, simpleVdMap + 4:
			d.bdType = valueTypeMap
		default:
			decErr("currentEncodedType: Unrecognized d.vd: 0x%x", d.bd)
		}
	}
	return d.bdType
}

func (d *simpleDecDriver) tryDecodeAsNil() bool {
	if d.bd == simpleVdNil {
		d.bdRead = false
		return true
	}
	return false
}

func (d *simpleDecDriver) isBuiltinType(rt uintptr) bool {
	return false
}

func (d *simpleDecDriver) decodeBuiltin(rt uintptr, v interface{}) {
}

func (d *simpleDecDriver) decIntAny() (ui uint64, i int64, neg bool) {
	switch d.bd {
	case simpleVdPosInt:
		ui = uint64(d.r.readn1())
		i = int64(ui)
	case simpleVdPosInt + 1:
		ui = uint64(d.r.readUint16())
		i = int64(ui)
	case simpleVdPosInt + 2:
		ui = uint64(d.r.readUint32())
		i = int64(ui)
	case simpleVdPosInt + 3:
		ui = uint64(d.r.readUint64())
		i = int64(ui)
	case simpleVdNegInt:
		ui = uint64(d.r.readn1())
		i = -(int64(ui))
		neg = true
	case simpleVdNegInt + 1:
		ui = uint64(d.r.readUint16())
		i = -(int64(ui))
		neg = true
	case simpleVdNegInt + 2:
		ui = uint64(d.r.readUint32())
		i = -(int64(ui))
		neg = true
	case simpleVdNegInt + 3:
		ui = uint64(d.r.readUint64())
		i = -(int64(ui))
		neg = true
	default:
		decErr("decIntAny: Integer only valid from pos/neg integer1..8. Invalid descriptor: %v", d.bd)
	}

	return
}

func (d *simpleDecDriver) decodeInt(bitsize uint8) (i int64) {
	_, i, _ = d.decIntAny()
	checkOverflow(0, i, bitsize)
	d.bdRead = false
	return
}

func (d *simpleDecDriver) decodeUint(bitsize uint8) (ui uint64) {
	ui, i, neg := d.decIntAny()
	if neg {
		decErr("Assigning negative signed value: %v, to unsigned type", i)
	}
	checkOverflow(ui, 0, bitsize)
	d.bdRead = false
	return
}

func (d *simpleDecDriver) decodeFloat(chkOverflow32 bool) (f float64) {
	switch d.bd {
	case simpleVdFloat32:
		f = float64(math.Float32frombits(d.r.readUint32()))
	case simpleVdFloat64:
		f = math.Float64frombits(d.r.readUint64())
	default:
		if d.bd >= simpleVdPosInt && d.bd <= simpleVdNegInt+3 {
			_, i, _ := d.decIntAny()
			f = float64(i)
		} else {
			decErr("Float only valid from float32/64: Invalid descriptor: %v", d.bd)
		}
	}
	checkOverflowFloat32(f, chkOverflow32)
	d.bdRead = false
	return
}

func (d *simpleDecDriver) decodeBool() (b bool) {
	switch d.bd {
	case simpleVdTrue:
		b = true
	case simpleVdFalse:
	default:
		decErr("Invalid single-byte value for bool: %s: %x", msgBadDesc, d.bd)
	}
	d.bdRead = false
	return
}

func (d *simpleDecDriver) readMapLen() (length int) {
	d.bdRead = false
	return d.decLen()
}

func (d *simpleDecDriver) readArrayLen() (length int) {
	d.bdRead = false
	return d.decLen()
}

func (d *simpleDecDriver) decLen() int {
	switch d.bd % 8 {
	case 0:
		return 0
	case 1:
		return int(d.r.readn1())
	case 2:
		return int(d.r.readUint16())
	case 3:
		ui := uint64(d.r.readUint32())
		checkOverflow(ui, 0, intBitsize)
		return int(ui)
	case 4:
		ui := d.r.readUint64()
		checkOverflow(ui, 0, intBitsize)
		return int(ui)
	}
	decErr("decLen: Cannot read length: bd%8 must be in range 0..4. Got: %d", d.bd%8)
	return -1
}

func (d *simpleDecDriver) decodeString() (s string) {
	s = string(d.r.readn(d.decLen()))
	d.bdRead = false
	return
}

func (d *simpleDecDriver) decodeBytes(bs []byte) (bsOut []byte, changed bool) {
	if clen := d.decLen(); clen > 0 {

		if len(bs) != clen {
			if len(bs) > clen {
				bs = bs[:clen]
			} else {
				bs = make([]byte, clen)
			}
			bsOut = bs
			changed = true
		}
		d.r.readb(bs)
	}
	d.bdRead = false
	return
}

func (d *simpleDecDriver) decodeExt(verifyTag bool, tag byte) (xtag byte, xbs []byte) {
	switch d.bd {
	case simpleVdExt, simpleVdExt + 1, simpleVdExt + 2, simpleVdExt + 3, simpleVdExt + 4:
		l := d.decLen()
		xtag = d.r.readn1()
		if verifyTag && xtag != tag {
			decErr("Wrong extension tag. Got %b. Expecting: %v", xtag, tag)
		}
		xbs = d.r.readn(l)
	case simpleVdByteArray, simpleVdByteArray + 1, simpleVdByteArray + 2, simpleVdByteArray + 3, simpleVdByteArray + 4:
		xbs, _ = d.decodeBytes(nil)
	default:
		decErr("Invalid d.vd for extensions (Expecting extensions or byte array). Got: 0x%x", d.bd)
	}
	d.bdRead = false
	return
}

func (d *simpleDecDriver) decodeNaked() (v interface{}, vt valueType, decodeFurther bool) {
	d.initReadNext()

	switch d.bd {
	case simpleVdNil:
		vt = valueTypeNil
	case simpleVdFalse:
		vt = valueTypeBool
		v = false
	case simpleVdTrue:
		vt = valueTypeBool
		v = true
	case simpleVdPosInt, simpleVdPosInt + 1, simpleVdPosInt + 2, simpleVdPosInt + 3:
		vt = valueTypeUint
		ui, _, _ := d.decIntAny()
		v = ui
	case simpleVdNegInt, simpleVdNegInt + 1, simpleVdNegInt + 2, simpleVdNegInt + 3:
		vt = valueTypeInt
		_, i, _ := d.decIntAny()
		v = i
	case simpleVdFloat32:
		vt = valueTypeFloat
		v = d.decodeFloat(true)
	case simpleVdFloat64:
		vt = valueTypeFloat
		v = d.decodeFloat(false)
	case simpleVdString, simpleVdString + 1, simpleVdString + 2, simpleVdString + 3, simpleVdString + 4:
		vt = valueTypeString
		v = d.decodeString()
	case simpleVdByteArray, simpleVdByteArray + 1, simpleVdByteArray + 2, simpleVdByteArray + 3, simpleVdByteArray + 4:
		vt = valueTypeBytes
		v, _ = d.decodeBytes(nil)
	case simpleVdExt, simpleVdExt + 1, simpleVdExt + 2, simpleVdExt + 3, simpleVdExt + 4:
		vt = valueTypeExt
		l := d.decLen()
		var re RawExt
		re.Tag = d.r.readn1()
		re.Data = d.r.readn(l)
		v = &re
		vt = valueTypeExt
	case simpleVdArray, simpleVdArray + 1, simpleVdArray + 2, simpleVdArray + 3, simpleVdArray + 4:
		vt = valueTypeArray
		decodeFurther = true
	case simpleVdMap, simpleVdMap + 1, simpleVdMap + 2, simpleVdMap + 3, simpleVdMap + 4:
		vt = valueTypeMap
		decodeFurther = true
	default:
		decErr("decodeNaked: Unrecognized d.vd: 0x%x", d.bd)
	}

	if !decodeFurther {
		d.bdRead = false
	}
	return
}

//
//
type SimpleHandle struct {
	BasicHandle
}

func (h *SimpleHandle) newEncDriver(w encWriter) encDriver {
	return &simpleEncDriver{w: w, h: h}
}

func (h *SimpleHandle) newDecDriver(r decReader) decDriver {
	return &simpleDecDriver{r: r, h: h}
}

func (_ *SimpleHandle) writeExt() bool {
	return true
}

func (h *SimpleHandle) getBasicHandle() *BasicHandle {
	return &h.BasicHandle
}

var _ decDriver = (*simpleDecDriver)(nil)
var _ encDriver = (*simpleEncDriver)(nil)
