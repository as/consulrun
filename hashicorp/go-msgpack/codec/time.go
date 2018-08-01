// Copyright (c) 2012, 2013 Ugorji Nwoke. All rights reserved.

package codec

import (
	"time"
)

var (
	timeDigits = [...]byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}
)

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
func encodeTime(t time.Time) []byte {

	tsecs, tnsecs := t.Unix(), t.Nanosecond()
	var (
		bd   byte
		btmp [8]byte
		bs   [16]byte
		i    int = 1
	)
	l := t.Location()
	if l == time.UTC {
		l = nil
	}
	if tsecs != 0 {
		bd = bd | 0x80
		bigen.PutUint64(btmp[:], uint64(tsecs))
		f := pruneSignExt(btmp[:], tsecs >= 0)
		bd = bd | (byte(7-f) << 2)
		copy(bs[i:], btmp[f:])
		i = i + (8 - f)
	}
	if tnsecs != 0 {
		bd = bd | 0x40
		bigen.PutUint32(btmp[:4], uint32(tnsecs))
		f := pruneSignExt(btmp[:4], true)
		bd = bd | byte(3-f)
		copy(bs[i:], btmp[f:4])
		i = i + (4 - f)
	}
	if l != nil {
		bd = bd | 0x20

		_, zoneOffset := t.Zone()

		zoneOffset /= 60
		z := uint16(zoneOffset)
		bigen.PutUint16(btmp[:2], z)

		bs[i] = btmp[0] & 0x3f
		bs[i+1] = btmp[1]
		i = i + 2
	}
	bs[0] = bd
	return bs[0:i]
}

func decodeTime(bs []byte) (tt time.Time, err error) {
	bd := bs[0]
	var (
		tsec  int64
		tnsec uint32
		tz    uint16
		i     byte = 1
		i2    byte
		n     byte
	)
	if bd&(1<<7) != 0 {
		var btmp [8]byte
		n = ((bd >> 2) & 0x7) + 1
		i2 = i + n
		copy(btmp[8-n:], bs[i:i2])

		if bs[i]&(1<<7) != 0 {
			copy(btmp[0:8-n], bsAll0xff)

		}
		i = i2
		tsec = int64(bigen.Uint64(btmp[:]))
	}
	if bd&(1<<6) != 0 {
		var btmp [4]byte
		n = (bd & 0x3) + 1
		i2 = i + n
		copy(btmp[4-n:], bs[i:i2])
		i = i2
		tnsec = bigen.Uint32(btmp[:])
	}
	if bd&(1<<5) == 0 {
		tt = time.Unix(tsec, int64(tnsec)).UTC()
		return
	}

	i2 = i + 2
	tz = bigen.Uint16(bs[i:i2])
	i = i2

	if tz&(1<<13) == 0 { // positive
		tz = tz & 0x3fff //clear 2 MSBs: dst bits
	} else { // negative
		tz = tz | 0xc000 //set 2 MSBs: dst bits

	}
	tzint := int16(tz)
	if tzint == 0 {
		tt = time.Unix(tsec, int64(tnsec)).UTC()
	} else {

		tt = time.Unix(tsec, int64(tnsec)).In(time.FixedZone("", int(tzint)*60))
	}
	return
}

func timeLocUTCName(tzint int16) string {
	if tzint == 0 {
		return "UTC"
	}
	var tzname = []byte("UTC+00:00")

	var tzhr, tzmin int16
	if tzint < 0 {
		tzname[3] = '-' // (TODO: verify. this works here)
		tzhr, tzmin = -tzint/60, (-tzint)%60
	} else {
		tzhr, tzmin = tzint/60, tzint%60
	}
	tzname[4] = timeDigits[tzhr/10]
	tzname[5] = timeDigits[tzhr%10]
	tzname[7] = timeDigits[tzmin/10]
	tzname[8] = timeDigits[tzmin%10]
	return string(tzname)

}
