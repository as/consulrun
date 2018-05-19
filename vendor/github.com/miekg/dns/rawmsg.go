package dns

import "encoding/binary"

func rawSetRdlength(msg []byte, off, end int) bool {
	l := len(msg)
Loop:
	for {
		if off+1 > l {
			return false
		}
		c := int(msg[off])
		off++
		switch c & 0xC0 {
		case 0x00:
			if c == 0x00 {

				break Loop
			}
			if off+c > l {
				return false
			}
			off += c

		case 0xC0:

			off++
			break Loop
		}
	}

	off += 2 + 2 + 4
	if off+2 > l {
		return false
	}

	rdatalen := end - (off + 2)
	if rdatalen > 0xFFFF {
		return false
	}
	binary.BigEndian.PutUint16(msg[off:], uint16(rdatalen))
	return true
}
