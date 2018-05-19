// Copyright 2016 The Go Authors. All rights reserved.

package bidi

import "unicode/utf8"

type Properties struct {
	entry uint8
	last  uint8
}

var trie = newBidiTrie(0)

//

func (p Properties) Class() Class {
	c := Class(p.entry & 0x0F)
	if c == Control {
		c = controlByteToClass[p.last&0xF]
	}
	return c
}

func (p Properties) IsBracket() bool { return p.entry&0xF0 != 0 }

func (p Properties) IsOpeningBracket() bool { return p.entry&openMask != 0 }

func (p Properties) reverseBracket(r rune) rune {
	return xorMasks[p.entry>>xorMaskShift] ^ r
}

var controlByteToClass = [16]Class{
	0xD: LRO, // U+202D LeftToRightOverride,
	0xE: RLO, // U+202E RightToLeftOverride,
	0xA: LRE, // U+202A LeftToRightEmbedding,
	0xB: RLE, // U+202B RightToLeftEmbedding,
	0xC: PDF, // U+202C PopDirectionalFormat,
	0x6: LRI, // U+2066 LeftToRightIsolate,
	0x7: RLI, // U+2067 RightToLeftIsolate,
	0x8: FSI, // U+2068 FirstStrongIsolate,
	0x9: PDI, // U+2069 PopDirectionalIsolate,
}

func LookupRune(r rune) (p Properties, size int) {
	var buf [4]byte
	n := utf8.EncodeRune(buf[:], r)
	return Lookup(buf[:n])
}

func Lookup(s []byte) (p Properties, sz int) {
	c0 := s[0]
	switch {
	case c0 < 0x80: // is ASCII
		return Properties{entry: bidiValues[c0]}, 1
	case c0 < 0xC2:
		return Properties{}, 1
	case c0 < 0xE0: // 2-byte UTF-8
		if len(s) < 2 {
			return Properties{}, 0
		}
		i := bidiIndex[c0]
		c1 := s[1]
		if c1 < 0x80 || 0xC0 <= c1 {
			return Properties{}, 1
		}
		return Properties{entry: trie.lookupValue(uint32(i), c1)}, 2
	case c0 < 0xF0: // 3-byte UTF-8
		if len(s) < 3 {
			return Properties{}, 0
		}
		i := bidiIndex[c0]
		c1 := s[1]
		if c1 < 0x80 || 0xC0 <= c1 {
			return Properties{}, 1
		}
		o := uint32(i)<<6 + uint32(c1)
		i = bidiIndex[o]
		c2 := s[2]
		if c2 < 0x80 || 0xC0 <= c2 {
			return Properties{}, 1
		}
		return Properties{entry: trie.lookupValue(uint32(i), c2), last: c2}, 3
	case c0 < 0xF8: // 4-byte UTF-8
		if len(s) < 4 {
			return Properties{}, 0
		}
		i := bidiIndex[c0]
		c1 := s[1]
		if c1 < 0x80 || 0xC0 <= c1 {
			return Properties{}, 1
		}
		o := uint32(i)<<6 + uint32(c1)
		i = bidiIndex[o]
		c2 := s[2]
		if c2 < 0x80 || 0xC0 <= c2 {
			return Properties{}, 1
		}
		o = uint32(i)<<6 + uint32(c2)
		i = bidiIndex[o]
		c3 := s[3]
		if c3 < 0x80 || 0xC0 <= c3 {
			return Properties{}, 1
		}
		return Properties{entry: trie.lookupValue(uint32(i), c3)}, 4
	}

	return Properties{}, 1
}

func LookupString(s string) (p Properties, sz int) {
	c0 := s[0]
	switch {
	case c0 < 0x80: // is ASCII
		return Properties{entry: bidiValues[c0]}, 1
	case c0 < 0xC2:
		return Properties{}, 1
	case c0 < 0xE0: // 2-byte UTF-8
		if len(s) < 2 {
			return Properties{}, 0
		}
		i := bidiIndex[c0]
		c1 := s[1]
		if c1 < 0x80 || 0xC0 <= c1 {
			return Properties{}, 1
		}
		return Properties{entry: trie.lookupValue(uint32(i), c1)}, 2
	case c0 < 0xF0: // 3-byte UTF-8
		if len(s) < 3 {
			return Properties{}, 0
		}
		i := bidiIndex[c0]
		c1 := s[1]
		if c1 < 0x80 || 0xC0 <= c1 {
			return Properties{}, 1
		}
		o := uint32(i)<<6 + uint32(c1)
		i = bidiIndex[o]
		c2 := s[2]
		if c2 < 0x80 || 0xC0 <= c2 {
			return Properties{}, 1
		}
		return Properties{entry: trie.lookupValue(uint32(i), c2), last: c2}, 3
	case c0 < 0xF8: // 4-byte UTF-8
		if len(s) < 4 {
			return Properties{}, 0
		}
		i := bidiIndex[c0]
		c1 := s[1]
		if c1 < 0x80 || 0xC0 <= c1 {
			return Properties{}, 1
		}
		o := uint32(i)<<6 + uint32(c1)
		i = bidiIndex[o]
		c2 := s[2]
		if c2 < 0x80 || 0xC0 <= c2 {
			return Properties{}, 1
		}
		o = uint32(i)<<6 + uint32(c2)
		i = bidiIndex[o]
		c3 := s[3]
		if c3 < 0x80 || 0xC0 <= c3 {
			return Properties{}, 1
		}
		return Properties{entry: trie.lookupValue(uint32(i), c3)}, 4
	}

	return Properties{}, 1
}
