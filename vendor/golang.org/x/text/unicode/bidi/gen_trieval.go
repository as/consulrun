// Copyright 2015 The Go Authors. All rights reserved.

// +build ignore

package main

type Class uint

const (
	L       Class = iota // LeftToRight
	R                    // RightToLeft
	EN                   // EuropeanNumber
	ES                   // EuropeanSeparator
	ET                   // EuropeanTerminator
	AN                   // ArabicNumber
	CS                   // CommonSeparator
	B                    // ParagraphSeparator
	S                    // SegmentSeparator
	WS                   // WhiteSpace
	ON                   // OtherNeutral
	BN                   // BoundaryNeutral
	NSM                  // NonspacingMark
	AL                   // ArabicLetter
	Control              // Control LRO - PDI

	numClass

	LRO // LeftToRightOverride
	RLO // RightToLeftOverride
	LRE // LeftToRightEmbedding
	RLE // RightToLeftEmbedding
	PDF // PopDirectionalFormat
	LRI // LeftToRightIsolate
	RLI // RightToLeftIsolate
	FSI // FirstStrongIsolate
	PDI // PopDirectionalIsolate

	unknownClass = ^Class(0)
)

var controlToClass = map[rune]Class{
	0x202D: LRO, // LeftToRightOverride,
	0x202E: RLO, // RightToLeftOverride,
	0x202A: LRE, // LeftToRightEmbedding,
	0x202B: RLE, // RightToLeftEmbedding,
	0x202C: PDF, // PopDirectionalFormat,
	0x2066: LRI, // LeftToRightIsolate,
	0x2067: RLI, // RightToLeftIsolate,
	0x2068: FSI, // FirstStrongIsolate,
	0x2069: PDI, // PopDirectionalIsolate,
}

const (
	openMask     = 0x10
	xorMaskShift = 5
)
