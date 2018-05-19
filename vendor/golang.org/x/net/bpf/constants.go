// Copyright 2016 The Go Authors. All rights reserved.

package bpf

type Register uint16

const (
	RegA Register = iota

	RegX
)

type ALUOp uint16

const (
	ALUOpAdd ALUOp = iota << 4
	ALUOpSub
	ALUOpMul
	ALUOpDiv
	ALUOpOr
	ALUOpAnd
	ALUOpShiftLeft
	ALUOpShiftRight
	aluOpNeg // Not exported because it's the only unary ALU operation, and gets its own instruction type.
	ALUOpMod
	ALUOpXor
)

type JumpTest uint16

const (
	JumpEqual JumpTest = iota

	JumpNotEqual

	JumpGreaterThan

	JumpLessThan

	JumpGreaterOrEqual

	JumpLessOrEqual

	JumpBitsSet

	JumpBitsNotSet
)

//
//
type Extension int

const (
	extOffset = -0x1000

	ExtLen Extension = 1

	ExtProto Extension = 0

	//

	ExtType Extension = 4

	ExtPayloadOffset Extension = 52

	ExtInterfaceIndex Extension = 8

	ExtNetlinkAttr Extension = 12

	ExtNetlinkAttrNested Extension = 16

	ExtMark Extension = 20

	ExtQueue Extension = 24

	ExtLinkLayerType Extension = 28

	//

	ExtRXHash Extension = 32

	ExtCPUID Extension = 36

	ExtVLANTag Extension = 44

	//

	ExtVLANTagPresent Extension = 48

	ExtVLANProto Extension = 60

	ExtRand Extension = 56
)

const (
	opMaskCls uint16 = 0x7

	opMaskLoadDest  = 0x01
	opMaskLoadWidth = 0x18
	opMaskLoadMode  = 0xe0

	opMaskOperandSrc = 0x08
	opMaskOperator   = 0xf0

	opMaskJumpConst = 0x0f
	opMaskJumpCond  = 0xf0
)

const (
	opClsLoadA uint16 = iota

	opClsLoadX

	opClsStoreA

	opClsStoreX

	opClsALU

	opClsJump

	opClsReturn

	opClsMisc
)

const (
	opAddrModeImmediate uint16 = iota << 5
	opAddrModeAbsolute
	opAddrModeIndirect
	opAddrModeScratch
	opAddrModePacketLen // actually an extension, not an addressing mode.
	opAddrModeMemShift
)

const (
	opLoadWidth4 uint16 = iota << 3
	opLoadWidth2
	opLoadWidth1
)

const (
	opALUSrcConstant uint16 = iota << 3
	opALUSrcX
)

const (
	opJumpAlways = iota << 4
	opJumpEqual
	opJumpGT
	opJumpGE
	opJumpSet
)

const (
	opRetSrcConstant uint16 = iota << 4
	opRetSrcA
)

const (
	opMiscTAX = 0x00
	opMiscTXA = 0x80
)
