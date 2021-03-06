// Code generated by protoc-gen-go. DO NOT EDIT.

/*
Package duration is a generated protocol buffer package.

It is generated from these files:
	google/protobuf/duration.proto

It has these top-level messages:
	Duration
*/
package duration

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

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
//
type Duration struct {
	Seconds int64 `protobuf:"varint,1,opt,name=seconds" json:"seconds,omitempty"`

	Nanos int32 `protobuf:"varint,2,opt,name=nanos" json:"nanos,omitempty"`
}

func (m *Duration) Reset()                    { *m = Duration{} }
func (m *Duration) String() string            { return proto.CompactTextString(m) }
func (*Duration) ProtoMessage()               {}
func (*Duration) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }
func (*Duration) XXX_WellKnownType() string   { return "Duration" }

func (m *Duration) GetSeconds() int64 {
	if m != nil {
		return m.Seconds
	}
	return 0
}

func (m *Duration) GetNanos() int32 {
	if m != nil {
		return m.Nanos
	}
	return 0
}

func init() {
	proto.RegisterType((*Duration)(nil), "google.protobuf.Duration")
}

func init() { proto.RegisterFile("google/protobuf/duration.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{

	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x4b, 0xcf, 0xcf, 0x4f,
	0xcf, 0x49, 0xd5, 0x2f, 0x28, 0xca, 0x2f, 0xc9, 0x4f, 0x2a, 0x4d, 0xd3, 0x4f, 0x29, 0x2d, 0x4a,
	0x2c, 0xc9, 0xcc, 0xcf, 0xd3, 0x03, 0x8b, 0x08, 0xf1, 0x43, 0xe4, 0xf5, 0x60, 0xf2, 0x4a, 0x56,
	0x5c, 0x1c, 0x2e, 0x50, 0x25, 0x42, 0x12, 0x5c, 0xec, 0xc5, 0xa9, 0xc9, 0xf9, 0x79, 0x29, 0xc5,
	0x12, 0x8c, 0x0a, 0x8c, 0x1a, 0xcc, 0x41, 0x30, 0xae, 0x90, 0x08, 0x17, 0x6b, 0x5e, 0x62, 0x5e,
	0x7e, 0xb1, 0x04, 0x93, 0x02, 0xa3, 0x06, 0x6b, 0x10, 0x84, 0xe3, 0x54, 0xc3, 0x25, 0x9c, 0x9c,
	0x9f, 0xab, 0x87, 0x66, 0xa4, 0x13, 0x2f, 0xcc, 0xc0, 0x00, 0x90, 0x48, 0x00, 0x63, 0x94, 0x56,
	0x7a, 0x66, 0x49, 0x46, 0x69, 0x92, 0x5e, 0x72, 0x7e, 0xae, 0x7e, 0x7a, 0x7e, 0x4e, 0x62, 0x5e,
	0x3a, 0xc2, 0x7d, 0x05, 0x25, 0x95, 0x05, 0xa9, 0xc5, 0x70, 0x67, 0xfe, 0x60, 0x64, 0x5c, 0xc4,
	0xc4, 0xec, 0x1e, 0xe0, 0xb4, 0x8a, 0x49, 0xce, 0x1d, 0x62, 0x6e, 0x00, 0x54, 0xa9, 0x5e, 0x78,
	0x6a, 0x4e, 0x8e, 0x77, 0x5e, 0x7e, 0x79, 0x5e, 0x08, 0x48, 0x4b, 0x12, 0x1b, 0xd8, 0x0c, 0x63,
	0x40, 0x00, 0x00, 0x00, 0xff, 0xff, 0xdc, 0x84, 0x30, 0xff, 0xf3, 0x00, 0x00, 0x00,
}
