// Code generated by protoc-gen-go. DO NOT EDIT.

/*
Package timestamp is a generated protocol buffer package.

It is generated from these files:
	google/protobuf/timestamp.proto

It has these top-level messages:
	Timestamp
*/
package timestamp

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
//         .setNanos((int) ((millis % 1000) * 1000000)).build();
//
//
//
//
//
//
//
//
//
type Timestamp struct {
	Seconds int64 `protobuf:"varint,1,opt,name=seconds" json:"seconds,omitempty"`

	Nanos int32 `protobuf:"varint,2,opt,name=nanos" json:"nanos,omitempty"`
}

func (m *Timestamp) Reset()                    { *m = Timestamp{} }
func (m *Timestamp) String() string            { return proto.CompactTextString(m) }
func (*Timestamp) ProtoMessage()               {}
func (*Timestamp) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }
func (*Timestamp) XXX_WellKnownType() string   { return "Timestamp" }

func (m *Timestamp) GetSeconds() int64 {
	if m != nil {
		return m.Seconds
	}
	return 0
}

func (m *Timestamp) GetNanos() int32 {
	if m != nil {
		return m.Nanos
	}
	return 0
}

func init() {
	proto.RegisterType((*Timestamp)(nil), "google.protobuf.Timestamp")
}

func init() { proto.RegisterFile("google/protobuf/timestamp.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{

	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x4f, 0xcf, 0xcf, 0x4f,
	0xcf, 0x49, 0xd5, 0x2f, 0x28, 0xca, 0x2f, 0xc9, 0x4f, 0x2a, 0x4d, 0xd3, 0x2f, 0xc9, 0xcc, 0x4d,
	0x2d, 0x2e, 0x49, 0xcc, 0x2d, 0xd0, 0x03, 0x0b, 0x09, 0xf1, 0x43, 0x14, 0xe8, 0xc1, 0x14, 0x28,
	0x59, 0x73, 0x71, 0x86, 0xc0, 0xd4, 0x08, 0x49, 0x70, 0xb1, 0x17, 0xa7, 0x26, 0xe7, 0xe7, 0xa5,
	0x14, 0x4b, 0x30, 0x2a, 0x30, 0x6a, 0x30, 0x07, 0xc1, 0xb8, 0x42, 0x22, 0x5c, 0xac, 0x79, 0x89,
	0x79, 0xf9, 0xc5, 0x12, 0x4c, 0x0a, 0x8c, 0x1a, 0xac, 0x41, 0x10, 0x8e, 0x53, 0x1d, 0x97, 0x70,
	0x72, 0x7e, 0xae, 0x1e, 0x9a, 0x99, 0x4e, 0x7c, 0x70, 0x13, 0x03, 0x40, 0x42, 0x01, 0x8c, 0x51,
	0xda, 0xe9, 0x99, 0x25, 0x19, 0xa5, 0x49, 0x7a, 0xc9, 0xf9, 0xb9, 0xfa, 0xe9, 0xf9, 0x39, 0x89,
	0x79, 0xe9, 0x08, 0x27, 0x16, 0x94, 0x54, 0x16, 0xa4, 0x16, 0x23, 0x5c, 0xfa, 0x83, 0x91, 0x71,
	0x11, 0x13, 0xb3, 0x7b, 0x80, 0xd3, 0x2a, 0x26, 0x39, 0x77, 0x88, 0xc9, 0x01, 0x50, 0xb5, 0x7a,
	0xe1, 0xa9, 0x39, 0x39, 0xde, 0x79, 0xf9, 0xe5, 0x79, 0x21, 0x20, 0x3d, 0x49, 0x6c, 0x60, 0x43,
	0x8c, 0x01, 0x01, 0x00, 0x00, 0xff, 0xff, 0xbc, 0x77, 0x4a, 0x07, 0xf7, 0x00, 0x00, 0x00,
}
