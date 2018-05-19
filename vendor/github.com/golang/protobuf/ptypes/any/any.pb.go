// Code generated by protoc-gen-go. DO NOT EDIT.

/*
Package any is a generated protocol buffer package.

It is generated from these files:
	google/protobuf/any.proto

It has these top-level messages:
	Any
*/
package any

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
//
//
type Any struct {

	//

	//

	//

	//
	TypeUrl string `protobuf:"bytes,1,opt,name=type_url,json=typeUrl" json:"type_url,omitempty"`

	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (m *Any) Reset()                    { *m = Any{} }
func (m *Any) String() string            { return proto.CompactTextString(m) }
func (*Any) ProtoMessage()               {}
func (*Any) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }
func (*Any) XXX_WellKnownType() string   { return "Any" }

func (m *Any) GetTypeUrl() string {
	if m != nil {
		return m.TypeUrl
	}
	return ""
}

func (m *Any) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func init() {
	proto.RegisterType((*Any)(nil), "google.protobuf.Any")
}

func init() { proto.RegisterFile("google/protobuf/any.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{

	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x92, 0x4c, 0xcf, 0xcf, 0x4f,
	0xcf, 0x49, 0xd5, 0x2f, 0x28, 0xca, 0x2f, 0xc9, 0x4f, 0x2a, 0x4d, 0xd3, 0x4f, 0xcc, 0xab, 0xd4,
	0x03, 0x73, 0x84, 0xf8, 0x21, 0x52, 0x7a, 0x30, 0x29, 0x25, 0x33, 0x2e, 0x66, 0xc7, 0xbc, 0x4a,
	0x21, 0x49, 0x2e, 0x8e, 0x92, 0xca, 0x82, 0xd4, 0xf8, 0xd2, 0xa2, 0x1c, 0x09, 0x46, 0x05, 0x46,
	0x0d, 0xce, 0x20, 0x76, 0x10, 0x3f, 0xb4, 0x28, 0x47, 0x48, 0x84, 0x8b, 0xb5, 0x2c, 0x31, 0xa7,
	0x34, 0x55, 0x82, 0x49, 0x81, 0x51, 0x83, 0x27, 0x08, 0xc2, 0x71, 0xca, 0xe7, 0x12, 0x4e, 0xce,
	0xcf, 0xd5, 0x43, 0x33, 0xce, 0x89, 0xc3, 0x31, 0xaf, 0x32, 0x00, 0xc4, 0x09, 0x60, 0x8c, 0x52,
	0x4d, 0xcf, 0x2c, 0xc9, 0x28, 0x4d, 0xd2, 0x4b, 0xce, 0xcf, 0xd5, 0x4f, 0xcf, 0xcf, 0x49, 0xcc,
	0x4b, 0x47, 0xb8, 0xa8, 0x00, 0x64, 0x7a, 0x31, 0xc8, 0x61, 0x8b, 0x98, 0x98, 0xdd, 0x03, 0x9c,
	0x56, 0x31, 0xc9, 0xb9, 0x43, 0x8c, 0x0a, 0x80, 0x2a, 0xd1, 0x0b, 0x4f, 0xcd, 0xc9, 0xf1, 0xce,
	0xcb, 0x2f, 0xcf, 0x0b, 0x01, 0x29, 0x4d, 0x62, 0x03, 0xeb, 0x35, 0x06, 0x04, 0x00, 0x00, 0xff,
	0xff, 0x13, 0xf8, 0xe8, 0x42, 0xdd, 0x00, 0x00, 0x00,
}