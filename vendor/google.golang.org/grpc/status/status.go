/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//
package status

import (
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
)

type statusError spb.Status

func (se *statusError) Error() string {
	p := (*spb.Status)(se)
	return fmt.Sprintf("rpc error: code = %s desc = %s", codes.Code(p.GetCode()), p.GetMessage())
}

func (se *statusError) status() *Status {
	return &Status{s: (*spb.Status)(se)}
}

type Status struct {
	s *spb.Status
}

func (s *Status) Code() codes.Code {
	if s == nil || s.s == nil {
		return codes.OK
	}
	return codes.Code(s.s.Code)
}

func (s *Status) Message() string {
	if s == nil || s.s == nil {
		return ""
	}
	return s.s.Message
}

func (s *Status) Proto() *spb.Status {
	if s == nil {
		return nil
	}
	return proto.Clone(s.s).(*spb.Status)
}

func (s *Status) Err() error {
	if s.Code() == codes.OK {
		return nil
	}
	return (*statusError)(s.s)
}

func New(c codes.Code, msg string) *Status {
	return &Status{s: &spb.Status{Code: int32(c), Message: msg}}
}

func Newf(c codes.Code, format string, a ...interface{}) *Status {
	return New(c, fmt.Sprintf(format, a...))
}

func Error(c codes.Code, msg string) error {
	return New(c, msg).Err()
}

func Errorf(c codes.Code, format string, a ...interface{}) error {
	return Error(c, fmt.Sprintf(format, a...))
}

func ErrorProto(s *spb.Status) error {
	return FromProto(s).Err()
}

func FromProto(s *spb.Status) *Status {
	return &Status{s: proto.Clone(s).(*spb.Status)}
}

func FromError(err error) (s *Status, ok bool) {
	if err == nil {
		return &Status{s: &spb.Status{Code: int32(codes.OK)}}, true
	}
	if se, ok := err.(*statusError); ok {
		return se.status(), true
	}
	return nil, false
}

func (s *Status) WithDetails(details ...proto.Message) (*Status, error) {
	if s.Code() == codes.OK {
		return nil, errors.New("no error details for status with code OK")
	}

	p := s.Proto()
	for _, detail := range details {
		any, err := ptypes.MarshalAny(detail)
		if err != nil {
			return nil, err
		}
		p.Details = append(p.Details, any)
	}
	return &Status{s: p}, nil
}

func (s *Status) Details() []interface{} {
	if s == nil || s.s == nil {
		return nil
	}
	details := make([]interface{}, 0, len(s.s.Details))
	for _, any := range s.s.Details {
		detail := &ptypes.DynamicAny{}
		if err := ptypes.UnmarshalAny(any, detail); err != nil {
			details = append(details, err)
			continue
		}
		details = append(details, detail.Message)
	}
	return details
}

func Code(err error) codes.Code {

	if err == nil {
		return codes.OK
	}
	if se, ok := err.(*statusError); ok {
		return se.status().Code()
	}
	return codes.Unknown
}
