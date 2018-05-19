// Copyright 2017 The Go Authors. All rights reserved.

package bpf

type Setter interface {
	SetBPF(filter []RawInstruction) error
}
