// Copyright 2016 The Go Authors. All rights reserved.

package bpf

import "fmt"

//
func Assemble(insts []Instruction) ([]RawInstruction, error) {
	ret := make([]RawInstruction, len(insts))
	var err error
	for i, inst := range insts {
		ret[i], err = inst.Assemble()
		if err != nil {
			return nil, fmt.Errorf("assembling instruction %d: %s", i+1, err)
		}
	}
	return ret, nil
}

func Disassemble(raw []RawInstruction) (insts []Instruction, allDecoded bool) {
	insts = make([]Instruction, len(raw))
	allDecoded = true
	for i, r := range raw {
		insts[i] = r.Disassemble()
		if _, ok := insts[i].(RawInstruction); ok {
			allDecoded = false
		}
	}
	return insts, allDecoded
}
