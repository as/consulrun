// Copyright 2016 The Go Authors. All rights reserved.

package bpf

import (
	"errors"
	"fmt"
)

type VM struct {
	filter []Instruction
}

func NewVM(filter []Instruction) (*VM, error) {
	if len(filter) == 0 {
		return nil, errors.New("one or more Instructions must be specified")
	}

	for i, ins := range filter {
		check := len(filter) - (i + 1)
		switch ins := ins.(type) {

		case Jump:
			if check <= int(ins.Skip) {
				return nil, fmt.Errorf("cannot jump %d instructions; jumping past program bounds", ins.Skip)
			}
		case JumpIf:
			if check <= int(ins.SkipTrue) {
				return nil, fmt.Errorf("cannot jump %d instructions in true case; jumping past program bounds", ins.SkipTrue)
			}
			if check <= int(ins.SkipFalse) {
				return nil, fmt.Errorf("cannot jump %d instructions in false case; jumping past program bounds", ins.SkipFalse)
			}

		case ALUOpConstant:
			if ins.Val != 0 {
				break
			}

			switch ins.Op {
			case ALUOpDiv, ALUOpMod:
				return nil, errors.New("cannot divide by zero using ALUOpConstant")
			}

		case LoadExtension:
			switch ins.Num {
			case ExtLen:
			default:
				return nil, fmt.Errorf("extension %d not implemented", ins.Num)
			}
		}
	}

	switch filter[len(filter)-1].(type) {
	case RetA, RetConstant:
	default:
		return nil, errors.New("BPF program must end with RetA or RetConstant")
	}

	_, err := Assemble(filter)

	return &VM{
		filter: filter,
	}, err
}

func (v *VM) Run(in []byte) (int, error) {
	var (
		regA       uint32
		regX       uint32
		regScratch [16]uint32

		ok = true
	)

	for i := 0; i < len(v.filter) && ok; i++ {
		ins := v.filter[i]

		switch ins := ins.(type) {
		case ALUOpConstant:
			regA = aluOpConstant(ins, regA)
		case ALUOpX:
			regA, ok = aluOpX(ins, regA, regX)
		case Jump:
			i += int(ins.Skip)
		case JumpIf:
			jump := jumpIf(ins, regA)
			i += jump
		case LoadAbsolute:
			regA, ok = loadAbsolute(ins, in)
		case LoadConstant:
			regA, regX = loadConstant(ins, regA, regX)
		case LoadExtension:
			regA = loadExtension(ins, in)
		case LoadIndirect:
			regA, ok = loadIndirect(ins, in, regX)
		case LoadMemShift:
			regX, ok = loadMemShift(ins, in)
		case LoadScratch:
			regA, regX = loadScratch(ins, regScratch, regA, regX)
		case RetA:
			return int(regA), nil
		case RetConstant:
			return int(ins.Val), nil
		case StoreScratch:
			regScratch = storeScratch(ins, regScratch, regA, regX)
		case TAX:
			regX = regA
		case TXA:
			regA = regX
		default:
			return 0, fmt.Errorf("unknown Instruction at index %d: %T", i, ins)
		}
	}

	return 0, nil
}
