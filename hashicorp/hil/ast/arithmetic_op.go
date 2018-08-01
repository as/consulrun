package ast

type ArithmeticOp int

const (
	ArithmeticOpInvalid ArithmeticOp = 0
	ArithmeticOpAdd     ArithmeticOp = iota
	ArithmeticOpSub
	ArithmeticOpMul
	ArithmeticOpDiv
	ArithmeticOpMod
)
