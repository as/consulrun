package dns

var StringToType = reverseInt16(TypeToString)

var StringToClass = reverseInt16(ClassToString)

var StringToOpcode = reverseInt(OpcodeToString)

var StringToRcode = reverseInt(RcodeToString)

func reverseInt8(m map[uint8]string) map[string]uint8 {
	n := make(map[string]uint8, len(m))
	for u, s := range m {
		n[s] = u
	}
	return n
}

func reverseInt16(m map[uint16]string) map[string]uint16 {
	n := make(map[string]uint16, len(m))
	for u, s := range m {
		n[s] = u
	}
	return n
}

func reverseInt(m map[int]string) map[string]int {
	n := make(map[string]int, len(m))
	for u, s := range m {
		n[s] = u
	}
	return n
}
