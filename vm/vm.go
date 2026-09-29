package main

import "fmt"

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func main() {
	var bin Executable

	program := []uint32{
		0x00010042,
		0x00120010,
		0x00030000,
	}

	bin.Program = program
	row := make(map[string]interface{})

	RunVM(row, bin)
}

///////////////////////////////////////////////////////////////////Test Only//////////////////////////////////////////////////////////////////////////

type Executable struct {
	Program []uint32
	Data    []string
}

const (
	MOV   uint16 = 0x0001
	MOV_A        = 0x000A
	MOV_B        = 0x000B
	PUSH         = 0x0011
	POP          = 0x0012
	CMP          = 0x0013
	JE           = 0x0014
	JMP          = 0x0015
	JG           = 0x0016
)

func RunVM(row map[string]interface{}, bin Executable) {
	for _, line := range bin.Program {

		opcode := line >> 16
		arg := line & 0xFFFF

		fmt.Println(opcode == POP)
		fmt.Println(arg == 0x000010)

	}

}
