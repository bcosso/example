package main

import (
	"fmt"
	"strings"

	"github.com/bcosso/sqlparserproject"
)

// ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func main() {

	sql := "select field1, field2 from table01 where field3 = 1 or (field4 > 5 and field5 = 'active')"
	bin := CompileSQL(sql)

	row := make(map[string]interface{})

	RunVM(row, bin)
}

///////////////////////////////////////////////////////////////////Test Only//////////////////////////////////////////////////////////////////////////

type ExecuteActions interface {
	ExecuteJoin(tree *sqlparserproject.CommandTree)
}
type CustomAction struct {
	ExecuteActions
}

var _hAction ExecuteActions

func SetActionHandler(hAction ExecuteActions) {
	_hAction = hAction
}

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
func CompileSQL(query string) Executable {
	var bin Executable

	ast := sqlparserproject.ExecuteParsingProcess(query)

	ReadThrough(ast, &bin)

	mocProgram := []uint32{
		0x00010042,
		0x00120010,
		0x00030000,
	}

	bin.Program = mocProgram

	return bin

}

func ReadThrough(tree sqlparserproject.CommandTree, bin *Executable) {
	for _, leaf := range tree.CommandParts {
		switch strings.ToLower(leaf.TypeToken) {
		case "select":
			break
		case "join":
			_hAction.ExecuteJoin(&tree)
		default:
			break
		}

	}

}
