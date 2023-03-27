package mut

import (
	"fmt"
)

type Modifier = string // aka update-expression in dynamodb (almost)

// standard

const (
	//
	SET      Modifier = "__SET"      // set col = <value>
	SETNULL  Modifier = "__NULL"     // set col = <value>
	SUBTRACT Modifier = "__SUBTRACT" // set col = col - <value> or Delete from Set
	ADD      Modifier = "__ADD"      // set col = col - <num>
	MULTIPLY Modifier = "__MULTIPLY" // set col = col x <value>
	//LITERAL  Modifier = "__Literal" // Wrong use. Literal is a struct tag used in queries
	REMOVE   Modifier = "__REMOVE" // remove attribute
	IsKey    Modifier = "__KEY"    // "__KEY"
	KEY      Modifier = "__KEY"
	IsFilter Modifier = "__FILTER" //used in Attribute() to define a filter operation.Equivalent methods: Filter(),OrFilter()
	FILTER   Modifier = "__FILTER"
	//
	// use in Key, Filter methods only
	EQ  Modifier = "__EQ"
	LT  Modifier = "__LT"
	LE  Modifier = "__LE"
	GT  Modifier = "__GT"
	GE  Modifier = "__GE"
	NE  Modifier = "__NE"
	IN  Modifier = "__IN"
	ANY Modifier = "__ANY"
	NOT Modifier = "__NOT"
	// Append                       // update by appending to array/list attribute
	// Prepend                      //  update by prepending to array/list attribute
	// Remove                       // remove attribute
	// query portion of mutation
)

// dynamodb

const (
	//
	// Dynmaodb marshalling
	// in MethodDB all slice types are converted to List db type. To override use the *SET modifiers. This is in contrast to Dynamodb
	// which converts all slice types to List except for binay slices which are converted to binary SET (for some reason).
	// To emulate Dynamodb default behaviour specify BinarySet modifer for all binary slices.
	NumberSet     Modifier = "_NUMBERSET"
	StringSet     Modifier = "_STRINGSET"
	BinarySet     Modifier = "_BINARYSET" // default for [][]byte but overriden by txUpdate() txPut()
	Omitempty     Modifier = "_omitempty"
	Omitemptyelem Modifier = "_omitemptyelem"
	Nullempty     Modifier = "_nullempty"
	Nullemptyelem Modifier = "_nullemptyelem"
	String        Modifier = "_string"
	// applies to array types
	PREPEND Modifier = "_PREPEND"  // prepend elements to List
	APPEND  Modifier = "_APPEND"   // append elements to List
	DELETE  Modifier = "_SUBTRACT" // delete element in a Set
)

type ModifierS []Modifier

func (m *ModifierS) Exists(in Modifier) bool {
	for _, v := range *m {
		if v == in {
			return true
		}
	}
	return false
}

func (m *ModifierS) Assign(in Modifier) {

	if !m.Exists(in) {
		*m = append(*m, in)
	}

}

func (m *ModifierS) Delete(in Modifier) {
	var found []int

	for i, v := range *m {
		if v == in {
			found = append(found, i)
		}
	}

	if len(found) == 1 && len(*m) == 1 {
		*m = nil
		return
	}
	for k, i := range found {
		copy((*m)[i-k:], (*m)[i+1-k:])
		*m = (*m)[:len(*m)-1]
	}
}

func (m *ModifierS) Replace(this Modifier, with Modifier) {
	for i, v := range *m {
		fmt.Println("v, this ", v, this, with)
		if v == this {
			(*m)[i] = with
			return
		}
	}
}

func (m *ModifierS) Validate(im *Mutation, in Modifier) {
	switch in {
	case APPEND:
		if m.Exists(SET) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix APPEND and SET"))
		}
		// if m.Exists(PREPEND) {
		// 	im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mox APPEND and PREPEND"))
		// }
	case SET:
		if m.Exists(APPEND) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix SET and APPEND"))
		}
		// if m.Exists(PREPEND) {
		// 	im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mox SET and PREPEND"))
		// }
	case ADD:
		if m.Exists(SUBTRACT) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix ADD and SUBTRACT"))
		}
		if m.Exists(MULTIPLY) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix ADD and MULTIPLY"))
		}
	case SUBTRACT:
		if m.Exists(ADD) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix ADD and SUBTRACT"))
		}
		if m.Exists(MULTIPLY) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix MULTIPLY and SUBTRACT"))
		}
	case MULTIPLY:
		if m.Exists(ADD) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix ADD and MULTIPLY"))
		}
		if m.Exists(SUBTRACT) {
			im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix MULTIPLY and SUBTRACT"))
		}
		// case IsKey:
		// 	if m.Exists(IsFilter) {
		// 		im.addErr(fmt.Errorf("Invalid combination of modifiers in AddAttribute(). Cannot mix IsKey and IsFilter"))
		// 	}
	}
}
