//go:build dynamodb
// +build dynamodb

package dynamodb

type Modifier = string //

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
	Prepend Modifier = "_prepend"
	Append  Modifier = "_append"
	Set     Modifier = "_set"
)
