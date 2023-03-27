package mut

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/ros2hp/method-db/dbs"
	"github.com/ros2hp/method-db/key"
	"github.com/ros2hp/method-db/log"
	"github.com/ros2hp/method-db/tbl"
	"github.com/ros2hp/method-db/uuid"
)

type StdMut byte
type Cond byte
type BoolCd byte
type Attrty byte

const (
	NIL BoolCd = iota
	AND
	OR
)
const (
	Partition Attrty = iota + 1
	Sortkey
	Key
	Filter
)

//type OpTag Label

//type Modifier string

const (
	Merge StdMut = iota + 1
	Insert
	Delete
	Update // update performing "set =" operation etc
	TruncateTbl
)

type Operation = string

const (
	AttrExists Cond = iota
	AttrNotExists
)

func syslog(s string) {
	log.LogDebug("Mutation " + s)
}

func logAlert(s string) {
	log.LogAlert("Mutation " + s)
}

func (s StdMut) String() string {
	switch s {
	case Merge:
		return "merge"
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	}
	return "not-defined"
}

// func (c Cond) String() string {
// 	switch c {
// 	case AttrExists:
// 		return "Attribute_Exists"
// 	case AttrNotExists:
// 		return "Attribute_Not_Exists"
// 	}
// 	return "NA"
// }

var (
	err error
)

// Member defines all attributes in the Mutation, as either. Assigned during AddAttribute()
//
//	Set (used in update set), -- modify attribute in update
//	Append, Remove, Subtract, Add, Multiply -- all modify attribute in update
//	... now for where attributes...
//	IsKey in where predicate, Key()
//	IsFilter in where predicate, Filter(), AndFilter(), OrFilter()
//
// TODO hide Member fields - directly access from tx so needs to be exposed but not to world, so make it an internal (no need to be world too)
type Member struct {
	//sortk string
	name    string // attribute name: when contains "#",":"??
	Param   string // used in Spanner implementation. All value placements are identified by "@param"
	value   interface{}
	oper    Operation // SET, APPEND....
	array   bool      // set in AddAttribute(). Append modifier is valid to ie. is an array (List) type in Dynamodb
	mod     ModifierS // for update stmts only: default is to concat for Array type. When true will overide with set of array.
	literal string    // literal struct tag value - . alternative to value - replace attribute name with literal in query stmt.
	//
	aty    Attrty // attribute type, e.g. Key, Filter
	eqy    string // Scalar Opr: EQ, LE,...  Slice Opr: IN, ANY
	boolCd BoolCd // And, Or - appropriate for Filter only.
	//
	tags []string // struct tags
}

func (m *Member) GetTags() []string {
	return m.tags
}

func (m *Member) SetTags(t []string) {
	m.tags = t
}

func (m *Member) IsArray() bool {
	return m.array
}

func (m *Member) GetModifier() *ModifierS {
	return &m.mod
}

func (m *Member) SetModifier(mod_ ModifierS) {
	m.mod = mod_
}

func (m *Member) GetOperation() Operation {
	return m.oper
}

func (m *Member) IsKey() bool {
	switch m.aty {
	case Partition, Sortkey, Key:
		return true
	}
	return false
}

func (m *Member) IsPartitionKey() bool {
	return m.aty == Partition
}

func (m *Member) Name() string {
	return m.name
}

func (m *Member) Value() interface{} {
	return m.value
}

func (m *Member) SetValue(v interface{}) {
	m.value = v
}

// Aty emulates query aty -
// func (m *Member) Aty() Modifier {
// 	return m.mod
// }

// func (m *Member) Set() bool {
// 	return m.mod.Exists(Set)
// }

// // func (m *Member) Inc() bool {
// // 	return m.mod == Inc
// // }

// func (m *Member) Subtract() bool {
// 	return m.mod.Exists(Subtract)
// }

// func (m *Member) Add() bool {
// 	return m.mod.Exists(Add)
// }

// TODO: use IsLiteral ??? or change IsFilter, IsKey in mutation.

func (m *Member) Literal() string {
	return m.literal
}

func (m *Member) IsFilter() bool {
	return m.aty == Filter
}
func (m *Member) BoolCd() BoolCd {
	return m.boolCd
}

func (m *Member) GetOprStr() string {
	return m.eqy
}

// Examples of condition expression:
// "attribute_not_exists(Price)"    			"attribute_not_exists(%s)"
// "attribute_exists(ProductReviews.OneStar)".  "attribute_exists(%s)"
// "attribute_type(Color, :v_sub)"  			 "attribute_type(%s, %q)" 	  "attribute_type(%s, %g)"
// "begins_with(Pictures.FrontView, :v_sub)"
// "contains(Color, :v_sub)"
// "size(VideoClip) > :v_sub"

// type condition struct {
// 	cond  Cond        // ]ASZ
// 	attr  string      // =
// 	value interface{} // size
// }

type Option struct {
	Name string
	Val  interface{}
}

// func (c *condition) GetCond() Cond {
// 	return c.cond
// }

// func (c *condition) GetAttr() string {
// 	return c.attr
// }

// func (c *condition) GetValue() interface{} {
// 	return c.value
// }

type RtnMode string

const (
	RtnNone        RtnMode = "NONE"
	RtnAllOld      RtnMode = "ALL_OLD"
	RtnUpdatedOld  RtnMode = "UPDATED_OLD"
	RtnValueAllNew RtnMode = "ALL_NEW"
	RtnUpdatedNew  RtnMode = "UPDATED_NEW"
)

type Rtn struct {
	mode RtnMode
	val  interface{}
}

type Mutation struct {
	//dbHdl  db.Handle // error: import cycle setup between db & mut
	ms     []*Member   // AddAttribute - adds attribute mutation
	submit interface{} // use &struct{} definition to define []member. For Dynamodb, this is passed to MarshalMap, or MarshallListofMap
	//
	tag string // label for mutation. Potentially useful as a source of aggregation for performance statistics.
	//cd *condition
	// from Where() method
	where string
	// from Values() method
	values []interface{}
	// AndFilter, OrFilter counts
	af, of int
	//
	pKey    interface{}
	tbl     tbl.Name
	tblDesc *key.TabMeta
	// keys []db.TableKey  // table keys from database data dictionary
	// attr []db.TableAttr // table attribute names
	opr StdMut // update,insert(put), merge, delete
	//
	text     string      // alternate representation of a mutation e.g. sql
	prepStmt interface{} // some db's may optional "prepare" mutations before execution Three phase 1) single prepare stmt 2) multiple stmt executions 3) close stmt
	params   []interface{}
	err      []error
	config   []Option
	hint     string
	//
	sql string
	//
	rtn *Rtn // return data from mutation
}

type Mutations []dbs.Mutation //*Mutation

func (im *Mutations) GetMutation(i int) *Mutation {
	return (*im)[i].(*Mutation)
}

func (im *Mutations) NumMutations() int {
	return len(*im)
}

func NewInsert(tab tbl.Name, label ...string) *Mutation {

	if len(label) > 0 {
		return &Mutation{tbl: tab, opr: Insert, tag: label[0]}
	}
	return &Mutation{tbl: tab, opr: Insert}

}

func NewDelete(tab tbl.Name, label ...string) *Mutation {

	if len(label) > 0 {
		return &Mutation{tbl: tab, opr: Delete, tag: label[0]}
	}
	return &Mutation{tbl: tab, opr: Delete}

}

// NewMerge
// This operation is equivalent in a no-SQL Put operation as put will insert if new or update if present.
// However for SQL database it will perform an update, if not present, then insert.
func NewMerge(tab tbl.Name, label ...string) *Mutation {

	if len(label) > 0 {
		return &Mutation{tbl: tab, opr: Merge, tag: label[0]}
	}

	return &Mutation{tbl: tab, opr: Merge}
}

func NewUpdate(tab tbl.Name, label ...string) *Mutation {

	if len(label) > 0 {
		return &Mutation{tbl: tab, opr: Update, tag: label[0]}
	}
	return &Mutation{tbl: tab, opr: Update}
}

func Truncate(tab tbl.Name) *Mutation {

	return &Mutation{tbl: tab, opr: TruncateTbl}
}

// func NewMutationEventLog(table string, pk  opr interface{}) *Mutation {
// 	return &Mutation{tbl: table, pk: pk, sk: sk, opr: opr}
// }

func (m *Mutation) GetStatements() []dbs.Statement { return nil }

func (m *Mutation) GetMembers() []*Member {
	return m.ms
}

func (m *Mutation) GetMutateMembers() []*Member {
	var mb []*Member
	for _, v := range m.GetMembers() {
		if v.IsFilter() || v.IsKey() {
			continue
		}
		mb = append(mb, v)
	}
	return mb
}

func (m *Mutation) SetMembers(a []*Member) {
	m.ms = a
}
func SetMember(i int, ms []*Member, v interface{}) {
	ms[i].value = v
}

// func (m *Mutation) keys() []*Member {
// 	var k []*Member
// 	for _, v := range m.GetMembers() {
// 		if v.IsKey() {
// 			k = append(k, v)
// 		}
// 	}
// 	return k
// }

func (m *Mutation) SaveSQL(s string) {
	m.sql = s
}

func (m *Mutation) GetSQL() string {
	return m.sql
}

func (m *Mutation) GetKeyComparOpr(key string) string {
	for _, v := range m.GetMembers() {
		if v.IsKey() && v.name == key {
			return v.eqy
		}
	}
	return ""
}

func (m *Mutation) GetKeys() []*Member {
	var k []*Member
	for _, v := range m.GetMembers() {
		if v.IsKey() {
			k = append(k, v)
		}
	}
	return k
}

func (m *Mutation) GetFilter() []*Member {
	var k []*Member
	for _, v := range m.GetMembers() {
		if v.IsFilter() {
			k = append(k, v)
		}
	}
	return k
}

func (m *Mutation) GetLiterals() []*Member {
	var k []*Member
	for _, v := range m.GetMembers() {
		if len(v.Literal()) > 0 {
			k = append(k, v)
		}
	}
	return k
}

func (m *Mutation) GetFilterAttrs() []*Member {
	var mm []*Member
	for _, v := range m.GetMembers() {
		if v.aty == Filter {
			mm = append(mm, v)
		}
	}
	return mm
}

func (m *Mutation) Return(rm RtnMode, v interface{}) *Mutation {

	if m == nil {
		return nil
	}
	m.rtn = &Rtn{mode: rm, val: v}
	return m
}

func (m *Mutation) RtnRequested() bool {
	if m.rtn != nil {
		return true
	}
	return false
}

func (m *Mutation) GetRtnMode() RtnMode {
	if m.rtn != nil {
		return (*m.rtn).mode
	}
	return RtnNone
}

func (m *Mutation) GetRtnValue() interface{} {
	if m.rtn != nil {
		return (*m.rtn).val
	}
	return nil
}

func (m *Mutation) AddTableMeta(meta *key.TabMeta) {
	m.tblDesc = meta
}

func (m *Mutation) GetTableKeys() []key.TableKey {
	if m.tblDesc != nil {
		return m.tblDesc.GetKeys()
	}
	return nil
}

func (m *Mutation) GetTableAttrs() key.AttributeT {
	return m.tblDesc.GetAttrs()
	//return m.keys
}

func (m *Mutation) SetPrepStmt(p interface{}) {
	m.prepStmt = p
}

func (m *Mutation) PrepStmt() interface{} {
	return m.prepStmt
}

func (m *Mutation) SetText(p string) {
	m.text = p
}

func (m *Mutation) Where(s string) *Mutation {
	if len(m.GetFilter()) != 0 {
		m.addErr(fmt.Errorf("Cannot mix Filter() and Where() methods"))
		return m
	}

	m.where = s
	return m
}

func (m *Mutation) GetWhere() string {
	return m.where
}

func (m *Mutation) Values(v ...interface{}) *Mutation {
	m.values = v
	return m
}

func (m *Mutation) GetValues() []interface{} {
	return m.values
}

func (m *Mutation) Text() string {
	return m.text
}

func (m *Mutation) Hint(h string) *Mutation {
	m.hint = h
	return m
}

func (m *Mutation) GetHint() string {
	return m.hint
}

// Key used to target items in  Select, Update, Delete. Equiv: Attribue(<name>, <value>, SET)
// optional arguments included Dyn struct tags and dynamodb query functions e.g. BeginsWith
// Keys are scalar values in all databases. So not arrays, so In, Any not appropriate
func (im *Mutation) Key(attr string, value interface{}, e ...Modifier) *Mutation {

	if im == nil {
		return im
	}

	// Parameterised names based on spanner's. Spanner uses a parameter name based on attribute name starting with "@". Params can be ignored in other dbs.
	// For other database, such as MySQL, will need to convert from Spanner's repesentation to relevant database during query formulation in the Execute() phase.
	p := strings.Replace(attr, "#", "_", -1)
	p = strings.Replace(p, ":", "x", -1)
	if p[0] == '0' {
		p = "1" + p
	}

	var found bool // validation error
	for _, v := range e {
		switch v {
		case EQ, NE, LT, LE, GT, GE, NOT:
		default:
			if v[0:2] == "__" {
				f := strings.ToUpper(v[2:])
				im.addErr(fmt.Errorf("validation error in mutation %s: cannot specify %s with Key", im.tag, f))
				return im
			}

		}
	}

	m := &Member{name: attr, Param: "@" + p, value: value, aty: Key}

	m.eqy = "EQ"

	if len(e) > 0 {
		fmt.Println(">>> e[0]", e[0])
		m.eqy = strings.ToUpper(e[0]) // func based comparision opeator BeginsWith
	}

	found = false // key
	//tableKeys are correctly ordered based on table def
	for i, kk := range im.GetTableKeys() {
		//
		if strings.ToUpper(kk.Name) == strings.ToUpper(attr) {
			found = true
			switch i {
			case 0:
				m.aty = Partition
			case 1:
				m.aty = Sortkey
			}
		}
	}
	if len(im.GetTableKeys()) > 0 && !found {
		im.addErr(fmt.Errorf("Key attribute %q is not a table key.", attr))
		im.ms = append(im.ms, m)
		return im
	}

	im.ms = append(im.ms, m)

	return im
}

func (m *Mutation) appendFilter(attr string, value interface{}, bcd BoolCd, e ...string) {

	eqy := "EQ"
	if len(e) > 0 {
		switch e[0] {
		case EQ, NE, LT, LE, GE, GT, NOT:
			eqy = strings.ToUpper(e[0][2:])
		default:
			eqy = e[0]
		}
	}
	// Parameterised names based on spanner's. Spanner uses a parameter name based on attribute name starting with "@". Params can be ignored in other dbs.
	// For other database, such as MySQL, will need to convert from Spanner's repesentation to relevant database during query formulation in the Execute() phase.
	p := strings.Replace(attr, "#", "_", -1)
	p = strings.Replace(p, ":", "x", -1)
	if p[0] == '0' {
		p = "1" + p
	}

	mm := &Member{name: attr, Param: "@" + p, value: value, eqy: eqy, aty: Filter, boolCd: bcd}

	m.ms = append(m.ms, mm)

}

func (m *Mutation) Filter(attr string, value interface{}, e ...string) *Mutation {
	if m == nil {
		return m
	}
	if len(m.GetWhere()) != 0 {
		m.addErr(fmt.Errorf("Cannot mix Filter() and Where() methods"))
		return m
	}
	var found bool // attr as key

	// check attr is not. a key
	//tableKeys are correctly ordered based on table def
	for _, kk := range m.GetTableKeys() {
		if strings.ToUpper(kk.Name) == strings.ToUpper(attr) {
			m.addErr(fmt.Errorf("Filter attribute %q is a table key. Use Key() instead", attr))
			return m
		}
	}

	found = false // found error
	for _, v := range e {
		switch v {
		case EQ, NE, LT, LE, GE, GT, NOT:
		default:
			if v[0:2] == "__" {
				m.addErr(fmt.Errorf("Validation error in %s. Argument %s in mutation not expected. Expected inequalities or db functions  ", m.tag, v[2:]))
				found = true
			}
		}
	}
	if found {
		return m
	}

	found = false
	for _, mm := range m.GetMembers() {
		if mm.IsFilter() {
			found = true
			break
		}
	}
	if found {
		m.AndFilter(attr, value, e...)
		return m
		//
	}

	m.appendFilter(attr, value, NIL, e...)

	return m
}

func (m *Mutation) appendBoolFilter(attr string, v interface{}, bcd BoolCd, e ...string) *Mutation {

	var found bool

	// check attr is not. a key
	//tableKeys are correctly ordered based on table def
	for _, kk := range m.GetTableKeys() {
		//
		if strings.ToUpper(kk.Name) == strings.ToUpper(attr) {
			found = true
		}
	}
	if found {
		m.addErr(fmt.Errorf("Filter attribute %q is a table key. Use Key() instead", attr))
		return m
	}

	found = false
	for _, mm := range m.GetMembers() {
		if mm.IsFilter() && mm.boolCd == NIL {
			found = true
			break
		}
	}

	if !found {
		m.addErr(fmt.Errorf(`Mutation no "Filter" condition specified`))
		return m
		//
	}

	m.appendFilter(attr, v, bcd, e...)

	return m
}

func (m *Mutation) AndFilter(a string, v interface{}, e ...string) *Mutation {
	if m == nil {
		return m
	}
	m.af++
	return m.appendBoolFilter(a, v, AND, e...)

}

func (m *Mutation) OrFilter(a string, v interface{}, e ...string) *Mutation {
	if m == nil {
		return m
	}
	m.of++
	return m.appendBoolFilter(a, v, OR, e...)
}

func (m *Mutation) GetOr() int {
	return m.of
}
func (m *Mutation) GetAnd() int {
	return m.af
}

func (m *Mutation) FilterSpecified() bool {
	for _, v := range m.ms {
		if v.aty == Filter {
			return true
		}
	}
	return false
}

// end - func (q *QueryHandle)

// func (m *Mutation) SetError(e error) {
// 	m.err = e
// }

func (m *Mutation) GetError() []error {
	return m.err
}

func (m *Mutation) SQL() string {
	return m.text
}

func (m *Mutation) SetParams(p []interface{}) {
	m.params = p
}

func (m *Mutation) Params() []interface{} {
	return m.params
}

func (m *Mutation) GetOpr() StdMut {
	return m.opr
}

func (m *Mutation) GetPK() string {
	for _, mm := range m.GetMembers() {
		if mm.aty == Partition {
			return mm.name
		}
	}
	return ""
}

func (m *Mutation) GetSK() string {
	for _, mm := range m.GetMembers() {
		if mm.aty == Sortkey {
			return mm.name
		}
	}
	return ""
}

func (m *Mutation) GetKeyValue(nm string) interface{} {
	for _, mm := range m.GetMembers() {
		if mm.IsKey() && mm.name == nm {
			return mm.Value
		}
	}
	return nil
}

// func (m *Mutation) GetSK() string {
// 	return m.sk
// }

func (m *Mutation) GetTable() string {
	return string(m.tbl)
}

func (m *Mutation) getMemberIndex(attr string) int {
	for i, v := range m.ms {
		if v.name == attr {
			return i
		}
	}
	panic(fmt.Errorf("getMember: member %q not found in mutation members", attr))
	return -1
}

func (m *Mutation) GetMemberValue(attr string) interface{} {
	for _, v := range m.ms {
		if v.name == attr {
			return v.Value()
		}
	}
	return nil
}

func (m *Mutation) SetMemberValue(attr string, v interface{}) {
	i := m.getMemberIndex(attr)
	e := reflect.TypeOf(m.ms[i].value)
	g := reflect.TypeOf(v)
	if e.Kind() != g.Kind() {
		panic(fmt.Errorf("SetMemberValue for %s expected a type of %q got %a", attr, e, g))
	}
	m.ms[i].value = g
}

func (m *Mutation) addErr(e error) {
	logAlert(e.Error())
	m.err = append(m.err, e)
}

// Config set for individual mutations e.g. Override scan database config, used to detect full scan operations and abort
func (im *Mutation) Config(opt ...Option) *Mutation {
	im.config = append(im.config, opt...)
	return im
}

func (im *Mutation) GetConfig(s string) interface{} {
	for _, k := range im.config {
		if k.Name == s {
			return k.Val
		}
	}
	return nil
}

// func (im *Mutation) Filter(attr string, value interface{}) *Mutation {
// 	return im.AddAttribute(attr, value, IsFilter)
// }

func (im *Mutation) Increment(attr string) *Mutation {
	if im == nil {
		return im
	}
	return im.AddAttribute(attr, 1, ADD)
}

func (im *Mutation) Decrement(attr string) *Mutation {
	if im == nil {
		return im
	}
	return im.AddAttribute(attr, 1, SUBTRACT)
}

func (im *Mutation) Add(attr string, value interface{}) *Mutation {
	if im == nil {
		return im
	}
	return im.AddAttribute(attr, value, ADD)
}

func (im *Mutation) Subtract(attr string, value interface{}) *Mutation {
	if im == nil {
		return im
	}
	return im.AddAttribute(attr, value, SUBTRACT)
}
func (im *Mutation) Multiply(attr string, value interface{}) *Mutation {
	if im == nil {
		return im
	}
	return im.AddAttribute(attr, value, MULTIPLY)
}

func (im *Mutation) Set(attr string, value interface{}, mod ...Modifier) *Mutation {
	if im == nil {
		return im
	}

	var found bool

	for _, v := range mod {
		if v[:2] == "__" {
			im.addErr(fmt.Errorf("Cannot combine %s with SET", v[2:]))
			found = true
		}
	}
	if found {
		return im
	}

	m := im.newAttr(attr, value, SET)
	m.mod = mod

	return im
}

// Append suitable for Dynamodb LIST type only
func (im *Mutation) Append(attr string, value interface{}) *Mutation {
	if im == nil {
		return im
	}

	if !IsArray(value) {
		im.addErr(fmt.Errorf("Validation error in %s, expected an array/slice type for Append", im.tag))
		return im
	}

	m := im.newAttr(attr, value, APPEND)
	m.array = true

	return im
}

// Append suitable for Dynamodb LIST type only
func (im *Mutation) Prepend(attr string, value interface{}) *Mutation {
	if im == nil {
		return im
	}

	if !IsArray(value) {
		im.addErr(fmt.Errorf("Validation error in %s, expected an array/slice type for Append", im.tag))
		return im
	}

	m := im.newAttr(attr, value, PREPEND)
	m.array = true

	return im
}

// Remove suitable for Dynamodb only
func (im *Mutation) Remove(attr string) *Mutation {
	if im == nil {
		return im
	}

	im.newAttr(attr, nil, REMOVE)

	return im
}

func (im *Mutation) newAttr(attr string, value interface{}, oper Operation) *Member {
	// Parameterised names based on spanner's. Spanner uses a parameter name based on attribute name starting with "@". Params can be ignored in other dbs.
	// For other database, such as MySQL, will need to convert from Spanner's repesentation to relevant database during query formulation in the Execute() phase.
	p := strings.Replace(attr, "#", "_", -1)
	p = strings.Replace(p, ":", "x", -1)
	if p[0] == '0' {
		p = "1" + p
	}
	m := &Member{name: attr, Param: "@" + p, value: value, oper: oper}

	im.ms = append(im.ms, m)

	return m
}

// AddAttribute(<attribute>,<vauue>, <modifier>)
// for Insert/Put Modifier can be
//
//	for array: marshal to: DynSet, DynList
//
// for Update Modifier can be
//
//	for array: Set, Append, PrePend
//	for non-array: Set, Add, Subtract, Increment, Descrement
//
// for Delete Modifier can be:

func (im *Mutation) Attribute(attr string, value interface{}, mod ...Modifier) *Mutation {
	return im.AddMember(attr, value, mod...)
}

func (im *Mutation) AddAttribute(attr string, value interface{}, mod ...Modifier) *Mutation {
	return im.AddMember(attr, value, mod...)
}

func (im *Mutation) AddMember(attr string, value interface{}, mod ...Modifier) *Mutation {

	if im == nil {
		return im
	}
	// separate out the target methods
	mx := ModifierS(mod)
	if mx.Exists(IsKey) {
		mx.Delete(IsKey)
		fmt.Println("AddMember ISKEY.....")
		return im.Key(attr, value, mx...)
	}
	if mx.Exists(KEY) {
		mx.Delete(KEY)
		fmt.Println("AddMember ISKEY2.....")
		return im.Key(attr, value, mx...)
	}
	if mx.Exists(IsFilter) {
		mx.Delete(IsFilter)
		return im.Filter(attr, value, mx...)
	}
	if mx.Exists(FILTER) {
		mx.Delete(FILTER)
		return im.Filter(attr, value, mx...)
	}

	// separate out the mutation operations
	if mx.Exists(APPEND) {
		// TODO: check SET not in mod
		return im.Append(attr, value)
	}
	if mx.Exists(PREPEND) {
		return im.Prepend(attr, value)
	}
	if mx.Exists(REMOVE) {
		return im.Remove(attr)
	}
	if mx.Exists(SET) {
		mx.Delete(SET)
		return im.Set(attr, value, mx...)
	}

	// left with mods of Inequalities EQ,NE,LT etc

	// Parameterised names based on spanner's. Spanner uses a parameter name based on attribute name starting with "@". Params can be ignored in other dbs.
	// For other database, such as MySQL, will need to convert from Spanner's repesentation to relevant database during query formulation in the Execute() phase.
	p := strings.Replace(attr, "#", "_", -1)
	p = strings.Replace(p, ":", "x", -1)
	if p[0] == '0' {
		p = "1" + p
	}

	m := &Member{name: attr, Param: "@" + p, value: value, oper: SET, mod: mod}

	// if !mx.Exists(SET) {
	// 	m.mod.Assign(SET)
	// }

	//tableKeys are correctly ordered based on table def
	// check attr is key
	var isKey_, eqySet, operSet bool

	if im.opr != Insert {
		for i, kk := range im.GetTableKeys() {
			//
			if strings.ToUpper(kk.Name) == strings.ToUpper(attr) {
				isKey_ = true
				if i == 0 {
					m.aty = Partition
				}
				if i == 1 {
					m.aty = Sortkey
				}
			}
		}
		// validate Modifier value.
		// Must specifiy IsFilter to be used in where clause or filter expression, otherwise will be used to Set in a Update mutation.
		switch len(mod) {
		case 0:
			//	below is not an error. KEY is used as target identifier and is passed into where clause, not set.
			if isKey_ {
				im.addErr(fmt.Errorf("Validation error in %s. %q is a key in table and cannot be updated", im.tag, attr))
			}

		default:
			// check only if table keys supplied
			if len(im.GetTableKeys()) > 0 {
				if isKey_ {
					im.addErr(fmt.Errorf("Validation error in %s. %q is a key in table and cannot be updated", im.tag, attr))
					return nil
				}
			}
		}

	}

	if IsArray(value) {

		if operSet {
			im.addErr(fmt.Errorf("Validation error in %s. %s is not an appropriate value for array type %s", im.tag, m.oper, attr))
		}
		if eqySet {
			im.addErr(fmt.Errorf("Validation error in %s. %s is not an appropriate value for array type %s", im.tag, m.eqy, attr))
		}
		// check if BS, NS, SS modifier is used. Default is List
		m.array = true

		// set "oper".

		// For Dynamodb:

		if m.mod.Exists(NumberSet) || m.mod.Exists(StringSet) || m.mod.Exists(BinarySet) {
			if m.mod.Exists(SUBTRACT) || m.mod.Exists(DELETE) {
				m.oper = SUBTRACT
			} else if m.mod.Exists(SET) {
				m.oper = SET
			} else {
				m.oper = ADD
			}
		} else {
			// List
			if m.mod.Exists(PREPEND) {
				m.oper = PREPEND
			} else if m.mod.Exists(SET) {
				m.oper = SET
			} else {
				m.oper = APPEND
			}
		}

		im.ms = append(im.ms, m)

		return im

	}

	// set equality (eqy) EQ..NE,<any db function e.g. BEGINSWITH, for mutation - only appropriate for IsKey, IsFilter modifiers
	// AddAttribute(SortK,<>,mut.IsKey,"BeginsWith")
	for i, v := range mod {

		switch v {

		// inequalities, used in Key, Filter only.
		case EQ, NE, GT, GE, LT, LE, NOT:
			if len(mod) == 1 {
				return im.Filter(attr, value, mod...)
			} else {
				im.addErr(fmt.Errorf("Validation error in %s. Expected one Inequality got %d", len(mod)))
			}
			// m.eqy = mod[i][2:]
			// m.aty = Filter
			//	im.addErr(fmt.Errorf("Validation error in %s. %s defined on attribute %s for Key or Filter only", im.tag, m.eqy, attr))

		// math operations
		case ADD, SUBTRACT, MULTIPLY, SETNULL:
			m.oper = mod[i]
			//	m.mod.Delete(SET)
			operSet = true
		}

		// database function
		if v[0] != '_' {
			// db function e.g. BeginsWith
			m.eqy = mod[i]
			eqySet = true
		}

	}

	// set eqy to EQ, default for IsKey, IsFilter (ignored for all others)
	if len(m.eqy) == 0 {
		m.eqy = "EQ"
	}

	//validate mod
	// for _, v := range mod {
	// 	m := ModifierS(mod)
	// 	m.Validate(im, v)
	// }
	// if len(im.GetError()) > 0 {
	// 	return im
	// }
	// for _, v := range m.mod {
	// 	fmt.Println("3  modifier ", v)
	// }

	im.ms = append(im.ms, m)

	return im
}

func (im *Mutation) Submit(in interface{}) *Mutation {

	if im == nil {
		return nil
	}
	if len(im.ms) > 0 {
		panic(fmt.Errorf("Cannot use Submit() with other mutation methods"))
	}

	im.submit = in

	return im
}

func (im *Mutation) GetSubmit() interface{} {
	return im.submit
}

func (im *Mutation) MarshalAttributes(in interface{}, tag_ ...string) ([]*Member, error) { // []*Attribute

	//keys := im.GetTableKeys()
	var ty reflect.Type
	var vi reflect.Value

	//in := im.GetSubmit()
	if in == nil {
		return nil, fmt.Errorf("nil input value for MarshalAttribute2 ")
	}
	for {
		vi = reflect.ValueOf(in)
		ty = vi.Type()
		if ty.Kind() == reflect.Pointer {
			vi = reflect.ValueOf(in).Elem()
			ty = vi.Type()
		}

		if ty.Kind() == reflect.Slice {
			vi = reflect.ValueOf(in).Elem()
			ty = vi.Type()
		}
		if ty.Kind() == reflect.Struct {
			break
		}
	}
	msOld := im.GetMembers()
	im.SetMembers(nil)

	switch im.GetOpr() {
	case Insert:

		// table attribute names
		// for f := 0; f < ty.NumField(); f++ {
		// 	var fnm string
		// 	// ignore tag
		// 	if ty.Field(f).Tag.Get("mdb") == "-" {
		// 		continue
		// 	}
		// 	tag := strings.Split(ty.Field(f).Tag.Get("mdb"), ",")
		// 	if len(tag[0]) > 0 {
		// 		fnm = tag[0]
		// 	} else {
		// 		fnm = ty.Field(f).Name
		// 	}
		// 	fmt.Println("field: ", fnm)

		// 	// check field is a table attribute
		// 	if _, ok := attrs[fnm]; !ok {
		// 		continue
		// 	}
		// 	// match
		// 	m := im.Attribute(fnm, vi.Field(f).Interface(), SET)
		// 	if m == nil {
		// 		return im.GetError()[0]
		// 	}
		// }
		// table attribute names
		fmt.Println("Arg; ", tag_)
		for f := 0; f < ty.NumField(); f++ {

			var (
				fnm    string
				omit   bool
				atrtag []Modifier
			)
			// ignore tag
			if ty.Field(f).Tag.Get("mdb") == "-" {
				continue
			}
			//
			// non-mdb
			//
			if len(tag_) > 0 {
				// support tag dynamodbav
				switch tag_[0] {
				case "dynamodbav":

					if ty.Field(f).Tag.Get(tag_[0]) == "-" {
						continue
					}

					tags := strings.Split(ty.Field(f).Tag.Get(tag_[0]), ",")

					for i, t := range tags {

						switch i {
						case 0:
							// attribute name
							if len(t) > 0 {
								fnm = t
								fmt.Println("***attribute renamed to : ", fnm)
							} else {
								fnm = ty.Field(f).Name
								fmt.Println("***attribute  : ", fnm)
							}

						default:

							fmt.Println("insert dynamodb: ", t)
							switch strings.ToLower(t) {
							case "binaryset":
								atrtag = append(atrtag, "_BINARYSET")
							case "numberset":
								atrtag = append(atrtag, "_NUMBERSET")
							case "stringset":
								atrtag = append(atrtag, "_STRINGSET")
							case "string":
								switch vi.Field(f).Type().Kind() {
								case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
									atrtag = append(atrtag, "_string")
								case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
									atrtag = append(atrtag, "_string")
								case reflect.Float32, reflect.Float64:
									atrtag = append(atrtag, "_string")
								default:
									fmt.Println("tag string not appropriate for type")
								}
							case "omitempty":
								if vi.Field(f).IsZero() {
									omit = true
								}
							case "omitemptyelem":
								switch vi.Field(f).Type().Kind() {
								case reflect.Slice, reflect.Map:
									if vi.Field(f).IsZero() {
										omit = true
									}
								default:
									return nil, fmt.Errorf("error in tag for field %s. nullemptyelem applies to slice or map only. ", ty.Field(f).Name)
								}
							case "nullempty":
								if vi.Field(f).IsZero() {
									atrtag = append(atrtag, SETNULL)
								}
							case "nullemptyelem":
								switch vi.Field(f).Type().Kind() {
								case reflect.Slice, reflect.Map:
									if vi.Field(f).IsZero() {
										atrtag = append(atrtag, SETNULL)
									}
								default:
									return nil, fmt.Errorf("error in tag for field %s. nullemptyelem applies to slice or map only. ", ty.Field(f).Name)
								}
							}
						}
					}
				}
			}
			//
			// mdb
			//
			tag := strings.Split(ty.Field(f).Tag.Get("mdb"), ",")

			for i, t := range tag {

				switch i {
				case 0:
					if len(t) > 0 {
						fnm = t
					} else {
						if len(fnm) == 0 {
							fnm = ty.Field(f).Name
						}
					}

				default:

					fmt.Println("insert mdb: ", t)
					switch "__" + strings.ToUpper(t) {
					case KEY:
						atrtag = append(atrtag, KEY)
					}

					fmt.Println("tag: ", t)

				}
			}

			if omit {
				continue
			}
			// match
			fmt.Printf("++++ Attribute(%s , %#v\n", fnm, atrtag)
			m := im.Attribute(fnm, vi.Field(f).Interface(), atrtag...)
			if m == nil {
				return nil, im.GetError()[0]
			}
		}

	case Update:

		attrs := im.GetTableAttrs()
		fmt.Println(" : len(attrs) ", len(attrs))

		// table attribute names
		for f := 0; f < ty.NumField(); f++ {

			var (
				fnm    string
				omit   bool
				atrtag []Modifier
			)
			// ignore tag
			if ty.Field(f).Tag.Get("mdb") == "-" {
				continue
			}
			//
			// non-mdb
			//
			if len(tag_) > 0 {
				// support tag dynamodbav
				switch tag_[0] {
				case "dynamodbav":

					if ty.Field(f).Tag.Get(tag_[0]) == "-" {
						continue
					}
					// capture attribute alias

					tags := strings.Split(ty.Field(f).Tag.Get(tag_[0]), ",")
					for i, t := range tags {

						switch i {
						case 0:
							if len(t) > 0 {
								fnm = t
								fmt.Println("***attribute renamed to : ", fnm)
							} else {
								fnm = ty.Field(f).Name
								fmt.Println("***attribute  : ", fnm)
							}

						default:
							switch strings.ToLower(t) {
							case "binaryset":
								atrtag = append(atrtag, "_BINARYSET")
							case "numberset":
								atrtag = append(atrtag, "_NUMBERSET")
							case "stringset":
								atrtag = append(atrtag, "_STRINGSET")
							case "string":
								switch vi.Field(f).Type().Kind() {
								case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
									atrtag = append(atrtag, "_string")
								case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
									atrtag = append(atrtag, "_string")
								case reflect.Float32, reflect.Float64:
									atrtag = append(atrtag, "_string")
								default:
									fmt.Println("tag string not appropriate for type")
								}
							case "omitempty":
								if vi.Field(f).IsZero() {
									omit = true
								}
							case "omitemptyelem":
								switch vi.Field(f).Type().Kind() {
								case reflect.Slice, reflect.Map:
									if vi.Field(f).IsZero() {
										omit = true
									}
								default:
									return nil, fmt.Errorf("error in tag for field %s. nullemptyelem applies to slice or map only. ", ty.Field(f).Name)
								}
							case "nullempty":
								if vi.Field(f).IsZero() {
									atrtag = append(atrtag, SETNULL)
								}
							case "nullemptyelem":
								switch vi.Field(f).Type().Kind() {
								case reflect.Slice, reflect.Map:
									if vi.Field(f).IsZero() {
										atrtag = append(atrtag, SETNULL)
									}
								default:
									return nil, fmt.Errorf("error in tag for field %s. nullemptyelem applies to slice or map only. ", ty.Field(f).Name)
								}
							}
						}
					}
				}
			}
			//
			// mdb
			//
			tag := strings.Split(ty.Field(f).Tag.Get("mdb"), ",")
			for i, t := range tag {

				switch i {
				case 0:
					if len(t) > 0 {
						fnm = t
					} else {
						if len(fnm) == 0 {
							fnm = ty.Field(f).Name
						}
					}
					fmt.Println("field: ", fnm)

				default:

					fmt.Println("update mdb: ", t)
					switch "__" + strings.ToUpper(t) {
					case KEY:
						atrtag = append(atrtag, KEY)

					case "__INCREMENT":
						atrtag = append(atrtag, ADD)
						// assign numeral 1 to field value
						switch vi.Field(f).Kind() {
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							if vi.Field(f).CanSet() {
								vi.Field(f).SetInt(1)
							} else {
								return nil, fmt.Errorf(`reflect.Value.SetInt using unaddressable value. Pass in address of struct to Submit()`)
							}
						case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
							if vi.Field(f).CanSet() {
								vi.Field(f).SetInt(1)
							} else {
								return nil, fmt.Errorf(`reflect.Value.SetInt using unaddressable value. Pass in address of struct to Submit()`)
							}
						case reflect.Float32, reflect.Float64:
							if vi.Field(f).CanSet() {
								vi.Field(f).SetFloat(1.0)
							} else {
								return nil, fmt.Errorf(`reflect.Value.SetFloat using unaddressable value. Pass in address of struct to Submit()`)
							}
						}

					case "__DECREMENT":
						atrtag = append(atrtag, SUBTRACT)
						// assign numeral 1 to field value
						switch vi.Field(f).Kind() {
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							if vi.Field(f).CanSet() {
								vi.Field(f).SetInt(1)
							} else {
								return nil, fmt.Errorf(`reflect.Value.SetInt using unaddressable value. Pass in address of struct to Submit()`)
							}
						case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
							if vi.Field(f).CanSet() {
								vi.Field(f).SetInt(1)
							} else {
								return nil, fmt.Errorf(`reflect.Value.SetInt using unaddressable value. Pass in address of struct to Submit()`)
							}
						case reflect.Float32, reflect.Float64:
							if vi.Field(f).CanSet() {
								vi.Field(f).SetFloat(1.0)
							} else {
								return nil, fmt.Errorf(`reflect.Value.SetFloat using unaddressable value. Pass in address of struct to Submit()`)
							}
						}

					case "__MULTIPLY":
						atrtag = append(atrtag, MULTIPLY)
						switch vi.Field(f).Kind() {
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
						case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
						case reflect.Float32, reflect.Float64:
						default:
							return nil, fmt.Errorf(`Multiply applies to number type only`)

						}
					}
				}

				fmt.Println("tag: ", t)

			}

			// check field is a table attribute - makes sense for SQL where schema is fixed, but not for Dynamodb. Only check for Keys.
			// fmt.Println("check in attrs for ", fnm)
			if len(attrs) > 0 {
				if _, ok := attrs[fnm]; !ok {
					fmt.Println("Continue....")
					continue
				}
			}

			if omit {
				continue
			}
			// match
			fmt.Printf("**** Attribute(%s , %#v\n", fnm, atrtag)
			m := im.Attribute(fnm, vi.Field(f).Interface(), atrtag...)
			if m == nil {
				return nil, im.GetError()[0]
			}
		}
	case Delete:
	}
	ms_ := im.GetMembers()
	im.SetMembers(msOld)

	return ms_, nil
}

// func NewMutation(tab tbl.Name, opr StdMut, keys []key.Key) *Mutation {
func NewMutation(tab tbl.Name, opr StdMut) *Mutation {
	mut := &Mutation{tbl: tab, opr: opr}

	// for _, v := range keys { // moved to SetKeys()
	// 	mut.Key(v.Name, v.Value)
	// }
	return mut
}

// AddKeys assigns keys passed into  tx.MergeMutation
func (m *Mutation) AddKeys(keys []key.Key) {
	for _, v := range keys {
		m.Key(v.Name, v.Value)
	}
}

// FindMutation searches the associated batch of mutations based on key values.
func (bm *Mutations) FindMutation(table tbl.Name, keys []key.MergeKey) (*Mutation, error) {
	var (
		ok    bool
		sm    *Mutation
		match int
	)
	// TODO: what about active batch???
	for _, sm_ := range *bm {

		if sm, ok = sm_.(*Mutation); !ok {
			continue
		}
		if sm.opr == Merge {
			panic(fmt.Errorf("Merge mutation cannot be used with a MergeMuatation method"))
		}
		if sm.tbl != table || !(sm.opr == Insert || sm.opr == Update) {
			continue
		}
		match = 0

		// merge keys have been validated and are in table key order (partition, sortk)
		for _, k := range keys {

			// cycle thru members of source mutations -
			for _, attr := range sm.ms {

				// evaluate Partition Key and Sortk types (for DYnamodb these are scalar types, number, string, [])
				if k.Name != attr.name {
					continue
				}

				switch x := k.Value.(type) {

				case int64:
					if k.DBtype != "N" {
						switch k.DBtype {
						case "B":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a binary type, supplied a number type", attr.name)
						case "S":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a string type,  supplied a number type", attr.name)
						}
					}
					if av, ok := attr.value.(int64); !ok {
						return nil, fmt.Errorf("in find mutation attribute %q. Expected an int64 type but supplied a %T type", attr.name, attr.value)
					} else if x == av {
						match++
					}

				case int32:
					if k.DBtype != "N" {
						switch k.DBtype {
						case "B":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a binary type, supplied a number type", attr.name)
						case "S":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a string type,  supplied a number type", attr.name)
						}
					}
					if av, ok := attr.value.(int32); !ok {
						return nil, fmt.Errorf("in find mutation attribute %q. Expected an int type but supplied a %T type", attr.name, attr.value)
					} else if x == av {
						match++
					}

				case int:
					if k.DBtype != "N" {
						switch k.DBtype {
						case "B":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a binary type, supplied a number type", attr.name)
						case "S":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a string type,  supplied a number type", attr.name)
						}
					}
					if av, ok := attr.value.(int); !ok {
						return nil, fmt.Errorf("in find mutation attribute %q. Expected an int type but supplied a %T type", attr.name, attr.value)
					} else if x == av {
						match++
					}

				case float64:
					if k.DBtype != "N" {
						switch k.DBtype {
						case "B":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a binary type, supplied a number type", attr.name)
						case "S":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a string type,  supplied a number type", attr.name)
						}
					}
					if av, ok := attr.value.(float64); !ok {
						return nil, fmt.Errorf("in find mutation attribute %q. Expected a float64 type but supplied a %T type", attr.name, attr.value)
					} else if x == av {
						match++
					}

				case string:
					if k.DBtype != "S" {
						switch k.DBtype {
						case "B":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a binary type, supplied a string type", attr.name)
						case "N":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a number type, supplied a string type", attr.name)
						}
					}
					if av, ok := attr.value.(string); !ok {
						return nil, fmt.Errorf("in find mutation attribute %q. Expected a string type but suppled a %T type", attr.name, attr.value)
					} else if x == av {
						match++
					}

				case []byte:
					if k.DBtype != "B" {
						switch k.DBtype {
						case "S":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a string type, supplied a binary type", attr.name)
						case "N":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a number type, supplied a binary type", attr.name)
						}
					}
					if av, ok := attr.value.([]byte); !ok {
						return nil, fmt.Errorf("in find mutation attribute %q. Expected a binary ([]byte type but is a %T type", attr.name, attr.value)
					} else if bytes.Equal(x, av) {
						match++
					}

				case uuid.UID:
					if k.DBtype != "B" {
						switch k.DBtype {
						case "S":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a string type, supplied a binary type", attr.name)
						case "N":
							return nil, fmt.Errorf("value is of wrong datatype for %q. Expected a number type, supplied a binary type", attr.name)
						}
					}
					if av, ok := attr.value.(uuid.UID); !ok {
						if av, ok := attr.value.([]uint8); !ok {
							return nil, fmt.Errorf("in find mutation attribute %q. Expected a binary ([]uint8) type but is a %T type", attr.name, attr.value)
						} else if bytes.Equal([]byte(x), []byte(av)) {
							match++
						}
					} else if bytes.Equal([]byte(x), []byte(av)) {
						match++
					}
				}
				break
			}

			if match == len(keys) {
				return sm, nil
			}
		}
	}

	if match == len(keys) {
		return sm, nil
	}
	return nil, nil
}
