package query

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	elog "github.com/GoGraph/errlog"
	slog "github.com/GoGraph/syslog"
	"github.com/GoGraph/tx/tbl"
	"github.com/GoGraph/uuid"
)

type ScanOrder int8
type Orderby int8
type NullOrder int8
type AccessTy byte
type BoolCd byte
type Mode byte
type Cfunc func(*sync.WaitGroup, interface{}) // channel consumer func arg to ExecuteByFunc()

const (
	NA string = "NA"
	//
	ASC     ScanOrder = 0 // default
	Forward ScanOrder = 0
	DESC    ScanOrder = 1
	Reverse ScanOrder = 1
	//
	Asc        Orderby   = 0 // default
	Desc       Orderby   = 1
	NullsFirst NullOrder = 0
	NullsLast  NullOrder = 1
	//
	GetItem AccessTy = iota
	Query
	Scan
	Transact
	Batch
	Null
	//
	NIL BoolCd = iota
	AND
	OR
	//
	CHANNEL Mode = iota
	FUNC
	//
)

type Attrty byte

type Label string

//type OpTag Label

const (
	IsKey Attrty = iota + 1
	IsFilter
	IsFetch // projection fields
)

func syslog(s string) {
	slog.Log("query", s)
}
func syslogAlert(s string) {
	slog.LogAlert("query", s)
}

type Attr struct {
	name    string
	param   string
	value   interface{}
	literal string // literal struct tag value - . alternative to value - replace attribute name with literal in query stmt.
	aty     Attrty // attribute type, e.g. Key, Filter, Fetch
	eqy     string // Scalar Opr: EQ, LE,...  Slice Opr: IN, ANY
	boolCd  BoolCd // And, Or - appropriate for Filter only.
}

func (a *Attr) GetOprStr() string {
	return strings.ToUpper(a.eqy)
}

func (a *Attr) AttrType() Attrty {
	return a.aty
}

func (a *Attr) Name() string {
	return a.name
}

func (a *Attr) Literal() string {
	return a.literal
}

func (a *Attr) BoolCd() BoolCd {
	return a.boolCd
}

func (a *Attr) Value() interface{} {
	return a.value
}

func (a *Attr) IsKey() bool {
	return a.aty == IsKey
}

func (a *Attr) IsFetch() bool {
	return a.aty == IsFetch
}

func (a *Attr) Filter() bool {
	return a.aty == IsFilter
}

type orderby struct {
	attr string
	sort Orderby
}
type Option struct {
	Name string
	Val  interface{}
}

type QueryHandle struct {
	Tag    string
	config []Option
	//
	err error
	//ctx context.Context
	//stateId util.UID - id for maintaining state
	attr     []*Attr // all attributes sourced from  Key , Select, filter , addrVal clauses as determined by attr atyifier (aty)
	tbl      tbl.Name
	idx      tbl.Name
	limit    int
	workers  int  //  (old parallel) number of channels to create for parallel scan operation
	parallel int  // parallel query - SQL only
	css      bool // read consistent mode
	//
	queryMode Mode
	//
	channel interface{} // returned from ExecByChannel() - slice of channels from 1 to #workers
	f       Cfunc       // channel consumer func : channel type []<fetch type>
	// prepare mutation in db's that support separate preparation phase
	prepare  bool
	prepStmt interface{}
	//
	//first bool ??
	//
	eodlc int           // eod loop counter - used to determine first EOD execution which drives switch logic
	abuf  int           // active buffer - index into bufs. See EOD()
	bufs  []interface{} // buffers : variadic arg of bind variables passed in Select()  : type []*[]unprocBuf
	//
	scan bool // NewScan specified
	//
	// pk     string
	// sk     string
	//	orderby  string
	so      ScanOrder
	orderBy orderby
	//
	accessTy AccessTy // TODO: remove
	// select() handlers
	fetch   interface{}   //  Select()
	select_ bool          // indicates Select() has been executed. Catches cases when Select() specified more than once.
	binds   []interface{} // populated by Split() with address of all struct fields in fetch type, which can included embedded struct. Used in SQL.Scan()
	// is query restarted. Paginated queries only.
	restart bool
	// pagination state
	pgStateId   uuid.UID
	pgStateValI interface{}
	pgStateValS string
	// other runtime state data
	eod bool
	// varM map[string]interface{}
	// varS []interface{}
	worker int
	// AndFilter, OrFilter counts
	af, of int
	// Where, Values method
	where  string
	values []interface{}
}

func New(tbl tbl.Name, label string, idx ...tbl.Name) *QueryHandle {
	if len(idx) > 0 {
		return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, idx: idx[0], css: true, so: ASC}
	}
	return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, css: true, so: ASC}
}

func New2(label string, tbl tbl.Name, idx ...tbl.Name) *QueryHandle {
	if len(idx) > 0 {
		return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, idx: idx[0], css: true, so: ASC}
	}
	return &QueryHandle{Tag: label, tbl: tbl, accessTy: Null, css: true, so: ASC}
}

// func NewContext(ctx context.Context, tbl tbl.Name, label string, idx ...tbl.Name) *QueryHandle {
// 	if len(idx) > 0 {
// 		return &QueryHandle{Tag: label, ctx: ctx, tbl: tbl, accessTy: Null, idx: idx[0], css: true}
// 	}
// 	return &QueryHandle{Tag: label, ctx: ctx, tbl: tbl, accessTy: Null, css: true}
// }

// func (q *QueryHandle) SetDB(d driver.Handle) {
// 	q.dbh = d
// }

func (q *QueryHandle) Clone() *QueryHandle {
	d := QueryHandle{}
	d.config = q.config

	d.Tag = q.Tag
	//stateId uuid.UID - id for maintaining state
	d.attr = q.attr // all attributes sourced from  Key , Select, filter , addrVal clauses as determined by attr atyifier (aty)
	d.tbl = q.tbl
	d.idx = q.idx
	d.limit = q.limit
	d.workers = q.workers
	d.parallel = q.parallel
	d.css = q.css
	//
	//d.first = q.first
	//
	d.scan = q.scan
	//
	// d.pk = q.pk
	// d.sk = q.sk
	//	orderby  string
	d.so = q.so
	d.accessTy = q.accessTy
	d.orderBy = q.orderBy
	// select() handlers
	// is query restarted. Paginated queries only.
	d.restart = q.restart
	// pagination state
	d.pgStateId = q.pgStateId
	d.pgStateValI = q.pgStateValI
	d.pgStateValS = q.pgStateValS
	d.eod = q.eod

	// AndFilter, OrFilter counts
	d.af, d.of = q.af, q.of
	// Where, Values method
	d.where = q.where
	d.values = q.values

	return &d

}

func (q *QueryHandle) GetTag() string {
	return q.Tag
}

func (q *QueryHandle) Config(opt ...Option) *QueryHandle {
	q.config = append(q.config, opt...)
	return q
}

func (q *QueryHandle) GetConfig(s string) interface{} {
	for _, k := range q.config {
		if k.Name == s {
			return k.Val
		}
	}
	return nil
}

func (q *QueryHandle) qh() {}

// Reset nullifies certain data after a prepared stmt execution, for later reprocessing
func (q *QueryHandle) Reset() {
	q.attr = nil
	//	q.pk, q.sk = "", ""
}

func (q *QueryHandle) Channel() Mode {
	return CHANNEL
}

func (q *QueryHandle) Func() Mode {
	return FUNC
}

func (q *QueryHandle) SetFunc(f Cfunc) {
	q.queryMode = FUNC
	q.f = f
}

func (q *QueryHandle) GetFunc() Cfunc {
	return q.f
}

func (q *QueryHandle) GetLiterals() []*Attr {
	var literals []*Attr
	for _, a := range q.attr {
		if len(a.literal) > 0 {
			literals = append(literals, a)
		}
	}
	return literals
}

func (q *QueryHandle) Error() error {
	return q.err
}

func (q *QueryHandle) SetError(e error) {
	q.err = e
}

func (q *QueryHandle) SetWorkerId(i int) {
	q.worker = i
}

func (q *QueryHandle) Worker() int {
	return q.worker
}

func (q *QueryHandle) SetPrepare() {
	q.prepare = true
}

func (q *QueryHandle) SetExecMode(m Mode) {
	q.queryMode = m
}

func (q *QueryHandle) ExecMode() Mode {
	return q.queryMode
}

func (q *QueryHandle) Prepare() bool {
	return q.prepare
}

func (q *QueryHandle) PrepStmt() interface{} {
	return q.prepStmt
}

func (q *QueryHandle) SetPrepStmt(p interface{}) {
	q.prepStmt = p
}

func (q *QueryHandle) Where(s string) {
	q.where = s
}

func (q *QueryHandle) GetWhere() string {
	return q.where
}

func (q *QueryHandle) Values(v []interface{}) {
	q.values = v
}

func (q *QueryHandle) GetValues() []interface{} {
	return q.values
}

// func (q *QueryHandle) Ctx() context.Context {
// 	return q.ctx
// }

// func (q *QueryHandle) Channel() interface{} {
// 	return q.channel
// }

func (q *QueryHandle) SetChannel(c interface{}) {
	q.channel = c
}

func (q *QueryHandle) GetChannel() interface{} {
	return q.channel
}

func (q *QueryHandle) GetTable() string {
	return string(q.tbl)
}

func (q *QueryHandle) GetTableName() string {
	return string(q.tbl)
}

func (q *QueryHandle) IndexSpecified() bool {
	return len(q.idx) > 0
}

func (q *QueryHandle) SetScan() {
	q.scan = true
}

func (q *QueryHandle) SetWorker(i int) {
	q.worker = i
}

func (q *QueryHandle) SetTag(s string) {
	q.Tag = s
}

func (q *QueryHandle) Workers(n int) {
	q.SetWorkers(n)
}

func (q *QueryHandle) SetWorkers(n int) {
	q.workers = n
}

func (q *QueryHandle) NumWorkers() int {
	return q.workers
}

// func (q *QueryHandle) Parallel(n int) {
// 	q.parallel = n
// }

// func (q *QueryHandle) GetParallel() int {
// 	return q.parallel
// }

// func (q *QueryHandle) SKset() bool {
// 	return len(q.sk) > 0
// }
func (q *QueryHandle) GetIndex() string {
	return string(q.idx)
}

func (q *QueryHandle) GetIndexName() string {
	return string(q.idx)
}

func (q *QueryHandle) IsScanASCSet() bool {
	return q.so == Forward
}

func (q *QueryHandle) IsScanForwardSet() bool {
	return q.so == Forward
}

func (q *QueryHandle) GetAttr() []*Attr {
	return q.attr
}

func (q *QueryHandle) ConsistentMode() bool {
	if len(q.idx) == 0 {
		return q.css
	} else {
		return false
	}
}

func (q *QueryHandle) GetLimit() int {
	return q.limit
}

func (q *QueryHandle) Access() AccessTy {
	return q.accessTy
}

func (q *QueryHandle) GetFetch() interface{} {
	return q.fetch
}

func (q *QueryHandle) Fetch() interface{} {
	return q.fetch
}

func (q *QueryHandle) SetFetch(v interface{}) {
	q.fetch = v
}

// SetFetch uses reflect to set the internal value of a value.
func (q *QueryHandle) SetFetchValue(v reflect.Value) {
	reflect.ValueOf(q.fetch).Elem().Set(v)
}

// func (q *QueryHandle) GetPkSk() (string, string) {
// 	return q.pk, q.sk
// }

// func (q *QueryHandle) GetPK() string {
// 	return q.pk
// }

// func (q *QueryHandle) GetSK() string {
// 	return q.sk
// }

func (q *QueryHandle) GetKeyValue(n string) interface{} {
	return q.getValue(IsKey, n)
}

func (q *QueryHandle) GetFilterValue(n string) interface{} {
	return q.getValue(IsFilter, n)
}

func (q *QueryHandle) getValue(t Attrty, n string) interface{} {
	for _, v := range q.attr {
		if v.name == n && v.aty == t {
			return v.value
		}
	}
	return nil
}

func (q *QueryHandle) GetKeys() []string {
	var keys []string
	for _, v := range q.attr {
		if v.aty == IsKey {
			keys = append(keys, v.name)
		}
	}
	return keys
}

func (q *QueryHandle) SetEOD() {
	q.eod = true
}

// EOD - end of data for query/scan on table segment (workers)
// should accept int arg for worker id (segmet id)???
func (q *QueryHandle) EOD() bool {

	if len(q.pgStateId) == 0 {
		// query has not configured paginate
		q.err = fmt.Errorf("Query [tag: %s] has not configured paginate. EOD is therefore not available", q.Tag)
		elog.Add(q.Tag, q.err)
		return false
	}
	// check if multiple select bind vars (bv) used and switch appropriate
	// to support non-blocking db reads need two bv per table segment (workers). Five workers requires 10 bv (2 per worker)
	if len(q.bufs) > 0 {
		if q.eodlc > 0 {
			// ignore first execution of EOD to switch buffer
			q.switchBuf()
		}
		q.eodlc++
	}
	return q.eod
}

func (q *QueryHandle) FilterSpecified() bool {
	for _, v := range q.attr {
		if v.aty == IsFilter {
			return true
		}
	}
	return false
}

func (q *QueryHandle) Restart() bool {
	return q.restart
}

// func (q *QueryHandle) State(id uuid.UID, restart bool) {
// 	q.pgStateId = id
// 	q.pgStateVal = ""
// }

func (q *QueryHandle) PgStateId() uuid.UID {
	return q.pgStateId
}

func (q *QueryHandle) SetPgStateValS(val string) {
	q.pgStateValS = val
}

func (q *QueryHandle) PgStateValS() string {
	return q.pgStateValS
}

func (q *QueryHandle) SetPgStateValI(val interface{}) {
	q.pgStateValI = val
}

func (q *QueryHandle) PgStateValI() interface{} {
	return q.pgStateValI
}

func (q *QueryHandle) IsRestart() bool {
	return q.restart
}

func (q *QueryHandle) SetRestart(b bool) {
	q.restart = b
}

// func (q *QueryHandle) LastKey(key, d) string {
// 	return q.varM[key]
// }

// func (q *QueryHandle) SetVarM(key string, d interface{}) {
// 	if q.varM == nil {
// 		q.varM = make(map[string]interface{})
// 	}
// 	q.varM[key] = d
// }

// func (q *QueryHandle) GetVarM(key string) interface{} {
// 	if q.varM == nil {
// 		return nil
// 	}
// 	return q.varM[key]
// }

// func (q *QueryHandle) GetComparitor(n string) string {
// 	for _, v := range q.attr {
// 		if v.name == n && v.aty == IsFilter {
// 			return v.eqy
// 		}
// 	}
// 	return NA
// }

func (q *QueryHandle) GetFilter() []string {
	var flt []string
	for _, v := range q.attr {
		if v.aty == IsFilter {
			flt = append(flt, v.name)
		}
	}
	return flt
}

func (q *QueryHandle) GetFilterAttrs() []*Attr {
	var flt []*Attr
	for _, v := range q.attr {
		if v.aty == IsFilter {
			flt = append(flt, v)
		}
	}
	return flt
}

// GetWhereAttrs - for SQL only
func (q *QueryHandle) GetKeyAttrs() []*Attr {
	var key []*Attr
	for _, v := range q.attr {
		switch v.aty {
		case IsKey:
			key = append(key, v)
		}
	}
	return key
}

// GetWhereAttrs - for SQL only
func (q *QueryHandle) GetWhereAttrs() []*Attr {
	var flt []*Attr
	for _, v := range q.attr {
		switch v.aty {
		case IsFilter:
			flt = append(flt, v)
		case IsKey:
			flt = append(flt, v)
		}
	}
	return flt
}

// func (a Attr) Fetch() bool {
// 	return a.aty == IsFilter
// }
// for Query only. In Dynamodb Attribute must be indexed (LSI or GSI).
// func (q *QueryHandle) OrderBy(a string, ob ...SortOrder) *QueryHandle {

// 	q.orderby = a
// 	q.order = ASC
// 	return q
// }

func (q *QueryHandle) ReadConsistency(b bool) *QueryHandle {
	q.css = b
	return q
}

func (q *QueryHandle) Limit(l int) *QueryHandle {
	q.limit = l
	return q
}

func (q *QueryHandle) Key(a string, v interface{}, e ...string) *QueryHandle {
	// Input fields are fields used as key for query and filter attribute (when non-key)
	// all var fields are addrVal fields, ie. return values from db.

	// delay all validation checks till execute() - as query has no access to db (tried but always run into import cycles which it must based on current design)
	eqy := "EQ"
	if len(e) > 0 {
		eqy = e[0]
	}

	at := &Attr{name: a, value: v, aty: IsKey, eqy: eqy}
	q.attr = append(q.attr, at)

	return q
}

func (q *QueryHandle) PkeyAssigned() bool {
	for _, v := range q.attr {
		if v.aty == IsKey {
			return true
		}
	}
	return false
}

func (q *QueryHandle) GetKeyComparOpr(sk string) string {
	return q.getComparOpr(IsKey, sk)
}

func (q *QueryHandle) GetFilterComparOpr(sk string) string {
	return q.getComparOpr(IsFilter, sk)
}

func (q *QueryHandle) getComparOpr(t Attrty, s string) string {
	for _, a := range q.attr {
		if a.name == s && a.aty == t {
			return a.eqy
		}
	}
	return NA
}

func (q *QueryHandle) Paginate(id uuid.UID, restart bool) *QueryHandle {

	q.restart = restart
	q.pgStateId = id
	return q
}

func (q *QueryHandle) appendFilter(a string, v interface{}, bcd BoolCd, e ...string) {

	eq := "EQ"
	if len(e) > 0 {
		eq = e[0]
	}

	at := &Attr{name: a, value: v, aty: IsFilter, eqy: eq, boolCd: bcd}
	q.attr = append(q.attr, at)

}
func (q *QueryHandle) Filter(a string, v interface{}, e ...string) *QueryHandle {

	var found bool

	for _, a := range q.attr {
		if a.aty == IsFilter {
			found = true
			break
		}
	}
	if found {
		q.AndFilter(a, v, e...)
		// err := errors.New("A filter condition has already been specified. Use either AndFilter or OrFilter")
		// elog.Add("parseQuery", err)
		// q.err = err
		return q
		//
	}

	if q.err != nil {
		return q
	}

	q.appendFilter(a, v, NIL, e...)

	return q
}

func (q *QueryHandle) appendBoolFilter(a string, v interface{}, bcd BoolCd, e ...string) *QueryHandle {

	var found bool

	for _, a := range q.attr {
		if a.aty == IsFilter && a.boolCd == NIL {
			found = true
			break
		}
	}

	if !found {
		err := errors.New(fmt.Sprintf(`Query Tag: %s, no "Filter" condition specified`, q.Tag))
		elog.Add("query", err)
		q.err = err
		return q
		//
	}

	q.appendFilter(a, v, bcd, e...)

	return q
}

func (q *QueryHandle) AndFilter(a string, v interface{}, e ...string) *QueryHandle {
	q.af++
	return q.appendBoolFilter(a, v, AND, e...)

}

func (q *QueryHandle) OrFilter(a string, v interface{}, e ...string) *QueryHandle {
	q.of++
	return q.appendBoolFilter(a, v, OR, e...)
}

func (q *QueryHandle) GetOr() int {
	return q.of
}
func (q *QueryHandle) GetAnd() int {
	return q.af
}

// ScanOrder alias for Sort().  Used by NoSQL - sets order of sort, ascending, descending
func (q *QueryHandle) ScanOrder(so ScanOrder) *QueryHandle {
	// if !q.SKset() {
	// 	panic(fmt.Errorf("When using Sort() a sort key must be specified using Key()"))
	// }
	q.so = so
	return q
}

// Sort alias for ScanOrder used by NoSQL - sets order of sort, ascending, descending
func (q *QueryHandle) Sort(so ScanOrder) *QueryHandle {
	return q.ScanOrder(so)
}

// OrderBy used for SQL
func (q *QueryHandle) OrderBy(ob string, s Orderby) *QueryHandle {
	q.orderBy = orderby{ob, s}
	return q
}

func (q *QueryHandle) HasOrderBy() bool {
	return len(q.orderBy.attr) > 0
}

func (q *QueryHandle) OrderByString() string {
	s := " order by " + q.orderBy.attr
	if q.orderBy.sort == Asc {
		return s + " asc"
	}
	return s + " desc"

}

func (q *QueryHandle) OutBuf() int {
	return q.Result()
}

func (q *QueryHandle) Result() int {
	abuf := q.abuf
	syslog(fmt.Sprintf("Result Buffer id: %d", abuf))
	return abuf
}

func (q *QueryHandle) switchBuf() {

	q.abuf++
	if q.abuf > len(q.bufs)-1 {
		q.abuf = 0
	}
	q.fetch = q.bufs[q.abuf]
	syslogAlert(fmt.Sprintf("switch Buffer now : %d of %d", q.abuf, len(q.bufs))) //reflect.ValueOf(q.abuf).Elem().Len()))

}

func (q *QueryHandle) Bufs() []interface{} {
	return q.bufs
}

// Select allocates attributes in []Attr{}. Used to write attributes in Select clause of SQL statement.
// It also specifies the destination variables (aka bind variables) for the query data which will require dynamic allocation for slice type
// in SQL non-prepared tx means Select may be in a loop which means it can be specified more than once. This is accepted and will
// be processed using stand sql API rather than prepared API. Is this OK?
// Valid inputs:
//
//       *struct
//        stuct
//        []struct
//        []*struct
//
// Struct can include nested types or anonymous embedded type
//
//        var x struct {
//	         Status byte
//           Person
//           Loc    Address
//        }
//
//       type Person struct {
//           FirstName string
//           LastName string
//           DOB string
//      }
//      type Address struct {
//           Line1 , Line2, Line3 string
//           City string
//           Zip string
//           State string
//           Cntry     Country
//      }
//
//      type Country struct {
//           Name string
//           Population int
//      }
//

func (q *QueryHandle) Select(a_ ...interface{}) *QueryHandle {

	if q.err != nil {
		return q
	}

	a := a_[0]

	q.abuf = 0
	q.bufs = a_ // []interface{} []*[]unprocBuf

	// commented out to allow Select in for loop - usually would be Prepared() but may incorrectly not be.
	// if q.select_ && !q.prepare {
	// 	panic(fmt.Errorf("Select already specified. Only one Select permitted."))
	// }

	// q.select_ = true

	t := reflect.TypeOf(a)
	if t.Kind() != reflect.Ptr {
		panic(fmt.Errorf("Fetch argument: expected a pointer, got a %s", t.Kind()))
	}
	//save addressable component of interface argument

	if q.fetch != nil {
		q.fetch = a
		return q
	}
	q.fetch = a

	st := t.Elem()                  // what a points to
	sv := reflect.ValueOf(a).Elem() // what a points to

	if st.Kind() == reflect.Slice {

		// []*struct
		// []struct

		st = st.Elem()
		switch st.Kind() {

		case reflect.Struct:
			sv = sv.Index(0)

		case reflect.Pointer:
			st = st.Elem()
			sv = sv.Elem().Elem()

			if st.Kind() != reflect.Struct {
				panic(fmt.Errorf("QueryHandle Select(): expected a struct got %s", st.Kind()))
			}
		default:
			panic(fmt.Errorf("QueryHandle Select(): expected a struct got %s", st.Kind()))
		}
	}

	if st.Kind() == reflect.Struct {

		// used in GetItem (single row select)
		var name, lit string

		for i := 0; i < st.NumField(); i++ {

			f := st.Field(i) // field as reflect.Type
			ft := f.Type     // field as reflect.Type

			if ft.Kind() == reflect.Struct {

				name = f.Name + "."
				if f.Anonymous {
					name = ""
				}

				q.rSelect(sv.Field(i), name, ft) // recurusive call

			} else {

				if name = f.Tag.Get("dynamodbav"); len(name) == 0 {
					name = f.Name
					lit = f.Tag.Get("literal")
				}
				at := &Attr{name: name, aty: IsFetch, literal: lit}
				q.attr = append(q.attr, at)

			}

		}
	} else {
		panic(fmt.Errorf("QueryHandle Select(): expected a struct got %s", st.Kind()))
	}

	return q

}

func (q *QueryHandle) rSelect(sv reflect.Value, nm string, st reflect.Type) {

	// struct value
	// fn name of struct field

	//save addressable component of interface argument
	if st.Kind() != reflect.Struct {
		panic(fmt.Errorf("QueryHandle Select(),rSelect(): expected a struct got %s", st.Kind()))
	}
	var name string
	for i := 0; i < st.NumField(); i++ {

		f := st.Field(i)
		ft := f.Type

		if ft.Kind() == reflect.Struct {

			name = nm + f.Name + "."
			if f.Anonymous {
				name = nm
			}

			q.rSelect(sv.Field(i), name, ft) // recurusive call

		} else {

			name = nm + f.Name
			if f.Anonymous {
				name = nm
			}

			at := &Attr{name: name, aty: IsFetch}
			q.attr = append(q.attr, at)

		}
	}

}

func (q *QueryHandle) HasInstring() bool {
	for _, v := range q.attr {
		if v.aty == IsKey {
			if strings.ToUpper(v.eqy) != "EQ" {
				return false
			}
		}
	}
	return true
}

// func (q *QueryHandle) MakeResultSlice(size int) reflect.Value {

// 	return reflect.MakeSlice(q.addrValRVal, size, size)

// }

// Split allocates memory for the bind variables if required otherwise allocates user defined variables to []any
// TODO: Used by MySQL only (so far), consequently Split should be in mysql/query (maybe)

func (q *QueryHandle) Split() []interface{} { // )

	q.binds = nil

	v := reflect.ValueOf(q.fetch).Elem() // q.fetch ([]interface{}) is dynamically built with results from query one row at a time.

	if v.Kind() == reflect.Slice {

		// allocate new entry to slice to receive db data.
		// var aa []*struct
		// var aa []struct
		vs := v
		v = v.Elem()

		switch v.Kind() {

		case reflect.Pointer:

			if v.Elem().Kind() == reflect.Struct {

				// var aa []*struct
				// alloc ptr to new struct
				v = reflect.New(v.Elem().Type())
				// append v to q.fetch
				vs.Set(reflect.Append(vs, v))
				// assign v to newly alloc struct
				v = v.Elem()

			} else {
				panic(fmt.Errorf("Split(). Expected pointer or struct got %s", v.Kind()))
			}

		case reflect.Struct:

			// aa []struct
			// use New to allocate memory for a new struct that will be appended to the slice pointed to by q.fetch pointer.
			v = reflect.New(v.Type()).Elem()
			// following Set is equiv to x:=append(x,a) to allow for allocation of a new underlying x array when current array size will be exceeded.
			vs.Set(reflect.Append(vs, v))
			// the bind variables are taken from each field of the struct just appended to the slice
			// mysql will populate them with data from the db, in the Exec() function.
			v = vs.Index(vs.Len() - 1)

		default:
			panic(fmt.Errorf("Split(). Expected pointer or struct got %s", v.Kind()))

		}
	}

	// for new slice entry, struct or scalar, add pointer value to q.binds which will be passed to db.Scan()
	if v.Kind() == reflect.Struct {

		for i := 0; i < v.NumField(); i++ {

			e := v.Field(i)

			if e.Kind() == reflect.Struct {

				// nested types; struct
				q.rBinds(e)

			} else {

				// scalar types
				q.binds = append(q.binds, e.Addr().Interface())
			}
		}

	}

	for i, v := range q.binds {
		slog.Log("Split: ", fmt.Sprintf("bind: %d   %#v\n", i, v))
	}
	return q.binds
}

func (q *QueryHandle) rBinds(v reflect.Value) { // v is a reflect.struct

	for i := 0; i < v.NumField(); i++ {

		e := v.Field(i)

		if e.Kind() == reflect.Struct {

			// nested types; struct
			q.rBinds(e)

		} else {

			// scalar types
			q.binds = append(q.binds, e.Addr().Interface())
		}
	}

}

// 	e := n.Field(i).Elem()

// 	switch e.Kind() {

// 	case reflect.Ptr:

// 		if e.Elem().Kind() == reflect.Slice {

// 			q.rBinds(e) // start recurusive call - pass slice

// 		} else {
// 			panic(fmt.Errorf("Expectect slice got %s", e.Elem().Kind()))
// 		}

// 	case reflect.Slice:

// 		q.rBinds(e) // start recurusive call - pass slice

// 	default:

// 		p := n.Field(i).Addr().Interface()

// 		q.binds = append(q.binds, p)
// 	}
// }
