package tx

import (
	"context"
	//	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ros2hp/method-db/mysql"
	"github.com/ros2hp/method-db/tbl"
)

// var sqltbl = tbl.Name("GoGraph.unit$Test")
var (
	sqltbl = tbl.Name("unit$Test")
	ctx    context.Context
	cancel context.CancelFunc
)

func init() {

	//select test,logdt,status,nodes from Log$GoTest;	/
	// context is passed to all underlying mysql methods which will release db resources on main termination
	ctx, cancel = context.WithCancel(context.Background())

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
}

func compareSQL(n string, t *testing.T, s1, s2 string) {
	if strings.Replace(s1, " ", "", -1) != strings.Replace(s2, " ", "", -1) {
		t.Errorf("%s expected sql %q got %q", n, s1, s2)
	}
}

func TestSQLSubmitx(t *testing.T) {

	type Tab struct {
		Id        int `mdb:",Key"`
		FirstName string
		LastName  string  `mdb:"SirName"`
		Years     int     `mdb:"Age,increment"`
		Salary    float64 `mdb:",multiply"`
		DOB       string
		Height    int
		C         int    `mdb:"Ceiling"`
		Name      string `mdb:"-"`
	}

	in := &Tab{Id: 20, FirstName: "Ross", LastName: "Smith", Years: 59, Salary: 155000, Name: "This is my name", Height: 174, C: 232}
	in2 := &Tab{Id: 21, FirstName: "Paul", LastName: "Smith", Years: 45, Salary: 320000, Name: "Another name", Height: 175, C: 5562}

	data := []*Tab{in, in2}

	utx := New("DataLoad").DB("mysql-GoGraph") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete("unit$Test").Key("Id", 20)
	utx.NewDelete("unit$Test").Key("Id", 21)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api

	for _, r := range data {
		utx.NewInsert("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}
	var recs []Tab
	// note, NewQuery does not solicit table columns so columns passed to Key() can be verify as keys.
	// This check is deferred to database where it will fail during Execute() with db error
	txg := NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id", 20, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if len(recs) != 2 {
		t.Errorf("Expected 2 rows got %d", len(recs))
		return
	}

	if recs[0].Salary != 155000 {
		t.Errorf("Expected Salary of 155000 got %v", recs[0].Salary)
	}
	if recs[1].Salary != 320000 {
		t.Errorf("Expected Salary of 320000 got %v", recs[1].Salary)
	}

	for i, v := range recs {
		t.Logf("Inserted record  %d: %#v", i, v)
	}

	in3 := Tab{Id: 20, Years: 0, Salary: 1.25, Name: "This is my name", DOB: "13-03-1946", FirstName: "Ross", LastName: "Smith"}
	in4 := Tab{Id: 21, Years: 0, Salary: 1.1, Name: "Another name", DOB: "26-01-1977", FirstName: "Paul", LastName: "Smith"}

	data2 := []*Tab{&in3, &in4}

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api

	for _, r := range data2 {
		utx.NewUpdate("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}

	recs = nil
	txg = NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id", 20, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	for i, v := range recs {
		t.Logf("First Update:  %d: %#v", i, v)
	}

	if recs[0].Salary != 193750 {
		t.Errorf("Expected Salary of 193750 got %v", recs[0].Salary)
	}
	if recs[1].Salary != 352000 {
		t.Errorf("Expected Salary of 352000 got %v", recs[1].Salary)
	}
	if recs[0].Years != 60 {
		t.Errorf("Expected Age of 60 got %v", recs[0].Years)
	}
	if recs[1].Years != 46 {
		t.Errorf("Expected Age of 46 got %v", recs[1].Years)
	}
	time.Sleep(1 * time.Second)

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api

	for _, r := range data2 {
		utx.NewUpdate("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}
	recs = nil
	txg = NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id", 20, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}
	for i, v := range recs {
		t.Logf("Second Update:  %d: %#v", i, v)
	}

	if recs[0].Salary != 242187.5 {
		t.Errorf("Expected Salary of 242187.5 got %v", recs[0].Salary)
	}
	if recs[1].Salary != 387200 {
		t.Errorf("Expected Salary of 387200 got %v", recs[1].Salary)
	}

	if recs[0].Years != 61 {
		t.Errorf("Expected Age of 61 got %v", recs[0].Years)
	}
	if recs[1].Years != 47 {
		t.Errorf("Expected Age of 47 got %v", recs[1].Years)
	}

	time.Sleep(1 * time.Second)

	// Below version does works but corrupts data.  work using &r of struct, as local r is a copy of data3 element not the address of the actual element.
	data3 := []Tab{in3, in4}

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api

	for _, r := range data3 {
		utx.NewUpdate("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), `using unaddressable value`) {
			t.Errorf(`SQLSubmit Error: expected error "using unaddressable value" got %q`, err)
		}
	}

	// recs = nil
	// txg = NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	// txg.Select(&recs).Key("Id", 20, "GE")
	// err = txg.Execute()
	// if err != nil {
	// 	t.Errorf("Error: %s", err)
	// }

	// for i, v := range recs {
	// 	t.Logf("Final update  %d: %#v", i, v)
	// }
	//sql_test.go:125: rec 0: tx.Tab{Id:20, FirstName:"Ross", LastName:"Smith", Years:60, Salary:193750, DOB:"0000-00-00", Height:0, C:0, Name:""}
	//sql_test.go:125: rec 1: tx.Tab{Id:21, FirstName:"Paul", LastName:"Smith", Years:46, Salary:352000, DOB:"0000-00-00", Height:0, C:0, Name:""}
}

func TestSQLSubmit2(t *testing.T) {

	type Tab struct {
		Id        int
		FirstName string
		LastName  string `mdb:"SirName"`
		Years     int    `mdb:"Age"`
		Salary    float64
		DOB       string
		Height    int
		C         int    `mdb:"Ceiling"`
		Name      string `mdb:"-"`
	}

	in := &Tab{Id: 20, FirstName: "Ross", LastName: "Smith", Years: 59, Salary: 155000, Name: "This is my name", Height: 174, C: 232}
	in2 := &Tab{Id: 21, FirstName: "Paul", LastName: "Smith", Years: 45, Salary: 320000, Name: "Another name", Height: 175, C: 5562}

	data := []*Tab{in, in2}

	utx := New("DataLoad").DB("mysql-GoGraph") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete("unit$Test").Key("Id", 20)
	utx.NewDelete("unit$Test").Key("Id", 21)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api

	for _, r := range data {
		utx.NewInsert("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}

	var recs []Tab
	// note, NewQuery does not solicit table columns so columns passed to Key() can be verify as keys.
	// This check is deferred to database where it will fail during Execute() with db error
	txg := NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id", 20, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if len(recs) != 2 {
		t.Errorf("Expected 2 rows got %d", len(recs))
		return
	}

	if recs[0].Salary != 155000 {
		t.Errorf("Expected Salary of 155000 got %v", recs[0].Salary)
	}
	if recs[1].Salary != 320000 {
		t.Errorf("Expected Salary of 320000 got %v", recs[1].Salary)
	}

	for i, v := range recs {
		t.Logf("Inserted record  %d: %#v", i, v)
	}

	type TabU struct {
		Id     int     `mdb:",Key"`
		Years  int     `mdb:"Age,increment"`
		Salary float64 `mdb:",multiply"`
		DOB    string
		Name   string `mdb:"-"`
	}

	in3 := TabU{Id: 20, Years: 0, Salary: 1.25, Name: "This is my name", DOB: "13-03-1946"}
	in4 := TabU{Id: 21, Years: 0, Salary: 1.1, Name: "Another name", DOB: "26-01-1977"}

	data2 := []*TabU{&in3, &in4}

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api
	// utx.NewUpdate("unit$Test").Submit(data2)
	// err = utx.Execute()
	for _, r := range data2 {
		utx.NewUpdate("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}

	recs = nil
	txg = NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id", 20, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	for i, v := range recs {
		t.Logf("First Update:  %d: %#v", i, v)
	}

	if recs[0].Salary != 193750 {
		t.Errorf("Expected Salary of 193750 got %v", recs[0].Salary)
	}
	if recs[1].Salary != 352000 {
		t.Errorf("Expected Salary of 352000 got %v", recs[1].Salary)
	}
	if recs[0].Years != 60 {
		t.Errorf("Expected Salary of 60 got %v", recs[0].Years)
	}
	if recs[1].Years != 46 {
		t.Errorf("Expected Salary of 46 got %v", recs[1].Years)
	}
	time.Sleep(1 * time.Second)

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api

	for _, r := range data2 {
		utx.NewUpdate("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}
	recs = nil
	txg = NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id", 20, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}
	for i, v := range recs {
		t.Logf("Second Update:  %d: %#v", i, v)
	}

	if recs[0].Salary != 242187.5 {
		t.Errorf("Expected Salary of 242187.5 got %v", recs[0].Salary)
	}
	if recs[1].Salary != 387200 {
		t.Errorf("Expected Salary of 387200 got %v", recs[1].Salary)
	}

	if recs[0].Years != 61 {
		t.Errorf("Expected Salary of 61 got %v", recs[0].Years)
	}
	if recs[1].Years != 47 {
		t.Errorf("Expected Salary of 47 got %v", recs[1].Years)
	}

	time.Sleep(1 * time.Second)

	// Below version does works but corrupts data.  work using &r of struct, as local r is a copy of data3 element not the address of the actual element.
	data3 := []TabU{in3, in4}

	utx = NewTx("DataLoad").DB("mysql-GoGraph") // std api

	for _, r := range data3 {
		utx.NewUpdate("unit$Test").Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), `using unaddressable value`) {
			t.Errorf(`SQLSubmit Error: expected error "using unaddressable value" got %q`, err)
		}
	}

	// recs = nil
	// txg = NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	// txg.Select(&recs).Key("Id", 20, "GE")
	// err = txg.Execute()
	// if err != nil {
	// 	t.Errorf("Error: %s", err)
	// }

	// for i, v := range recs {
	// 	t.Logf("Final update  %d: %#v", i, v)
	// }
	//sql_test.go:125: rec 0: tx.Tab{Id:20, FirstName:"Ross", LastName:"Smith", Years:60, Salary:193750, DOB:"0000-00-00", Height:0, C:0, Name:""}
	//sql_test.go:125: rec 1: tx.Tab{Id:21, FirstName:"Paul", LastName:"Smith", Years:46, Salary:352000, DOB:"0000-00-00", Height:0, C:0, Name:""}
}

// TestSQLUpdateView tests updating through a view.
// Restrictions: view references only one table for DML.
// For DML mdb must determine keys in table by interrogating view definition..how?
// cache contains views & tables. If not in cache, presume it is a table
// if no rows returned, then it must be a view. Get view def. Parse for table name and table keys.
// stores keys with view in cache. Are keys renamed in view?
// For query: how does mdb determine keys for multi-table view def? Does it defer to database just like in a single table query.

// SQLNonKeyReference. Unlike Dynamodb keys are not sourced from cache (db) prior to each query execute. As a result keys are not checked during Key().
// Checks on keys are deferred to the database duing Execute() for SQL databases owing to the complication of views and multi-table references.
// no big deal I would imagine. Key() are still used to target rows for all DML and query operations.
// as a result no checks are made of field names against column names. Any non-column field names must be ignored using "-" tag value
// otherwise a database SQL error will occur.
func TestSQLNonKeyReference(t *testing.T) {

	type Tab struct {
		Id        int `mdb:",Key"`
		FirstName string
		LastName  string  `mdb:"SirName"`
		Age       int     `mdb:",increment"`
		Salary    float64 `mdb:",multiply"`
		DOB       string
		Height    int    `mdb:"-"`
		C         int    `mdb:"Ceiling"`
		Name      string `mdb:"-"`
	}

	var recs []Tab

	txg := NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id2", 20, "GE")
	err = txg.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), `Unknown column 'Id2' in 'where clause'`) {
			t.Errorf("TestSQLNonKeyReference Error: %s", err)
		}
	}

}

// TestSQLTableColumns - unlike dynamodb query does not fetch table columns and check fields in struct are in table. It defers check to database at execute time.
// developer must tell mdb to ignore field using tag value "-"
// similarly key values are not checked (as we don't have column spec) to key checks are deferred to datbase at execute time.
func TestSQLTableColumns(t *testing.T) {

	type Tab struct {
		Id        int `mdb:",Key"`
		FirstName string
		LastName  string  `mdb:"SirName"`
		Age       int     `mdb:",increment"`
		Salary    float64 `mdb:",multiply"`
		DOB       string
		Height    int    `mdb:"-"`
		C         int    `mdb:"Ceiling"`
		Name      string `mdb:"-"`
	}

	var recs []Tab

	txg := NewQuery("query-test-label", "unit$Test").DB("mysql-GoGraph")
	txg.Select(&recs).Key("Id", 20, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	for i, v := range recs {
		t.Logf("rec %d: %#v", i, v)
	}

}

func TestSQLLoadData(t *testing.T) {

	var i int = 10

	ltx := New("SQLLoadData").DB("mysql-GoGraph")
	u := ltx.NewInsert(sqltbl).Attribute("Id", i).Attribute("SirName", "Ian").Attribute("LastName", "Paine").Attribute("Age", 69).Attribute("Salary", "250000")
	u.Attribute("DOB", "01-Jan-1955")
	u.Attribute("Height", 176).Attribute("Ceiling", 123)
	// i++
	// u = ltx.NewInsert(sqltbl).Key("Id", i).Key("SirName", "Ross").Attribute("LastName", "Payne").Attribute("Age", 64).Attribute("Salary", "150000").Attribute("DOB", "13-Aug-1958")
	// u.Attribute("Height", 173).Attribute("Ceiling", 1024)
	// i++
	// u = ltx.NewInsert(sqltbl).Key("Id", i).Key("SirName", "Paul").Attribute("LastName", "Payne").Attribute("Age", 55).Attribute("Salary", "180000").Attribute("DOB", "2-Jul-1964")
	// u.Attribute("Height", 168).Attribute("Ceiling", 2145)

	err := ltx.Execute()

	if !strings.Contains(err.Error(), "Unknown column") {
		t.Errorf(`SQLLoadData: expected error "Unknown column" got %q`, err.Error())
	}

}

func TestSQLUpdateStaffx(t *testing.T) {

	expectedSQL := `select /* tag: query-test-label  */ Id,SirName,Age,Salary,DOB from unit$Test where (Id>=? and Id<?)  and (Age>=? and Salary>=?)`
	type Person struct {
		Id       int
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int `mdb:"-"`
	}
	var (
		staff []Person
		err   error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GE").Key("Id", 10, "LT").Filter("Age", 30, "GE").Filter("Salary", 100000, "GE")
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}
	compareSQL("SQLUpdateStaff", t, expectedSQL, txg.GetSQL())

	if len(staff) != 4 {
		t.Errorf("SQLUpdateStaff: Expected 4 got %d", len(staff))
	}

	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffOr(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GT").Filter("Age", 30, "GE").OrFilter("Salary", 100000, "GE")
	err = txg.Execute()

	t.Logf("SQL: %s", txg.GetSQL())
	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate(sqltbl).Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffOrAnd(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GE").Filter("Age", 30, "GE").OrFilter("Salary", 100000, "GE").AndFilter("SirName", "Payne")
	err = txg.Execute()

	if err != nil {
		if err.Error() != `Cannot mix OrFilter, AndFilter conditions. Use Where() & Values() instead` {
			t.Errorf("Error: %s", err)
			t.Fail()
		}
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate(sqltbl).Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffKeyWhere(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GT").Where(" AGE > 30 or salary > 100000")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate(sqltbl).Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffWhere(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Where("Id > 2 and AGE > 30 and salary > 100000")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate(sqltbl).Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaffWhereOr(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Where("Id > 2 and (AGE > 30 or salary > 100000)")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate(sqltbl).Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}
func TestSQLUpdateStaffWhereOr2(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination

	var b int
	var c float64
	//a = 2
	b = 30
	c = 100000

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph").Prepare()

	for i := 0; i < 4; i++ {
		txg.Select(&staff).Key("Id", i, "GT").Where(" AGE > ? or salary > ?").Values(b+i, c)
		err = txg.Execute()

		if err != nil {
			t.Logf("Error: %s", err)
		}
		for i, v := range staff {

			t.Logf("Staff %d  %#v\n", i, v)
		}
		staff = nil
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate(sqltbl).Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func xfW(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination

	var b int
	var c float64
	//a = 2
	b = 30
	c = 100000

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph").Prepare()

	for i := 0; i < 4; i++ {
		txg.Select(&staff).Key("Id", i, "GT").Where(" AGE > ? or salary > ?").Values(b+i, c).Limit(1)
		err = txg.Execute()

		if err != nil {
			t.Logf("Error: %s", err)
		}
		for i, v := range staff {

			t.Logf("Staff %d  %#v\n", i, v)
		}
		staff = nil
	}

	// 	txg := New("LogTest").DB("mysql-GoGraph")
	// 	txg.NewUpdate(sqltbl).Key("test", "TestMoviex")
	// 	err := txg.Execute()

	// 	if err != nil {
	// 		t.Logf("Error: %s", err)
	// 	}

	// t.Logf("Query count %d\n", len(sk))
	//
	//	for _, v := range sk {
	//		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	//	}
}

func TestSQLUpdateStaff9(t *testing.T) {

	type Person struct {
		Id       int
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	if len(staff) != 1 {
		t.Errorf("TestSQLUpdateStaff9 expected 1 item returned got %d", len(staff))
	}
	ageBefore := staff[0].Age
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate(sqltbl).Key("Id", "2").Filter("Salary", 600000, "GE").Add("Age", 1)
	// var i int
	// for {
	// 	m, err := txu.GetMutation(i)
	// 	if err != nil {
	// 		if !strings.Contains(err.Error(), `Requested mutation index 0 is greater than number of mutations`) {
	// 			t.Logf("SQLLoadData error in GetMutation(): %s", err)
	// 		}
	// 		break
	// 	}
	// 	t.Logf("SQL: [%d] %s", i, m.GetSQL())
	// 	i++
	// }

	err = txu.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	staff = nil
	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	if staff[0].Age != ageBefore+1 {
		t.Errorf("TestSQLUpdateStaff9 expected Age %d got %d", staff[0].Age, ageBefore)
	}
}

func TestSQLUpdateReturn(t *testing.T) {
	t.Fail()
}

func TestSQLUpdateStaff10(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate(sqltbl).Key("Id", "2").Filter("Salary", 600000, "GE").Add("Age", 1)
	err = txu.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	staff = nil
	txg.Select(&staff).Key("Id", 2)
	err = txg.Execute()
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffWhereOr3(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination

	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate(sqltbl).Key("Id", 2, "GE").Add("Age", 1).Where(" AGE > ? or salary > ?").Values(b, c)
	err = txu.Execute()

	txg.Select(&staff).Key("Id", 2, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffWhereOr4(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
	}
	var (
		staff []Person

		err error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination

	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error in first query: %s", err)
	}
	for i, v := range staff {
		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate(sqltbl).Key("Id", 2, "GE").Add("Age", 1).Where("Salary>=Age")
	err = txu.Execute()
	if err != nil {
		t.Errorf("Error in LogTest update: %s", err)
	}

	txg = NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")
	txg.Select(&staff).Key("Id", 2, "GE")
	err = txg.Execute()

	if err != nil {
		t.Errorf("Error in txg last select: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffWhere5(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
		Ceiling  int
	}
	var (
		staff []Person

		err error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination

	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate(sqltbl).Key("Id", 2, "GE").Add("Age", 1).Where("Height>=Ceiling")
	err = txu.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	txg2 := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph")
	txg2.Select(&staff).Key("Id", 2, "GE")
	err = txg2.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLUpdateStaffFilter5(t *testing.T) {

	type Person struct {
		Id       int
		SirName  string
		LastName string `mdb:"SirName"`
		Age      int
		Salary   float64
		DOB      string
		Height   int
		Ceiling  int
	}
	var (
		staff []Person

		err error
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination

	var b int
	var c float64
	//a = 2
	b = 23
	c = 100000

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph").Prepare()

	txg.Select(&staff).Key("Id", 2, "GE").Where(" AGE > ? or salary > ?").Values(b, c)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
	staff = nil

	txu := New("LogTest").DB("mysql-GoGraph")
	txu.NewUpdate(sqltbl).Key("Id", 2, "GE").Add("Age", 1).Filter("Height", "Ceiling", "GE")
	err = txu.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	txg.Select(&staff).Key("Id", 2, "GE")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	for i, v := range staff {

		t.Logf("Staff %d  %#v\n", i, v)
	}
}

func TestSQLPaginate(t *testing.T) {

	type Rec struct {
		Id int
		A  []byte
		B  string
		C  int
		D  float64
	}

	var (
		recs     []*Rec
		sqltbl   = tbl.Name("test$Page")
		err      error
		pageSize = 5
	)
	// context is passed to all underlying mysql methods which will release db resources on main termination

	a := "B"
	e := "H"
	b := "AI"
	c := 19

	txg := NewQuery("query-test-label", sqltbl).DB("mysql-GoGraph").Prepare()
	id := 0
	for i := 0; i < 3; i++ {
		txg.Select(&recs).Key("Id", id, "GT").Where(` A > ? and A < ? or (B >= ? and C < ? )`).Values(a, e, b, c+i).Limit(pageSize).OrderBy("Id")
		err = txg.Execute()

		if err != nil {
			break
		}
		for i, v := range recs {
			t.Logf("rec %d  %#v\n", i, v)
		}
		if len(recs) == 0 {
			break
		}
		t.Logf("len(recs %d", len(recs))
		id = recs[len(recs)-1].Id
		t.Log("\nNext page....")
		recs = nil
	}

	if err != nil {
		t.Errorf("Error: %s", err)
	}

}

func TestSQLQueryTypesPtrSliceSQL(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	var sk []Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if len(sk) != 28 {
		t.Errorf("Expected len(sk) to be 28, got %d", len(sk))
	}

	t.Logf("Query count %d\n", len(sk))
	for _, v := range sk {
		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	}
}

func TestSQLQueryThreeBindvars(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	var (
		sk  []Testlog
		sk2 []Testlog
		sk3 []Testlog
	)

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk, &sk2, &sk3).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		if err.Error() != `no more than two bind variables are allowed.` {
			t.Errorf(`Expected Error: "no more than two bind variables are allowed." got %q`, err)
		}
	} else {
		t.Errorf(`Expected Error: "no more than two bind variables are allowed."`)
	}

}

func TestSQLQueryZeroBindvars(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select().Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		if err.Error() != `requires upto two bind variables` {
			t.Errorf(`Expected Error: "requires upto two bind variables" got %q`, err)
		}
	} else {
		t.Errorf(`Expected Error: "requires upto two bind variables"`)
	}

}

func TestSQLQueryTypesPtrSlicePtrSQL(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Errorf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	}
}

func TestSQLQueryTypesPtrSlicePtrNestedSQL(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Testlog struct {
		Test  string
		Logdt string
		Nstatus
	}
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Test, v.Logdt, v.Status, v.Nodes)
	}
}

func TestSQLQueryTypesPtrSliceNested2SQL(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test  string `mdb:"Test"`
		Logdt string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}
	var sk []Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Logdt, v.Status, v.Nodes)
	}
}

func TestSQLQueryTypesPtrSlicePtrNested2SQL(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test  string `mdb:"Test"`
		Logdt string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Logdt, v.Status, v.Nodes)
	}
}

func TestSQLQueryTypesPtrSlicePtrNested2SQL2(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test  string `mdb:"-"`
		Logdt string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Logdt, v.Status, v.Nodes)
	}
}

func TestSQLQueryTypesPtrSlicePtrNested2SQL3(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Ntest struct {
		Test string `mdb:"-"`
		Fred string `mdb:"Logdt"`
	}

	type Testlog struct {
		Xyz Ntest
		Nstatus
	}
	var sk []*Testlog

	txg := NewQuery("LogTest", "Log$GoTest").DB("mysql-GoGraph")
	txg.Select(&sk).Key("test", "TestMoviex")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))

	for _, v := range sk {
		t.Log(v.Xyz.Test, v.Xyz.Fred, v.Status, v.Nodes)
	}
}
