package tx

import (
	"context"
	//	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ros2hp/method-db/db"
	dyn "github.com/ros2hp/method-db/dynamodb"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/mysql"
	"github.com/ros2hp/method-db/tbl"
)

type Person struct {
	FirstName string
	LastName  string
	DOB       string
}

type Country struct {
	Name       string
	Population int
	Person
	Nperson Person
}

type Address struct {
	Line1, Line2, Line3 string
	City                string
	Zip                 string
	State               string
	Cntry               Country
}

var (
	err  error
	gtbl tbl.Name = "GoUnitTest"
)

func init() {

	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

}

func TestSelect(t *testing.T) {

	type Input struct {
		Status byte
		Person
		Loc Address
	}

	p := Person{FirstName: "John", LastName: "Payneh", DOB: "13 March 1967"}
	//c := Country{Name: "Australia", Population: 23000}
	c := Country{Name: "Australia", Population: 23000, Person: p, Nperson: p}
	ad := Address{Line1: "Villa 647", Line2: "5 Bukitt St", Line3: "Page", Zip: "2678", State: "ACU", Cntry: c}
	x := Input{Status: 'C', Person: p, Loc: ad}

	nn := NewQuery("label", "table")
	nn.Select(&x)

	if len(nn.GetAttr()) != 18 {
		t.Errorf("Expected 17 attributes got %d", len(nn.GetAttr()))
	}
	for k, v := range nn.GetAttr() {
		t.Logf("%d: name: %s", k, v.Name())
		if k == 0 && v.Name() != "Status" {
			t.Errorf(`Expected "Status" attributes got %q`, v.Name())
		}
		if k == 17 && v.Name() != "Loc.Cntry.Nperson.DOB" {
			t.Errorf(`Expected "Loc.Cntry.Nperson.DOB" attributes got %q`, v.Name())
		}
	}

	for i, v := range nn.Split() {
		switch x := v.(type) {
		case *int:
			t.Logf("%d: int: [%d]", i, *x)
		case *uint8:
			t.Logf("%d: uint8: [%d]", i, *x)
		case *string:
			t.Logf("%d: String: [%s]", i, *x)
			if i == 17 && *x != "13 March 1967" {
				t.Errorf(`Expected "13 March 1967" attributes got %q`, *x)
			}
		}

	}
}

func TestSelectSlice(t *testing.T) {

	type Input struct {
		Status byte
		Person
		Loc Address
	}

	var x []Input
	//x := Input{Status: 'C', Person: p, Loc: ad}

	nn := NewQuery("label", "table")
	nn.Select(&x)

	for k, v := range nn.GetAttr() {
		t.Logf("%d: name: %s", k, v.Name())
	}

	for i := 0; i < 4; i++ {
		for _, v := range nn.Split() {

			switch x := v.(type) {
			case *int:
				*x = 234 * i
				t.Logf("%d: int: [%d]", i, *x)
			case *uint8:
				*x = '$'
				t.Logf("%d: uint8: [%d]", i, *x)
			case *string:
				*x = "abcdefg"
				t.Logf("%d: String: [%s]", i, *x)
			}
		}
	}
}

func TestSelectSlicePtr(t *testing.T) {

	type Input struct {
		Status byte
		Person
		Loc Address
	}

	var x []*Input
	//x := Input{Status: 'C', Person: p, Loc: ad}

	nn := NewQuery("label", "table")
	nn.Select(&x)

	for k, v := range nn.GetAttr() {
		t.Logf("%d: name: %s", k, v.Name())
	}

	for i := 0; i < 4; i++ {
		for _, v := range nn.Split() {

			switch x := v.(type) {
			case *int:
				*x = 234 * i

			case *uint8:
				*x = '$'

			case *string:
				*x = "abcdefg" + strconv.Itoa(i)

			}
		}
	}

	for i, v := range x {
		t.Logf("%d: status: [%d], State: %s", i, v.Loc.Cntry.Population, v.Loc.State)
	}

}

type tyNames struct {
	ShortNm string `dynamodbav:"SortK"`
	LongNm  string `dynamodbav:"Name"`
}

func TestQueryTypesPtrSlice(t *testing.T) {

	type graphMeta struct {
		SortK string
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk []*graphMeta

	txg := NewQuery("GraphName", "GoGraphSS")
	txg.Select(&sk).Key("PKey", "#Graph").Filter("Name", "Movies")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query count %d\n", len(sk))
	if len(sk) != 1 {
		t.Errorf(`Expected 1 got %d`, len(sk))
	}
	for _, v := range sk {
		t.Logf("Query count %#v\n", v.SortK)
		if v.SortK != "m" {
			t.Errorf(`Expected "m" got %q`, v.SortK)
		}
	}

}

func TestQueryTypesStruct(t *testing.T) {

	type graphMeta struct {
		SortK string
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk graphMeta

	txg := NewQuery("GraphName", "GoGraphSS")
	txg.Select(&sk).Key("PKey", "#Graph").Filter("Name", "Movies")
	err := txg.Execute()

	if err != nil {
		if err.Error() != "Bind variable in Select() must be slice for a query database operation" {
			t.Errorf("Expected error: %s", err)
		}
	} else {

		t.Logf("Expected error: Bind variable in Select() must be slice for a query database operation")
	}

}

func TestQueryTypesPtrSliceSQL(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryThreeBindvars(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryZeroBindvars(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;

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

func TestQueryTypesPtrSlicePtrSQL(t *testing.T) {

	type Testlog struct {
		Test   string
		Logdt  string
		Status string
		Nodes  int
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryTypesPtrSlicePtrNestedSQL(t *testing.T) {

	type Nstatus struct {
		Status string
		Nodes  int
	}

	type Testlog struct {
		Test  string
		Logdt string
		Nstatus
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryTypesPtrSliceNested2SQL(t *testing.T) {

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryTypesPtrSlicePtrNested2SQL(t *testing.T) {

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryTypesPtrSlicePtrNested2SQL2(t *testing.T) {

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryTypesPtrSlicePtrNested2SQL3(t *testing.T) {

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mysql.Register(ctx, "mysql-GoGraph", os.Getenv("MYSQL")+"/GoGraph")
	//select test,logdt,status,nodes from Log$GoTest;
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

func TestQueryPop1(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)
	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

}

func TestQueryPop2(t *testing.T) {

	type City struct {
		Pop int `mdb:"Population"` // will not convert to dynamodbav
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)
	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

}

func TestQueryPopUpdate(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).AddMember("PKey", 1001, mut.IsKey).AddMember("SortK", "Sydney", mut.IsKey).AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)
	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateKey(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey", 1001, "GT").Key("SortK", "Sydney").Set("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		if err.Error() != `Expression error in txUpdate. Equality operator for Dynamodb Partition Key must be "EQ"` {
			t.Errorf("Update error: %s", err.Error())
		}
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)
	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}
func TestQueryPopUpdate2(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).AddMember("PKey", 1001).AddMember("SortK", "Sydney").AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)
	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdate3(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateSet(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Set("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateSetKeyWrong(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Set("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateError(t *testing.T) {
	var err error

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey2", 1001).Key("SortK", "Sydney").AddMember("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		if err.Error() != `Error in Tx setup (see system log for complete list): Error: "PKey2" is not a key in table` {
			t.Errorf("Update error: %s", err.Error())
		}
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}

func TestQueryPopUpdateSAdd(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Add("Population", 1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop+1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere22(t *testing.T) {

	type City struct {
		SortK string
		Pop   int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation)`).Subtract("Population", 1)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere23(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists("MaxPopulation") and Population<MaxPopulation`).Subtract("Population", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) `).Subtract("Population", 1)

	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update error: %s", err.Error())
		}
		t.Logf("xxxx error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere23a(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists("MaxPopulation") and Population<MaxPopulation`).Subtract("Population", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_not_exists(MaxPopulation) `).Subtract("Population", 1)

	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update error: %s", err.Error())
		}
		//	t.Logf("xxxx error: %s", err.Error())
	}
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}

// func TestShortNames(t *testing.T) {
// 	var (
// 		past []byte
// 	)
// 	past = append(past, 'a'-1)
// 	// aa, ab...az, ba,..bz, ca, .. cz, da,..dz, ea
// 	for i := 0; i < 1800; i++ {

// 		for i := len(past) - 1; i >= 0; i-- {
// 			past[i]++
// 			if past[i] == 'z'+1 {
// 				if i == 0 && past[0] == 'z'+1 {
// 					past = append(past, 'a')
// 					for ii := i; ii > -1; ii-- {
// 						past[ii] = 'a'
// 					}
// 					break
// 				} else {
// 					past[i] = 'a'
// 				}
// 			} else {
// 				break
// 			}
// 		}
// 		t.Logf("subnn: %s\n", past)
// 	}

// }

func TestQueryPopUpdateWhere24(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>20000`).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update xtz error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere25(t *testing.T) {

	var (
		plimit = 20000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>?`).Values(plimit).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Errorf("Update xtz error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere26(t *testing.T) {

	var (
		plimit = 20000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and (Population>? or Population<?)`).Values(plimit).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "expected 2 bind variables in Values, got 1") == -1 {
			t.Errorf("Update error: %s", err.Error())
		}
	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere28(t *testing.T) {

	var (
		plimit  = 20000000
		plimit2 = 100000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and (Population>? or Population<?)`).Values(plimit, plimit2).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException") == -1 {
			t.Logf("Update xtz error: %s", err.Error())
			err = nil
		}

	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	// expect no change
	if sk2.Pop != sk.Pop {
		t.Fail()
	}

}

func TestQueryPopUpdateWhere29(t *testing.T) {

	var (
		plimit  = 2000000
		plimit2 = 200000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}
	var wpEnd sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dyn.Register(ctx, "default", &wpEnd, []db.Option{db.Option{Name: "Region", Val: "us-east-1"}}...)

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and (Population>? or Population<?)`).Values(plimit, plimit2).Decrement("Population")
	err = utx.Execute()
	if err != nil {

		t.Errorf("Update xtz error: %s", err.Error())

	}
	//tce := &types.TransactionCanceledException{}

	// if errors.As(err, &tce) {
	// 	for _, e := range tce.CancellationReasons {

	// 		if *e.Code == "ConditionalCheckFailed" {
	// 			t.Errorf("Update xtz error: %s", tce.Error())
	// 		}
	// 	}
	// }
	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if sk2.Pop != sk.Pop-1 {
		t.Fail()
	}

}

func TestUpdateList(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA")
	utx.NewUpdate(gtbl).Key("PKey", 2500).Key("SortK", "AAA").AddMember("ListT", listinput)
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The provided expression refers to an attribute that does not exist in the item") {

			t.Logf("Error: in Execute of PreTestUpdateList %s", err)
			t.Fatal()
		}
	}
}

func TestPutList(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	type output struct {
		ListT []int64
	}

	var out_ output

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA").AddMember("ListT", listinput).Add("cond", 5)
	err = utx.Execute()

	if err != nil {
		t.Logf("Error: in Execute of PreTestPutList %s", err)
		t.Fatal()
	}

	q := NewQuery("qlablel", gtbl)
	q.Select(&out_).Key("PKey", 2500).Key("SortK", "AAA")
	err := q.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of qlabel %s", err)
	}

	if len(out_.ListT) != 10 {
		t.Errorf("Expected len of 10 got %d", len(out_.ListT))
	}

	utx2 := New("PostTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx2.NewDelete(gtbl).Key("PKey", 2500).Key("SortK", "AAA") //.Where(`attribute_exists(MaxPopulation)`)
	err = utx2.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of PostTestPutList %s", err)
	}

}

func TestPutUpdateList(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	type output struct {
		ListT []int64
	}

	var out_ output

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA").AddMember("ListT", listinput).Add("cond", 5)
	utx.NewUpdate(gtbl).Key("PKey", 2500).Key("SortK", "AAA").AddMember("ListT", listinput)
	err = utx.Execute()

	if err != nil {
		t.Logf("Error: in Execute of Pre PutUpdateList %s", err)
		t.Fatal()
	}

	q := NewQuery("qlablel", gtbl)
	q.Select(&out_).Key("PKey", 2500).Key("SortK", "AAA")
	err := q.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of qlabel %s", err)
	}

	if len(out_.ListT) != 20 {
		t.Errorf("Expected len of 10 got %d", len(out_.ListT))
	}

	utx = New("PostTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 2500).Key("SortK", "AAA") //.Where(`attribute_exists(MaxPopulation)`)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of Post PutUpdateList %s", err)
	}

}

func TestDeleteCond(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA").AddMember("ListT", listinput)
	err = utx.Execute()

	if err != nil {
		t.Logf("Error: in Execute of Pre DeleteCond %s", err)
		t.Fatal()
	}

	utx = New("PostTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 2500).Key("SortK", "AAA").Where(`attribute_exists(MaxPopulation)`)
	err = utx.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), "ConditionalCheckFailedException") {

			t.Logf("Error: in Execute of PreTestUpdateList %s", err)
			t.Fatal()
		}
	}

	utx = New("PostTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 2500).Key("SortK", "AAA")
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of  DeleteCond %s", err)
	}

}

func TestMarshallInput(t *testing.T) {

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int
	}

	r := Record{
		PKey:      3000,
		SortK:     "MarshallInput",
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}

	var or Record

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Submit(&r)
	err = utx.Execute()

	if err != nil {
		t.Logf("Error: in Execute of Pre DeleteCond %s", err)
		t.Fatal()
	}

	q := NewQuery("qlablel", gtbl)
	q.Select(&or).Key("PKey", 3000).Key("SortK", "MarshallInput")
	err := q.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of qlabel %s", err)
	}

	if len(or.Bytes) != 2 {
		t.Logf("Error: Query expected len of 2 got %d", len(or.Bytes))
		t.Fatal()
	}

}

func TestMarshallInput2(t *testing.T) {

	type Base struct {
		Bytes     []byte
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int
	}

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int
		Another   Base
	}

	r := Record{
		PKey:      3000,
		SortK:     "MarshallInput2",
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}
	r.Another = Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}

	r2 := Record{
		PKey:      3000,
		SortK:     "MarshallInput2",
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e", "f", "g", "h"},
		Numbers:   []int{1, 2, 3},
	}
	r2.Another = Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e2", "f2", "g2", "h2"},
		Numbers:   []int{1, 2, 3},
	}

	var or Record

	utx := New("PreTestUpdList") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_not_exists(PKey)")

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_exists(PKey)")
	err = utx.Execute()

	if err != nil {
		t.Logf("Error: in Execute of Pre MarshallInput2 %s", err)
		t.Fatal()
	}

	q := NewQuery("qlablel", gtbl)
	q.Select(&or).Key("PKey", 3000).Key("SortK", "MarshallInput2")
	err := q.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of qlabel %s", err)
	}

	if len(or.Bytes) != 2 {
		t.Logf("Error: Query expected len of 2 got %d", len(or.Bytes))
		t.Fatal()
	}
	utx = New("MarshallInput2") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 3000).Key("SortK", "MarshallInput2")
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of  DeleteCond %s", err)
	}

}

func TestTxMarshallInput3(t *testing.T) {

	type Base struct {
		Bytes     []byte
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int
	}

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int
		Another   Base
	}

	r := Record{
		PKey:      3000,
		SortK:     "TxMarshallInput3",
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}
	r.Another = Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}

	r2 := Record{
		PKey:      3001,
		SortK:     "TxMarshallInput3",
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e", "f", "g", "h"},
		Numbers:   []int{1, 2, 3},
	}
	r2.Another = Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e2", "f2", "g2", "h2"},
		Numbers:   []int{1, 2, 3},
	}

	utx := New("PreTestTxMarshallInput3") // std api

	utx.NewInsert(gtbl).Submit(&r)

	utx.NewInsert(gtbl).Submit(&r2)

	err = utx.Execute()

	utx = NewTx("PreTestTxMarshallInput3a") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_exists(PKey)")

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_not_exists(PKey)")
	err = utx.Execute()

	if err != nil {
		if strings.Index(err.Error(), "Transaction cancelled") == 0 {
			t.Logf(`Error: in Execute of TxMarshallInput3 Expected "Transaction cancelled" got %q`, err)
			t.Fatal()
		}
		t.Logf("valid output: %s", err)
	}

	utx = New("TxMarshallInput3") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 3000).Key("SortK", "TxMarshallInput3")
	utx.NewDelete(gtbl).Key("PKey", 3001).Key("SortK", "TxMarshallInput3")
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of  DeleteCond %s", err)
	}

}
