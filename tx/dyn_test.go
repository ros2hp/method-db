package tx

import (
	"context"
	//	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ros2hp/method-db/db"
	dyn "github.com/ros2hp/method-db/dynamodb"
	"github.com/ros2hp/method-db/key"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/tbl"
	"github.com/ros2hp/method-db/uuid"
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

// func TestGetValueSN(t *testing.T) {
// 	upd := "SET #0 = list_append(#0, :0), #1 = list_append(#1, :1), #2 = #2 + :H "
// 	sn := "#2"
// 	//	t.Logf(dyn.GetValueSN(&upd, sn))

// 	i := strings.Index(upd, sn)
// 	ii := strings.Index(upd[i:], ":")
// 	s := i + ii
// 	for k, v := range upd[s:] {
// 		if v == ')' || v == ',' || v == ' ' {
// 			t.Logf("return: %q", upd[s:s+k])
// 			return
// 		}
// 		if k == len(upd[s:])-1 {
// 			t.Logf("return: %q", upd[s:s+k+1])
// 			return
// 		}
// 	}
// 	t.Logf("End: %q", upd[s:])
// }

func TestDynSelect(t *testing.T) {

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
		if k == 2 && v.Name() != "LastName" {
			t.Errorf(`Expected "LastName" attributes got %q`, v.Name())
		}

		if k == 12 && v.Name() != "Loc.Cntry.FirstName" {
			t.Errorf(`Expected "Loc.Cntry.FirstName" attributes got %q`, v.Name())
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
			if i == 12 && *x != "John" {
				t.Errorf(`Expected "John" attributes got %q`, *x)
			}
			t.Logf("%d: String: [%s]", i, *x)
			if i == 17 && *x != "13 March 1967" {
				t.Errorf(`Expected "13 March 1967" attributes got %q`, *x)
			}
		default:
			t.Logf("%d: uint8: %T", i, x)
		}

	}
}

func TestDynSelectSlice(t *testing.T) {

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
		if k == 0 && v.Name() != "Status" {
			t.Errorf(`Expected "Status" attributes got %q`, v.Name())
		}
		if k == 2 && v.Name() != "LastName" {
			t.Errorf(`Expected "LastName" attributes got %q`, v.Name())
		}

		if k == 12 && v.Name() != "Loc.Cntry.FirstName" {
			t.Errorf(`Expected "Loc.Cntry.FirstName" attributes got %q`, v.Name())
		}
		if k == 17 && v.Name() != "Loc.Cntry.Nperson.DOB" {
			t.Errorf(`Expected "Loc.Cntry.Nperson.DOB" attributes got %q`, v.Name())
		}
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

func TestDynMergeMutationUpdate(t *testing.T) {

	pkey := 10001
	sortk := "U#P#v"

	var mutop = mut.Update
	var keys []key.Key
	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 1)
			xf[0] = 4

			cTx.MergeMutation(gtbl, mutop, keys).AddMember("Nd", cuid).AddMember("XF", xf).AddMember("ASZ", 1, mut.ADD)
		}

	}
	//Randomly chooses an overflow block. However before it can choose random it must create a set of overflow blocks
	//which relies upon an Overflow batch limit being reached and a new batch created.

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"PKey", pkey + i}, key.Key{"SortK", sortk}}

		m, err := cTx.GetMergedMutation(gtbl, keys)
		if err != nil {
			t.Errorf("Error in MergeMutation: %s", err)
		}

		for i, v := range m.GetMutateMembers() {
			t.Logf("Attribute:  %d   %s", i, v.Name())
		}

		asz := m.GetMemberValue("ASZ").(int)
		t.Logf("asz: %d", asz)
		xf_ := m.GetMemberValue("XF").([]int64)
		if 4 == asz {
			// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
			xf_[len(xf_)-1] = 99
			cTx.MergeMutation(gtbl, mut.Update, keys).AddMember("XF", xf_, mut.SET)
		}

	}

	keys = []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey}}

	cTx.MergeMutation(gtbl, mutop, keys).AddMember("N", 1, mut.ADD)

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

}

func TestDynMergeMutationBinarySet(t *testing.T) {

	pkey := 20001
	sortk := "U#P#v"

	utx := NewBatch("DynMergeMutation3a")
	for i := 0; i < 10; i++ {
		utx.NewDelete(gtbl).Key("PKey", pkey+i).Key("SortK", sortk)
	}
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of MergeMutation3a %s", err)
	}
	var mutop = mut.Insert
	var keys []key.Key
	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 1)
			xf[0] = int64(k)

			cTx.MergeMutation(gtbl, mutop, keys).AddMember("Nd", cuid, dyn.BinarySet).AddMember("XF", xf).AddMember("ASZ", 1, mut.ADD)
		}

	}
	//Randomly chooses an overflow block. However before it can choose random it must create a set of overflow blocks
	//which relies upon an Overflow batch limit being reached and a new batch created.

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"PKey", pkey + i}, key.Key{"SortK", sortk}}

		m, err := cTx.GetMergedMutation(gtbl, keys)
		if err != nil {
			t.Errorf("Error in MergeMutation: %s", err)
		}

		for i, v := range m.GetMutateMembers() {
			t.Logf("Attribute:  %d   %s", i, v.Name())
		}

		asz := m.GetMemberValue("ASZ").(int)
		xf_ := m.GetMemberValue("XF").([]int64)
		nd_ := m.GetMemberValue("Nd").([][]byte)
		cuid := make([][]byte, 1)
		cuid[0] = nd_[len(nd_)-1]
		if 4 == asz {
			// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
			xf_[len(xf_)-1] = 99
			cTx.MergeMutation(gtbl, mut.Update, keys).AddMember("XF", xf_, mut.ADD).Attribute("Nd", cuid, mut.SUBTRACT).AddMember("ASZ", 1, mut.SUBTRACT)
		}

	}

	keys = []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey}}

	cTx.MergeMutation(gtbl, mutop, keys).AddMember("N", 1, mut.ADD)

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}
	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey+5).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 3 {
		t.Errorf("Expected ASX of 43got %d", qrv.ASZ)
	}
	if len(qrv.Nd) != 3 {
		t.Errorf("Expected Nd of 3 got %d", len(qrv.Nd))
	}
	if len(qrv.XF) != 8 {
		t.Errorf("Expected XF of 8 got %d. Must not be a List", len(qrv.XF))
	}
	if qrv.XF[3] != 99 {
		t.Errorf("Expected XF[0] of 99 got %d", qrv.XF[0])
	}
	if qrv.XF[len(qrv.XF)-1] != 99 {
		t.Errorf("Expected XF[0] of 99 got %d", qrv.XF[0])
	}

	t.Logf("XF: %#v", qrv.XF)

}

func TestDynBinarySetUpdateSubtract(t *testing.T) {

	pkey := 20001
	sortk := "U#P#v"

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}

	for i := 0; i < 10; i++ {
		var qrv qr
		qt := NewQuery("label", gtbl)
		qt.Select(&qrv).Key("PKey", pkey+i).Key("SortK", sortk)
		err = qt.Execute()

		// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
		cTx := NewContext(context.Background(), "AttachEdges")

		cuid := make([][]byte, 1)
		cuid[0] = qrv.Nd[len(qrv.Nd)-1]
		// cTx.NewUpdate(gtbl).Key("PKey", pkey+i).Key("SortK", sortk).Attribute("Nd", cuid, mut.BinarySet, mut.DELETE).AddMember("ASZ", 1, mut.SUBTRACT) // works
		// cTx.NewUpdate(gtbl).Key("PKey", pkey+i).Key("SortK", sortk).Attribute("Nd", cuid, mut.DELETE).AddMember("ASZ", 1, mut.SUBTRACT) // Wrong - need to include mut.BinarySet
		cTx.NewUpdate(gtbl).Key("PKey", pkey+i).Key("SortK", sortk).Attribute("Nd", cuid, mut.BinarySet, mut.SUBTRACT).AddMember("ASZ", 1, mut.SUBTRACT)

		// err = cTx.Execute()
		err = cTx.Commit()
		if err != nil {
			//cTx.Dump() // serialise cTx to mysql dump table
			t.Errorf("Error in MergeMutation execute : %s ", err)
		}
	}

	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey+5).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 2 {
		t.Errorf("Expected ASX of 2 got %d", qrv.ASZ)
	}
	if len(qrv.Nd) != 2 {
		t.Errorf("Expected Nd of 2 got %d", len(qrv.Nd))
	}

}

func TestDynMergeMutation2b(t *testing.T) {

	pkey := 20001
	sortk := "U#P#v"

	var mutop = mut.Update
	var keys []key.Key
	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 1)
			xf[0] = 4

			cTx.MergeMutation(gtbl, mutop, keys).AddMember("Nd", cuid, dyn.BinarySet, mut.ADD).AddMember("XF", xf).AddMember("ASZ", 1, mut.ADD)
		}

	}

	//Randomly chooses an overflow block. However before it can choose random it must create a set of overflow blocks
	//which relies upon an Overflow batch limit being reached and a new batch created.

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"PKey", pkey + i}, key.Key{"SortK", sortk}}

		m, err := cTx.GetMergedMutation(gtbl, keys)
		if err != nil {
			t.Errorf("Error in MergeMutation: %s", err)
		}

		for i, v := range m.GetMutateMembers() {
			t.Logf("Attribute:  %d   %s", i, v.Name())
		}

		// asz := m.GetMemberValue("ASZ").(int)
		// t.Logf("asz: %d", asz)
		// xf_ := m.GetMemberValue("XF").([]int64)
		// if 4 == asz {
		// 	// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
		// 	xf_[len(xf_)-1] = 99
		// 	cTx.MergeMutation(gtbl, mut.Update, keys).AddMember("XF", xf_, mut.SET)
		// }

	}

	keys = []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey}}

	cTx.MergeMutation(gtbl, mutop, keys).AddMember("N", 1, mut.ADD)

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}
}

// MergeMutationListCreated (default for methodb)
func TestDynMergeMutationList(t *testing.T) {

	pkey := 30001
	sortk := "U#P#v"

	utx := NewBatch("DynMergeMutation3a")
	for i := 0; i < 10; i++ {
		utx.NewDelete(gtbl).Key("PKey", pkey+i).Key("SortK", sortk)
	}
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of MergeMutation3a %s", err)
	}
	var mutop = mut.Insert

	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(i)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf).Attribute("ASZ", 1, mut.ADD)
		}

	}

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}
	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 4 {
		t.Errorf("Expected ASX of 4 got %d", qrv.ASZ)
	}
	if len(qrv.Nd) != 4 {
		t.Errorf("Expected Nd of 4 got %d", len(qrv.Nd))
	}
	if len(qrv.XF) != 12 {
		t.Errorf("Expected XF of 12 got %d. Must not be a List", len(qrv.XF))
	}
	if qrv.XF[0] != 0 {
		t.Errorf("Expected XF[0] of 0 got %d", qrv.XF[0])
	}
	if qrv.XF[1] != 1 {
		t.Errorf("Expected XF[1] of 1 got %d", qrv.XF[0])
	}
	if qrv.XF[2] != 2 {
		t.Errorf("Expected XF[2] of 2 got %d", qrv.XF[0])
	}
	if qrv.XF[3] != 0 {
		t.Errorf("Expected XF[3] of 0 got %d", qrv.XF[0])
	}
	if qrv.XF[4] != 1 {
		t.Errorf("Expected XF[4] of 1 got %d", qrv.XF[0])
	}
	if qrv.XF[11] != 2 {
		t.Errorf("Expected XF[11] of 2 got %d", qrv.XF[0])
	}
	t.Logf("XF: %#v", qrv.XF)

}

// MergeMutationNumberSetCreated (dynamodb dedups number sets)
func SetupMergeMutationAllList(t *testing.T, pkey int) {

	sortk := "U#P#v"

	utx := NewBatch("DynMergeMutation3a")
	for i := 0; i < 10; i++ {
		utx.NewDelete(gtbl).Key("PKey", pkey+i).Key("SortK", sortk)
	}
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of MergeMutation3a %s", err)
	}
	var mutop = mut.Insert

	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf).Attribute("ASZ", 1, mut.ADD)
		}

	}

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

}

// MergeMutationNumberSetCreated (dynamodb dedups number sets)
func TestDynMergeMutationNumberSet(t *testing.T) {

	pkey := 30001
	sortk := "U#P#v"

	utx := NewBatch("DynMergeMutation3a")
	for i := 0; i < 10; i++ {
		utx.NewDelete(gtbl).Key("PKey", pkey+i).Key("SortK", sortk)
	}
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of MergeMutation3a %s", err)
	}
	var mutop = mut.Insert

	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf, dyn.NumberSet).Attribute("ASZ", 1, mut.ADD)
		}

	}

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}
	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 4 {
		t.Errorf("Expected ASX of 4 got %d", qrv.ASZ)
	}
	if len(qrv.Nd) != 4 {
		t.Errorf("Expected Nd of 4 got %d", len(qrv.Nd))
	}
	if len(qrv.XF) != 4 {
		t.Errorf("Expected XF of 4 got %d. Must not be a NumberSet", len(qrv.XF))
	}
	t.Logf("XF: %#v", qrv.XF)

}

// MergeMutationNSADD
func TestDynMergeMutationNSAdd(t *testing.T) {

	pkey := 30001
	sortk := "U#P#v"

	utx := NewBatch("DynMergeMutation3a")
	for i := 0; i < 10; i++ {
		utx.NewDelete(gtbl).Key("PKey", pkey+i).Key("SortK", sortk)
	}
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of MergeMutation3a %s", err)
	}
	var mutop = mut.Insert
	var keys []key.Key
	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf, dyn.NumberSet, mut.ADD).Attribute("ASZ", 1, mut.ADD)
		}

	}
	//Randomly chooses an overflow block. However before it can choose random it must create a set of overflow blocks
	//which relies upon an Overflow batch limit being reached and a new batch created.

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"PKey", pkey + i}, key.Key{"SortK", sortk}}

		m, err := cTx.GetMergedMutation(gtbl, keys)
		if err != nil {
			t.Errorf("Error in MergeMutation: %s", err)
		}

		for i, v := range m.GetMutateMembers() {
			t.Logf("Attribute:  %d   %s", i, v.Name())
		}

		asz := m.GetMemberValue("ASZ").(int)
		_ = m.GetMemberValue("XF").([]int64)
		if 4 == asz {
			// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
			xf := make([]int64, 1)
			xf[0] = 99
			cTx.MergeMutation(gtbl, mut.Update, keys).Attribute("XF", xf, mut.ADD)
		}

	}

	keys = []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey}}

	cTx.MergeMutation(gtbl, mutop, keys).Attribute("N", 1, mut.ADD)

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}
	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 4 {
		t.Errorf("Expected ASX of 4 got %d", qrv.ASZ)
	}
	if qrv.N != 1 {
		t.Errorf("Expected N of 1 got %d", qrv.N)
	}
	if len(qrv.Nd) != 4 {
		t.Errorf("Expected Nd of 4 got %d", len(qrv.Nd))
	}
	if len(qrv.XF) != 5 {
		t.Errorf("Expected XF of 5 got %d", len(qrv.XF))
	}
	expected := []int64{0, 1, 2, 3, 99}
	for _, v := range qrv.XF {
		var found bool
		for _, vv := range expected {
			if v == vv {
				found = true
			}
		}
		if !found {
			t.Errorf("Expected XF to be one of %#v", expected)
		}

	}
	t.Logf("XF: %#v", qrv.XF)

}

// MergeMutationNSDefaultOper
func TestDynMergeMutationNSDefaultOper(t *testing.T) {

	pkey := 30001
	sortk := "U#P#v"

	utx := NewBatch("DynMergeMutation3a")
	for i := 0; i < 10; i++ {
		utx.NewDelete(gtbl).Key("PKey", pkey+i).Key("SortK", sortk)
	}
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of MergeMutation3a %s", err)
	}
	var mutop = mut.Insert
	var keys []key.Key
	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf, dyn.NumberSet).Attribute("ASZ", 1, mut.ADD)
		}

	}
	//Randomly chooses an overflow block. However before it can choose random it must create a set of overflow blocks
	//which relies upon an Overflow batch limit being reached and a new batch created.

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"PKey", pkey + i}, key.Key{"SortK", sortk}}

		m, err := cTx.GetMergedMutation(gtbl, keys)
		if err != nil {
			t.Errorf("Error in MergeMutation: %s", err)
		}

		for i, v := range m.GetMutateMembers() {
			t.Logf("Attribute:  %d   %s", i, v.Name())
		}

		asz := m.GetMemberValue("ASZ").(int)
		_ = m.GetMemberValue("XF").([]int64)
		if 4 == asz {
			// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
			xf := make([]int64, 1)
			xf[0] = 99
			cTx.MergeMutation(gtbl, mut.Update, keys).Attribute("XF", xf) // default : mut.ADD
		}

	}

	keys = []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey}}

	cTx.MergeMutation(gtbl, mutop, keys).Attribute("N", 1, mut.ADD)

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}
	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 4 {
		t.Errorf("Expected ASX of 4 got %d", qrv.ASZ)
	}
	if qrv.N != 1 {
		t.Errorf("Expected N of 1 got %d", qrv.N)
	}
	if len(qrv.Nd) != 4 {
		t.Errorf("Expected Nd of 4 got %d", len(qrv.Nd))
	}
	if len(qrv.XF) != 5 {
		t.Errorf("Expected XF of 5 got %d", len(qrv.XF))
	}
	expected := []int64{0, 1, 2, 3, 99}
	for _, v := range qrv.XF {
		var found bool
		for _, vv := range expected {
			if v == vv {
				found = true
			}
		}
		if !found {
			t.Errorf("Expected XF to be one of %#v", expected)
		}

	}
	t.Logf("XF: %#v", qrv.XF)

}

// MergeMutationNSSet
func TestDynMergeMutationSubtract(t *testing.T) {

	pkey := 30001
	sortk := "U#P#v"

	utx := NewBatch("DynMergeMutation3a")
	for i := 0; i < 10; i++ {
		utx.NewDelete(gtbl).Key("PKey", pkey+i).Key("SortK", sortk)
	}
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of MergeMutation3a %s", err)
	}
	var mutop = mut.Insert
	var keys []key.Key
	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}
		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf, dyn.NumberSet).Attribute("ASZ", 1, mut.ADD)
		}
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}
		for k := 0; k < 2; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 6)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k)
				xf[i+3] = int64(k + 100)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf, mut.SUBTRACT).Attribute("ASZ", 1, mut.SUBTRACT)
		}
	}

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"PKey", pkey + i}, key.Key{"SortK", sortk}}

		m, err := cTx.GetMergedMutation(gtbl, keys)
		if err != nil {
			t.Errorf("Error in MergeMutation: %s", err)
		}

		asz := m.GetMemberValue("ASZ").(int)
		_ = m.GetMemberValue("XF").([]int64)
		if 2 == asz {
			// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
			xf := make([]int64, 1)
			xf[0] = 99
			cTx.MergeMutation(gtbl, mut.Update, keys).Attribute("XF", xf) // mut.ADD by default
		}
	}

	keys = []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey}}

	cTx.MergeMutation(gtbl, mutop, keys).Attribute("N", 1, mut.ADD)

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}
	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 2 {
		t.Errorf("Expected ASX of 2 got %d", qrv.ASZ)
	}
	if qrv.N != 1 {
		t.Errorf("Expected N of 1 got %d", qrv.N)
	}
	if len(qrv.Nd) != 6 {
		t.Errorf("Expected Nd of 6 got %d", len(qrv.Nd))
	}
	if len(qrv.XF) != 3 {
		t.Errorf("Expected XF of 3 got %d", len(qrv.XF)) // {"2", "3", "99"}
	}
	expected := []int64{2, 3, 99}
	for _, v := range qrv.XF {
		var found bool
		for _, vv := range expected {
			if v == vv {
				found = true
			}
		}
		if !found {
			t.Errorf("Expected XF to be one of %#v", expected)
		}

	}
	t.Logf("XF: %#v", qrv.XF)

}

// first instantiation of an Attribute modifier determines database (dynamodb) Operation as it executes mut.AddMember()
// subsequent Attribute modifiers applies to local in memory List/Set.
func TestDynMergeMutationUpdatePrepend(t *testing.T) {

	pkey := 30100
	sortk := "U#P#v"

	SetupMergeMutationAllList(t, pkey)

	var mutop = mut.Update
	var keys []key.Key
	ctx := context.Background()
	// as mutations are configured to use Insert (Put) - batch can be used. Issue - no transaction control. Cannot be used with merge stmt
	cTx := NewContext(ctx, "AttachEdges")
	if errs := cTx.GetErrors(); len(errs) > 0 {
		t.Errorf("Error in BatchMerge: %s", errs[0])
	}
	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}
		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k)
			}
			// first instantiation of an Attribute modifier determines dynamodb Operation as it executes mut.AddMember()
			// subsequent Attribute modifiers applies to local in memory List/Set.
			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf, mut.PREPEND).Attribute("ASZ", 1, mut.ADD)
		}
	}

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey + i}}

		for k := 0; k < 4; k++ {

			cuid := make([][]byte, 1)
			cuid[0], _ = uuid.MakeUID()
			xf := make([]int64, 3)
			for i := 0; i < 3; i++ {
				xf[i] = int64(k + 100)
			}

			cTx.MergeMutation(gtbl, mutop, keys).Attribute("Nd", cuid).Attribute("XF", xf).Attribute("ASZ", 1, mut.ADD)
		}

	}

	//Randomly chooses an overflow block. However before it can choose random it must create a set of overflow blocks
	//which relies upon an Overflow batch limit being reached and a new batch created.

	for i := 0; i < 10; i++ {

		keys := []key.Key{key.Key{"PKey", pkey + i}, key.Key{"SortK", sortk}}

		m, err := cTx.GetMergedMutation(gtbl, keys)
		if err != nil {
			t.Errorf("Error in MergeMutation: %s", err)
		}

		for i, v := range m.GetMutateMembers() {
			t.Logf("Attribute:  %d   %s", i, v.Name())
		}

		asz := m.GetMemberValue("ASZ").(int)
		_ = m.GetMemberValue("XF").([]int64)
		if 4 == asz {
			// update XF in UID-Pred (in parent node block) to overflow batch limit reached.
			xf := []int64{0, 2}
			cTx.MergeMutation(gtbl, mut.Update, keys).Attribute("XF", xf, mut.SUBTRACT)
		}

	}

	keys = []key.Key{key.Key{"SortK", sortk}, key.Key{"PKey", pkey}}

	cTx.MergeMutation(gtbl, mutop, keys).Attribute("N", 1, mut.ADD)

	// err = cTx.Execute()
	err = cTx.Commit()
	if err != nil {
		//cTx.Dump() // serialise cTx to mysql dump table
		t.Errorf("Error in MergeMutation execute : %s ", err)
	}

	type qr struct {
		PKey  int
		SortK string
		ASZ   int
		N     int
		Nd    [][]byte
		XF    []int64
	}
	var qrv qr

	qt := NewQuery("label", gtbl)
	qt.Select(&qrv).Key("PKey", pkey).Key("SortK", sortk)
	err = qt.Execute()
	if err != nil {
		t.Errorf("Error inTestDynMergeMutation3a Query : %s ", err)
	}
	if qrv.ASZ != 12 {
		t.Errorf("Expected ASX of 2 got %d", qrv.ASZ)
	}
	if qrv.N != 1 {
		t.Errorf("Expected N of 1 got %d", qrv.N)
	}
	if len(qrv.Nd) != 12 {
		t.Errorf("Expected Nd of 12 got %d", len(qrv.Nd))
	}
	if len(qrv.XF) != 36 {
		t.Errorf("Expected XF of 36 got %d", len(qrv.XF)) // {"2", "3", "99"}
	}
	if qrv.XF[0] != 3 {
		t.Errorf("Expected XF[0] of 3 got %d", qrv.XF[0]) // {"2", "3", "99"}
	}
	if qrv.XF[12] != 100 {
		t.Errorf("Expected XF[12] of 100 got %d", qrv.XF[12]) // {"2", "3", "99"}
	}
	if qrv.XF[16] != 101 {
		t.Errorf("Expected XF[16] of 101 got %d", qrv.XF[16]) // {"2", "3", "99"}
	}
	if qrv.XF[23] != 103 {
		t.Errorf("Expected XF[23] of 103 got %d", qrv.XF[23]) // {"2", "3", "99"}
	}
	if qrv.XF[24] != 0 {
		t.Errorf("Expected XF[24] of 0 got %d", qrv.XF[24]) // {"2", "3", "99"}
	}

	t.Logf("XF: %#v", qrv.XF)

}

func TestDynSelectSlicePtr(t *testing.T) {

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
		if k == 0 && v.Name() != "Status" {
			t.Errorf(`Expected "Status" attributes got %q`, v.Name())
		}
		if k == 2 && v.Name() != "LastName" {
			t.Errorf(`Expected "LastName" attributes got %q`, v.Name())
		}

		if k == 12 && v.Name() != "Loc.Cntry.FirstName" {
			t.Errorf(`Expected "Loc.Cntry.FirstName" attributes got %q`, v.Name())
		}
		if k == 17 && v.Name() != "Loc.Cntry.Nperson.DOB" {
			t.Errorf(`Expected "Loc.Cntry.Nperson.DOB" attributes got %q`, v.Name())
		}
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
	//dyn_test.go:184: 0: status: [0], State: abcdefg0
	//  dyn_test.go:184: 1: status: [234], State: abcdefg1
	//  dyn_test.go:184: 2: status: [468], State: abcdefg2
	//  dyn_test.go:184: 3: status: [702], State: abcdefg3
	for i, v := range x {
		switch i {
		case 0:
			if v.Loc.Cntry.Population != 0 {
				t.Errorf("SelectSlicePtr: expected 0 got %d", v.Loc.Cntry.Population)
			}
		case 1:
			if v.Loc.Cntry.Population != 234 {
				t.Errorf("SelectSlicePtr: expected 234 got %d", v.Loc.Cntry.Population)
			}
		case 2:
			if v.Loc.Cntry.Population != 468 {
				t.Errorf("SelectSlicePtr: expected 468 got %d", v.Loc.Cntry.Population)
			}
		case 3:
			if v.Loc.Cntry.Population != 702 {
				t.Errorf("SelectSlicePtr: expected 702 got %d", v.Loc.Cntry.Population)
			}
		}
	}

}

type tyNames struct {
	ShortNm string `dynamodbav:"SortK"`
	LongNm  string `dynamodbav:"Name"`
}

func TestDynQueryTypesPtrSlice(t *testing.T) {

	type graphMeta struct {
		SortK string
	}
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

func TestDynQueryTypesStruct(t *testing.T) {

	type graphMeta struct {
		SortK string
	}

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

func TestDynQueryPop1(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

	var sk City

	txg := NewQuery("pop", gtbl)
	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Errorf("TestQueryPop1: Error: %s", err)
	}

	if !(sk.Pop > 0) {
		t.Errorf("TestQueryPop1: Expected > 0 got: %d", sk.Pop)
	}
}

func TestDynQueryStructTag(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"` // mdb tag not supported. Need an UnmarshalMap() to work with mdb. Currently using DYnamodb's UnmarshalMap()
	}

	var sk City

	txg := NewQuery("pop", gtbl)
	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Errorf("TestQueryPop2: Error: %s", err)
	}

	t.Logf("sk %#v", sk)
	if !(sk.Pop > 0) {
		t.Errorf("TestQueryPop2: Expected > 0 got: %d", sk.Pop)
	}
	t.Logf("TestQueryPop2: Pop: %d", sk.Pop)

}

// TestDynQueryStructTagmdb test using mdb tags in query struct. Not yet implemented. THINK ABOUT IT....
// func TestDynQueryStructTagmdb(t *testing.T) {

// 	type City struct {
// 		PKey  int    `mdb:"-,KEY"`
// 		SortK string `mdb:"-,KEY"`
// 		Pop   int    `dynamodbav:"Population" ` // mdb tag not supported. Need an UnmarshalMap() to work with mdb. Currently using DYnamodb's UnmarshalMap()
// 	}

// 	sk := City{PKey: 1001, SortK: "Melbourne"}

// 	txg := NewQuery("pop", gtbl)
// 	txg.Select(&sk) //.Key("PKey", 1001).Key("SortK", "Melbourne")
// 	err := txg.Execute()

// 	if err != nil {
// 		t.Errorf("TestQueryPop2: Error: %s", err)
// 	}

// 	t.Logf("sk %#v", sk)
// 	if !(sk.Pop > 0) {
// 		t.Errorf("TestQueryPop2: Expected > 0 got: %d", sk.Pop)
// 	}
// 	t.Logf("TestQueryPop2: Pop: %d", sk.Pop)

// }

func TestDynQueryKey(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

	var sk City

	txg := NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey", 1001).Key("SortK", "Sydney")
	err := txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}
	if sk.Pop == 0 {
		t.Errorf("expected >0 got 0")
	}
	t.Logf("query: %#v", sk)

	txg = NewQuery("pop", gtbl)

	txg.Select(&sk).Key("PKey2", 1001).Key("SortK", "Sydney")
	err = txg.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), `is not a partition key for table`) {
			t.Errorf(`QueryKey expected error "is not a partition key for tabl" got %q`, err)
		}

	}

}

func TestDynQueryPopUpdateSAddv(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Attribute("Population", sk.Pop+1, mut.SET)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Add("Population", 10000)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Attribute("Population", sk.Pop) // default modifier SET
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Increment("Population")

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

func TestDynQueryPopUpdateKey(t *testing.T) {

	var (
		pk int    = 1001
		sk string = "Sydney"
	)

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

	var skpop City

	txg := NewQuery("pop", gtbl)

	txg.Select(&skpop).Key("PKey", pk).Key("SortK", sk)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", skpop.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	utx.NewUpdate(gtbl).Key("PKey", pk, "GT").Key("SortK", sk).Set("Population", 1)

	err = utx.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), `For UpdateItem(), equality operator for all keys must be "EQ"`) {
			t.Errorf("Update error: %s", err.Error())
		}
	}

	utx = New("IXFlag2")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Increment("Population")

	err = utx.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), `For UpdateItem(), equality operator for all keys must be "EQ"`) {
			t.Errorf("Update error: %s", err.Error())
		}
	}

	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", pk).Key("SortK", sk)
	err = txg.Execute()

	if err != nil {
		t.Errorf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v %#v\n", sk2.Pop, skpop.Pop)
	if !(sk2.Pop == skpop.Pop+1) {
		t.Errorf(`Expected %d got %d`, skpop.Pop, sk2.Pop)
	}

}
func TestDynUpdateDefaultSetModifier(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Attribute("Population", sk.Pop+1) //Set("Population", sk.Pop+1) //Attribute("Population", sk.Pop+1, mut.SET)
	//utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Attribute("Population", sk.Pop+1)
	//utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Attribute("Population", sk.Pop+1, mut.SET)
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

func TestDynUpdateSet(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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

func TestDynQueryPopUpdateSetKeyWrong(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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

func TestDynQueryPopUpdateError(t *testing.T) {
	var err error

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
	utx.NewUpdate(gtbl).Key("PKey2", 1001).Key("SortK", "Sydney").Attribute("Population", sk.Pop+1)

	err = utx.Execute()
	if err != nil {
		if strings.Contains(err.Error(), ` "PKey2" is not a key in table`) {
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

func TestDynQueryPopUpdateSAdd(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Increment("Population")
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Add("Population", 1)
	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Attribute("Population", 1, mut.ADD)
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Set("Population", 150030)

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

	if sk2.Pop != sk.Pop+3 {
		t.Fail()
	}

}

func TestDynQueryPopUpdateWhere22(t *testing.T) {

	type City struct {
		SortK string
		Pop   int `dynamodbav:"Population"`
	}

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

func TestDynQueryPopUpdateWhere23(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
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

func TestDynQueryPopUpdateWhere23a(t *testing.T) {

	pk := 1001
	sk := "Sydney"
	type City struct {
		Pop int `dynamodbav:"Population"`
	}

	var skq City

	txg := NewQuery("pop", gtbl)

	txg.Select(&skq).Key("PKey", pk).Key("SortK", sk)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", skq.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", sk).Where(`attribute_exists("MaxPopulation") and Population<MaxPopulation`).Subtract("Population", 1)
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Where(`attribute_not_exists(MaxPopulation) `).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), "ConditionalCheckFailedException") {
			t.Errorf("Update error: %s", err.Error())
		}
	}

	utx = New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", sk).Where(`attribute_exists("MaxPopulation") and Population<MaxPopulation`).Subtract("Population", 1)
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Where(`attribute_exists(MaxPopulation) `).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Update error: %s", err.Error())
	}

	var sk2 City
	txg = NewQuery("pop", gtbl)
	txg.Select(&sk2).Key("PKey", pk).Key("SortK", sk)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", sk2.Pop)

	if !(sk2.Pop == skq.Pop-1) {
		t.Fail()
	}

}

// func TestDynShortNames(t *testing.T) {
// 	var (
// 		past []byte
// 	)
// 	past = append(past, 'a'-1)
// 	// aa, ab...az, ba,..bz, ca, .. cz, da,..dz, ea
// 	for i := 0; i < 1800; i++ {

// 		for i := len(past) - 1; i >= 0; i-- {
// 			past[i]++
// 			if past[i] == 'z'+1 {
// 				if i  && past[0] == 'z'+1 {
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

func TestDynQueryPopUpdateWhere24(t *testing.T) {

	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
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

func TestDynQueryPopUpdateWhere25(t *testing.T) {

	var (
		plimit = 20000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
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

func TestDynQueryPopUpdateWhere26(t *testing.T) {

	var (
		pk     int    = 1000
		sk     string = "Sydney"
		plimit        = 20000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}

	var q City

	txg := NewQuery("pop", gtbl)

	txg.Select(&q).Key("PKey", pk).Key("SortK", sk)
	err = txg.Execute()

	if err != nil {
		t.Logf("Error: %s", err)
	}

	t.Logf("Query Population Sydney  %#v\n", q.Pop)

	utx := New("IXFlag")
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Where(`attribute_exists(MaxPopulation) and (Population>? or Population<?)`).Values(plimit).Subtract("Population", 1)
	err = utx.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), "expected 2 bind variables in Values, got 1") {
			t.Errorf("Update error: %s", err.Error())
		}
	}

}

func TestDynQueryPopUpdateWhere28(t *testing.T) {

	var (
		plimit  = 20000000
		plimit2 = 100000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
		if strings.Contains(err.Error(), "ConditionalCheckFailedException") {
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

func TestDynQueryPopUpdateWhere29(t *testing.T) {

	var (
		plimit  = 2000000
		plimit2 = 200000
	)
	type City struct {
		Pop int `dynamodbav:"Population"`
	}

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
		t.Errorf("Update QueryPopUpdateWhere29 error: %s", err.Error())
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

func TestDynUpdateList(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA")
	utx.NewUpdate(gtbl).Key("PKey", 2500).Key("SortK", "AAA").Attribute("ListT", listinput)
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The provided expression refers to an attribute that does not exist in the item") {

			t.Logf("Error: in Execute of PreTestUpdateList %s", err)
			t.Fatal()
		}
	}
}

func TestDynMutBatchInsert(t *testing.T) {

	utx := NewBatch("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	m := mut.NewInsert(gtbl).AddMember("PKey", 2501).AddMember("SortK", "DDD")
	utx.Add(m)
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The provided expression refers to an attribute that does not exist in the item") {

			t.Errorf("Error: in Execute of PreTestUpdateList %s", err)
		}
		t.Logf("Error: %s", err)
	}

}

func TestDynMutBatchInsertNoKey(t *testing.T) {

	utx := NewBatch("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	m := mut.NewInsert(gtbl).AddMember("PKey2", 2501)
	utx.Add(m)
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The provided key element does not match the schema") {

			t.Errorf("Error: in Execute of PreTestUpdateList %s", err)
		}
		//	t.Logf("Error: %s", err)
	}

}

func TestDynMutStdInsert(t *testing.T) {

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	m := mut.NewInsert(gtbl).AddMember("PKey", 2501).AddMember("SortK", "AAA")
	utx.Add(m)
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The provided expression refers to an attribute that does not exist in the item") {

			t.Errorf("Error: in Execute of PreTestUpdateList %s", err)
		}
		t.Logf("Error: %s", err)
	}

}

// insert does not fetch table description of keys. So Key modifier in AddMember has nothing to compare to so is ignored.
func TestDynMutStdInsertKey(t *testing.T) {

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	m := mut.NewInsert(gtbl).AddMember("PKey", 2501, mut.KEY).AddMember("SortK", "BBB")
	utx.Add(m)
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The provided expression refers to an attribute that does not exist in the item") {

			t.Errorf("Error: in Execute of PreTestUpdateList %s", err)
		}
		t.Logf("Error: %s", err)
	}

}

// Rather than get Method-db to check that all table keys are added when using mutation.Insert, leave it to database to detect.
func TestDynMutStdInsertNoKey(t *testing.T) {

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	m := mut.NewInsert(gtbl).AddMember("PKey2", 2501, mut.KEY).AddMember("SortK2", "BBB")
	utx.Add(m)
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "Missing the key PKey in the item") {

			t.Errorf("Error: in Execute of PreTestUpdateList %s", err)
		}
		t.Logf("Error: %s", err)
	}

}

func TestDynTxStdInsertNoKey(t *testing.T) {

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).AddMember("PKey2", 2501, mut.KEY).AddMember("SortK2", "BBB")
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "Missing the key PKey in the item") {

			t.Errorf("Error: in Execute of PreTestUpdateList %s", err)
		}
		t.Logf("Error: %s", err)
	}

}

func TestDynPutList(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	type output struct {
		ListT []int64
	}

	var out_ output

	utx := New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA").Attribute("ListT", listinput).Add("cond", 5)
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

func TestDynPutUpdateList(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	type output struct {
		ListT []int64
	}

	var out_ output

	utx := New("PostTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 2500).Key("SortK", "AAA") //.Where(`attribute_exists(MaxPopulation)`)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of Post PutUpdateList %s", err)
	}

	utx = New("PreTestUpdList") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA").Attribute("ListT", listinput).Add("cond", 5)
	utx.NewUpdate(gtbl).Key("PKey", 2500).Key("SortK", "AAA").Attribute("ListT", listinput)
	err = utx.Execute()

	if err != nil {
		t.Errorf("Error: in Execute of Pre PutUpdateList %s", err)
	}

	q := NewQuery("qlablel", gtbl)
	q.Select(&out_).Key("PKey", 2500).Key("SortK", "AAA")
	err := q.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of qlabel %s", err)
	}

	if len(out_.ListT) != 20 {
		t.Errorf("Expected len of 20 got %d", len(out_.ListT))
	}

}

func TestDynDeleteCond(t *testing.T) {

	listinput := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	utx := New("DynDeleteCond") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 2500).Key("SortK", "AAA")
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error DynDeleteCond: %s", err)
	}
	utx = New("DynDeleteCond") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Key("PKey", 2500).Key("SortK", "AAA").Attribute("ListT", listinput)
	err = utx.Execute()

	if err != nil {
		t.Errorf("Error DynDeleteCond: in Execute of Pre DeleteCond %s", err)
	}

	utx = New("DynDeleteCond") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", 2500).Key("SortK", "AAA").Where(`attribute_exists(MaxPopulation)`)
	err = utx.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), "ConditionalCheckFailedException") {
			t.Errorf("Error DynDeleteCond: in Execute of PreTestUpdateList %s", err)
		}
	}
	t.Log(err.Error())

}

func TestDynMarshalStructx(t *testing.T) {

	var pk = 3099
	var sk = "MarshalStruct"

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		ByteSlice [][]byte // dynamodb.MarshalMap will generate binary set for a binrary slice. Cannot be overriden from here as there is no dynamodbav for LIST. Use Attribute() to override.
		MyField   string
		Letters   []string // dynamodb.MarshalMap will generate string list from a string slice.
		Numbers   []int    // dynamodb.MarshalMap will generate number list from a int slice.
	}

	r := Record{
		PKey:      pk,
		SortK:     sk,
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}

	var or Record

	utx := New("DynMarshalStruct") // std api
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = New("DynMarshalStruct") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Submit(&r)
	err = utx.Execute()

	if err != nil {
		t.Errorf("DynMarshalStruct Error: in Execute of Pre DeleteCond %s", err)
	}

	q := NewQuery("qlablel", gtbl)
	q.Select(&or).Key("PKey", pk).Key("SortK", sk)
	err := q.Execute()
	if err != nil {
		t.Errorf("DynMarshalStruct Error: in Execute of qlabel %s", err)
	}

	if len(or.Bytes) != 2 {
		t.Errorf("DynMarshalStruct Error: Query expected len of 2 got %d", len(or.Bytes))
	}

	if len(or.ByteSlice) != 3 {
		t.Errorf("DynMarshalStruct Error: Query expected len of 3 got %d", len(or.ByteSlice))
	}

	if len(or.Letters) != 4 {
		t.Errorf("DynMarshalStruct Error: Query expected len of 4 got %d", len(or.ByteSlice))
	}

}

func TestDynMarshalStruct2(t *testing.T) {

	var pk = 3029
	var sk = "MarshalStruct2"
	var sk2 = "MarshalStruct2a"

	type Base struct {
		Bytes     []byte
		ByteSlice [][]byte // dynamodb.MarshalMap will generate binary set for a binrary slice. Cannot be overriden from here as there is no dynamodbav for LIST
		MyField   string
		Letters   []string // dynamodb.MarshalMap will generate string list from a string slice.
		Numbers   []int    // dynamodb.MarshalMap will generate number list from a int slice.
	}

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int `dynamodbav:",numberset"`
		Another   Base
	}

	r := Record{
		PKey:      pk,
		SortK:     sk,
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
		PKey:      pk,
		SortK:     sk2,
		Bytes:     []byte{48, 49, 51},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e", "f", "g", "h"},
		Numbers:   []int{1, 2, 3},
	}
	r2.Another = Base{
		Bytes:     []byte{48, 49, 52, 48},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e2", "f2", "g2", "h2"},
		Numbers:   []int{1, 2, 3},
	}

	utx := New("DynMarshalStruct") // std api
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk2)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = NewTx("PreTestUpdList") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_not_exists(PKey)")

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_exists(PKey)")
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), ` Transaction cancelled`) {
			t.Errorf(`MarshalStruct2 expected Error " Transaction cancelled" got %q`, err)
		}
	}

	utx = NewTx("MarshalStruct2") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_not_exists(PKey)")

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_not_exists(PKey)")
	err = utx.Execute()

	if err != nil {
		t.Errorf("MarshalStruct2 Unexpected Error: in Execute of Pre MarshalInput2 %s", err)
	}

	var or []Record
	q := NewQuery("qlablel", gtbl)
	q.Select(&or).Key("PKey", pk)
	err := q.Execute()
	if err != nil {
		t.Errorf("MarshalStruct2 Error: in Execute of qlabel %s", err)
	}

	if len(or) != 2 {
		t.Errorf("MarshalStruct2 Error: query expected 2 items got %d", len(or))
	}
	if len(or[0].Bytes) != 2 {
		t.Errorf("MarshalStruct2 Error: Query expected len of 2 got %d", len(or[0].Bytes))
	}

	if len(or[1].Bytes) != 3 {
		t.Errorf("MarshalStruct2 Error: Query expected len of 3 got %d", len(or[1].Bytes))
	}

}

func TestDynMarshalStruct3a1(t *testing.T) {

	var pk = 3099
	var sk = "MarshalStruct3"
	var sk2 = "MarshalStruct3a"

	type Base struct {
		Bytes     []byte
		ByteSlice [][]byte `dynamodbav:"renamedbyteslice"`
		MyField   string
		Letters   []string
		Numbers   []int
	}

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		Another   Base
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int
	}

	r := Record{
		PKey:      pk,
		SortK:     sk,
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
		PKey:      pk,
		SortK:     sk2,
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

	utx := New("TxMarshallInput3") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk2)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = NewTx("MarshalStruct3") // std api

	utx.NewInsert(gtbl).Submit(&r)

	utx.NewInsert(gtbl).Submit(&r2)

	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of TxPreTestByteSliceAsList got %q`, err)
	}

	utx = New("attribute_exists") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_exists(PKey)")

	err = utx.Execute()

	if err != nil {
		t.Errorf("TxMarshallInput3 attribute_exists error: %s", err)
	}
	utx = New("attribute_not_exists") // std api

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_not_exists(PKey)")

	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The conditional request failed") {
			t.Errorf(`expected error "The conditional request failed" got %q`, err.Error())
		}
	}

	utx = NewTx("TxByteSliceAsList") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_exists(PKey)")

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_not_exists(PKey)")

	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), `ConditionalCheckFailed`) {
			t.Errorf(`TxByteSliceAsList error: expected  ConditionalCheckFailed got %q`, err)
		}
	}

}
func TestDynMarshalStruct3a2(t *testing.T) {

	var pk = 3098
	var sk = "MarshalStruct3a21"
	var sk2 = "MarshalStruct3a22"

	type Base2 struct {
		Bytes     []byte
		ByteSlice [][]byte `dynamodbav:"renamedbyteslice,omitempty"`
		MyField   string
		Letters   []string
		Numbers   []int
	}

	type Base struct {
		Bytes     []byte
		ByteSlice [][]byte `dynamodbav:"renamedbyteslice"`
		MyField   string
		Letters   []string
		Numbers   []int
		XB        Base2
	}

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		Another   Base
		ByteSlice [][]byte
		MyField   string
		Letters   []string
		Numbers   []int
	}

	r := Record{
		PKey:      pk,
		SortK:     sk,
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}
	r.Another = Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}

	r.Another.XB = Base2{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{}, []byte{42, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}

	r2 := Record{
		PKey:      pk,
		SortK:     sk2,
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
	//r2.Another.XB = Base2{}
	r2.Another.XB = Base2{
		Bytes: []byte{48, 49},
		//	ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField: "MyFieldValue",
		Letters: []string{"a", "b", "c", "d"},
		Numbers: []int{1, 2, 3},
	}

	utx := New("TxMarshallInput3") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk2)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = NewTx("MarshalStruct3") // std api

	utx.NewInsert(gtbl).Submit(&r)

	utx.NewInsert(gtbl).Submit(&r2)

	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute  %q`, err)
	}

}

func TestDynMarshalStruct3b(t *testing.T) {

	var pk = 3100
	var sk = "MarshalStruct3a"
	var sk2 = "MarshalStruct3b"

	type Base struct {
		Bytes     []byte
		ByteSlice [][]byte `dynamodbav:"renamedbyteslice"`
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
		Another   []Base
		//	Another2  []Base
	}

	r := Record{
		PKey:      pk,
		SortK:     sk,
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}
	r.Another = []Base{Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}}

	r2 := Record{
		PKey:      pk,
		SortK:     sk2,
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e", "f", "g", "h"},
		Numbers:   []int{1, 2, 3},
	}
	r2base1 := Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e2", "f2 abc _ 56", "g2", "h2"},
		Numbers:   []int{1, 2, 3},
	}
	r2base2 := Base{
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"e2", "f2", "g2", "h2"},
		Numbers:   []int{1, 2, 3},
	}
	r2.Another = []Base{r2base1, r2base2}

	utx := New("TxMarshallInput3") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk2)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = NewTx("MarshalStruct3") // std api

	utx.NewInsert(gtbl).Submit(&r)

	utx.NewInsert(gtbl).Submit(&r2)

	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of TxPreTestByteSliceAsList got %q`, err)
	}

	var out Record
	utq := NewQuery("TxMarshallInput3", gtbl) // std api
	utq.Select(&out).Key("PKey", pk).Key("SortK", sk2)
	err = utq.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if len(out.ByteSlice) != 3 {
		t.Errorf("Expected out.ByteSlice of 3 got %d", len(out.ByteSlice))
	}

	if len(out.Another) != 2 {
		t.Errorf("Expected len(out.Another)  of 2 got %d", len(out.Another))
	}

	if len(out.Another[0].Letters) != 4 {
		t.Errorf("Expected len(out.Another[0].Letters)  of 4 got %d", len(out.Another[0].Letters))
	}

	t.Logf("out: %#v", out)

	var out2 []Record
	utq = NewQuery("TxMarshallInput3", gtbl) // std api
	//v := "f"
	utq.Select(&out2).Key("PKey", pk).Where(`Letters[1] = "f" and Letters[0] = "e"`) //Where(`Letters[1] = ?`).Values(v)
	err = utq.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	t.Logf("out2(where): %#v", out2)

	var out3 []Record
	utq = NewQuery("TxMarshallInput3", gtbl) // std api
	//v := "f"
	utq.Select(&out3).Key("PKey", pk).Where(`Another[0].Letters[1] = "f2 abc _ 56 `) //Where(`Letters[1] = ?`).Values(v)
	err = utq.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	t.Logf("out3(where): %#v", out3)

	utx = New("attribute_exists") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_exists(PKey)")

	err = utx.Execute()

	if err != nil {
		t.Errorf("TxMarshallInput3 attribute_exists error: %s", err)
	}
	utx = New("attribute_not_exists") // std api

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_not_exists(PKey)")

	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), "The conditional request failed") {
			t.Errorf(`expected error "The conditional request failed" got %q`, err.Error())
		}
	}

	utx = NewTx("TxByteSliceAsList") // std api

	utx.NewInsert(gtbl).Submit(&r).Where("attribute_exists(PKey)")

	utx.NewInsert(gtbl).Submit(&r2).Where("attribute_not_exists(PKey)")

	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), `ConditionalCheckFailed`) {
			t.Errorf(`TxByteSliceAsList error: expected  ConditionalCheckFailed got %q`, err)
		}
	}

}

// TestDynMarshalStruct query with attribute not in record.
func TestDynMarshalStruct4(t *testing.T) {

	var pk = 3000
	var sk = "MarshalStruct4"

	type Record struct {
		PKey      int
		SortK     string
		Bytes     []byte
		ByteSlice [][]byte // dynamodb.MarshalMap will generate binary set for a binrary slice. Cannot be overriden from here as there is no dynamodbav for LIST. Use Attribute() to override.
		MyField   string
		Letters   []string // dynamodb.MarshalMap will generate string list from a string slice.
		Numbers   []int    // dynamodb.MarshalMap will generate number list from a int slice.
	}

	type Record2 struct {
		PKey        int
		SortK       string
		Bytes       []byte
		ByteSlice   [][]byte // dynamodb.MarshalMap will generate binary set for a binrary slice. Cannot be overriden from here as there is no dynamodbav for LIST. Use Attribute() to override.
		MyField     string
		Letters     []string // dynamodb.MarshalMap will generate string list from a string slice.
		Numbers     []int    // dynamodb.MarshalMap will generate number list from a int slice.
		UnAttribute int
	}

	r := Record{
		PKey:      pk,
		SortK:     sk,
		Bytes:     []byte{48, 49},
		ByteSlice: [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}},
		MyField:   "MyFieldValue",
		Letters:   []string{"a", "b", "c", "d"},
		Numbers:   []int{1, 2, 3},
	}

	var or Record2

	utx := New("DynMarshalStruct") // std api
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = New("DynMarshalStruct") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewInsert(gtbl).Submit(&r)
	err = utx.Execute()

	if err != nil {
		t.Errorf("DynMarshalStruct Error: in Execute of Pre DeleteCond %s", err)
	}

	q := NewQuery("qlablel", gtbl)
	q.Select(&or).Key("PKey", pk).Key("SortK", sk)
	err := q.Execute()
	if err != nil {
		t.Errorf("DynMarshalStruct Error: in Execute of qlabel %s", err)
	}

	if len(or.Bytes) != 2 {
		t.Errorf("DynMarshalStruct Error: Query expected len of 2 got %d", len(or.Bytes))
	}

	if len(or.ByteSlice) != 3 {
		t.Errorf("DynMarshalStruct Error: Query expected len of 3 got %d", len(or.ByteSlice))
	}

	if len(or.Letters) != 4 {
		t.Errorf("DynMarshalStruct Error: Query expected len of 4 got %d", len(or.ByteSlice))
	}

}

func TestDynMarshalStruct5(t *testing.T) {

	var pk = 3000
	var sk = "mdb"
	var sk2 = "mdb2"

	type Tab struct {
		PKey          int    `mdb:",KEY"` // test query by removing Key() method
		SortK         string `mdb:",KEY"`
		FirstName     string
		LastName      string   `dynamodbav:"SirName"`
		Years         int      `dynamodbav:"Age"`
		ByteSliceSet  [][]byte `dynamodbav:"ByteSlSet,BinarySet" , mdb:",BinarySet"` // dynamodb.MarshalMap will generate binary set for a binrary slice. Cannot be overriden from here as there is no dynamodbav for LIST. Use Attribute() to override.
		ByteSliceList [][]byte `dynamodbav:"ByteSlList"`                             // dynamodb.MarshalMap will generate binary set for a binrary slice. Cannot be overriden from here as there is no dynamodbav for LIST. Use Attribute() to override.
		Salary        float64
		DOB           string
		Height        int
		C             int    `mdb:"Ceiling"`
		Name          string `mdb:"-"`
	}

	in := &Tab{PKey: pk, SortK: sk, FirstName: "Ross", LastName: "Smith", Years: 59, Salary: 155000, Name: "This is my name", Height: 174, C: 232}
	in.ByteSliceSet = [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}}
	in.ByteSliceList = [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}}

	in2 := &Tab{PKey: pk, SortK: sk2, FirstName: "Paul", LastName: "Smith", Years: 45, Salary: 320000, Name: "Another name", Height: 175, C: 5562}
	in2.ByteSliceSet = [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}}
	in2.ByteSliceList = [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}}
	data := []*Tab{in, in2}

	utx := New("DataLoad") // std api
	//  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	//	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk)
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk2)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = NewTx("DataLoad") // std api

	for _, r := range data {
		utx.NewInsert(gtbl).Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}
	t.Log("=================== insert complete ======================")
	var rec Tab

	t.Log("=================== select 1 check ======================")
	// note, NewQuery does not solicit table columns so columns passed to Key() can be verify as keys.
	// This check is deferred to database where it will fail during Execute() with db error
	txg := NewQuery("query-test-label", gtbl)
	txg.Select(&rec).Key("PKey", pk).Key("SortK", sk)
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if rec.Salary != 155000 {
		t.Errorf("Expected Salary of 155000 got %v", rec.Salary)
	}

	if rec.Years != 59 {
		t.Errorf("Expected Age of 59 got %v", rec.Years)
	}

	t.Log("=================== select 2 check ======================")

	txg = NewQuery("query-test-label", gtbl)
	txg.Select(&rec).Key("PKey", pk).Key("SortK", sk2)
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if rec.Salary != 320000 {
		t.Errorf("Expected Salary of 320000 got %v", rec.Salary)
	}

	if rec.Years != 45 {
		t.Errorf("Expected Age of 45 got %v", rec.Years)
	}
	t.Log("=================== Start update 1 ======================")

	type TabQ struct {
		PKey   int    `mdb:",KEY"` //TODO: test with one qote only
		SortK  string `mdb:",KEY"`
		Salary float64
	}

	type TabU struct {
		PKey   int     `mdb:",KEY"` //TODO: test with one qote only
		SortK  string  `mdb:",KEY"`
		Years  int     `mdb:"Age,increment"` //`dynamodbav:"Age", mdb:",increment"`
		Salary float64 //`mdb:",multiply"`
		DOB    string
		Name   string `dynamodbav:"-"` // `mdb:"-`"
	}

	// q1 := TabQ{PKey: pk, SortK: sk}
	// q2 := TabQ{PKey: pk, SortK: sk2}

	var q1, q2 TabQ

	qtx := NewQuery("multiply query", gtbl)
	qtx.Select(&q1).Key("PKey", pk).Key("SortK", sk) // TODO: Submit(q1)
	qtx.Execute()

	qtx = NewQuery("multiply query", gtbl)
	qtx.Select(&q2).Key("PKey", pk).Key("SortK", sk2) // TODO: Submit(q2)
	qtx.Execute()

	sal1 := q1.Salary * 1.25
	sal2 := q2.Salary * 1.1
	t.Logf("Salary 1= %v", sal1)
	t.Logf("Salary 2= %v", sal2)

	in3 := TabU{PKey: pk, SortK: sk, Years: 0, Salary: sal1, Name: "This is my name", DOB: "13-03-1946"}
	in4 := TabU{PKey: pk, SortK: sk2, Years: 0, Salary: sal2, Name: "Another name", DOB: "26-01-1977"}

	utx = NewTx("DataLoad") // std api
	utx.NewUpdate(gtbl).Submit(&in3).Where("Salary = ?").Values(q1.Salary)
	utx.NewUpdate(gtbl).Submit(&in4).Where("Salary = ?").Values(q2.Salary)
	err = utx.Execute()

	if err != nil {
		t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	}
	t.Log("=================== update 1 complete ======================")
	t.Log("=================== query check 1 ======================")
	txg = NewQuery("query-test-label", gtbl)
	txg.Select(&rec).Key("PKey", pk).Key("SortK", sk)
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if rec.Years != 60 {
		t.Errorf("Expected Salary of 60 got %v", rec.Years)
	}

	if rec.Salary != 193750 {
		t.Errorf("Expected Salary of 193750 got %v", rec.Salary)
	}

	if rec.Years != 60 {
		t.Errorf("Expected Salary of 46 got %v", rec.Years)
	}
	t.Log("=================== query check 2 ======================")
	txg = NewQuery("query-test-label", gtbl)
	txg.Select(&rec).Key("PKey", pk).Key("SortK", sk2)
	err = txg.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if rec.Salary != 352000 {
		t.Errorf("Expected Salary of 352000 got %v", rec.Salary)
	}

	if rec.Years != 46 {
		t.Errorf("Expected Salary of 46 got %v", rec.Years)
	}
	time.Sleep(1 * time.Second)
	// t.Log("=================== update 2 ======================")
	// utx = NewTx("DataLoad") // std api

	// for _, r := range data2 {
	// 	utx.NewUpdate(gtbl).Submit(r)
	// }
	// err = utx.Execute()

	// if err != nil {
	// 	t.Errorf(`Error: in Execute of SQLSubmit got %q`, err)
	// }
	t.Log("=================== query check 1 ======================")
	// txg = NewQuery("query-test-label", gtbl)
	// txg.Select(&rec).Key("PKey", pk).Key("SortK", sk)
	// err = txg.Execute()
	// if err != nil {
	// 	t.Errorf("Error: %s", err)
	// }

	// if rec.Years != 61 {
	// 	t.Errorf("Expected Salary of 60 got %v", rec.Years)
	// }
	// if rec.Salary != 242187.5 {
	// 	t.Errorf("Expected Salary of 242187.5 got %v", rec.Salary)
	// }
	// t.Log("=================== query check 2 ======================")
	// txg = NewQuery("query-test-label", gtbl)
	// txg.Select(&rec).Key("PKey", pk).Key("SortK", sk2)
	// err = txg.Execute()
	// if err != nil {
	// 	t.Errorf("Error: %s", err)
	// }

	// if rec.Salary != 387200 {
	// 	t.Errorf("Expected Salary of 387200 got %v", rec.Salary)
	// }

	// if rec.Years != 47 {
	// 	t.Errorf("Expected Salary of 46 got %v", rec.Years)
	// }

	// time.Sleep(1 * time.Second)

	// Below version does works but corrupts data.  work using &r of struct, as local r is a copy of data3 element not the address of the actual element.
	data3 := []TabU{in3, in4}

	utx = NewTx("DataLoad") // std api

	for _, r := range data3 {
		utx.NewUpdate(gtbl).Submit(r)
	}
	err = utx.Execute()

	if err != nil {
		if !strings.Contains(err.Error(), `using unaddressable value`) {
			t.Errorf(`SQLSubmit Error: expected error "using unaddressable value" got %q`, err)
		}
	}
}

// UpdateListAppendEmpty - update List attribute when not exists requires attributenotexist()
func TestDynUpdateListAppendEmpty(t *testing.T) {
	t.Fail()
}

func TestDynUpdateListReverse(t *testing.T) {
	t.Fail()
}

// TestByteSliceAsList test default data type for binary slice in dynamodb.
// Whereas the SDK Marshal() will create a datatype of SET for a binary slice MethodDB will create a LIST (as this is what GoGraph expects)
// This represents an inconsistency that may need to be changed.
func TestDynByteSliceAsList(t *testing.T) {

	var ByteSlice [][]byte = [][]byte{[]byte{48, 49}, []byte{49, 49}, []byte{52, 49}}

	utx := New("ByteSliceAsList") // std api
	u := utx.NewInsert(gtbl).Key("PKey", 4000).Key("SortK", "ByteSlice")
	u.Attribute("BinarySliceList", ByteSlice)
	u.Attribute("BinarySliceSet", ByteSlice, dyn.BinarySet)
	u.Attribute("BinarySliceNoMod", ByteSlice)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}
}

func TestDynDeleteMod(t *testing.T) {

	//s.          tring{"{"KEY", "APPEND", "PREPEND", "REMOVE", "IsKey", "omitEmpty", "omitEmpty", "omitEmpty"}"}
	input := []string{"KEY", "SET", "APPEND", "PREPEND", "SET", "REMOVE", "IsKey", "omitEmpty"}
	t.Logf("input: %#v\n", input)

	mods := mut.ModifierS(input)
	mods.Delete("SET")

	if len(mods) != 6 {
		t.Errorf("Expected 6 got %d", len(mods))
	}

}

func TestDynDeleteMod2(t *testing.T) {

	//s.          tring{"{"KEY", "APPEND", "PREPEND", "REMOVE", "IsKey", "omitEmpty", "omitEmpty", "omitEmpty"}"}
	input := []string{"KEY"}
	t.Logf("input: %#v\n", input)

	mods := mut.ModifierS(input)
	mods.Delete("KEY")

	if len(mods) != 0 {
		t.Errorf("Expected 0 got %d", len(mods))
	}

}

func TestDynMethodValidation(t *testing.T) {

	utx := New("MethodValidation") // std api
	utx.NewInsert(gtbl).Key("PKey", 3031, mut.SET)
	utx.NewInsert(gtbl).Key("PKey", 3031, mut.ANY)
	utx.NewInsert(gtbl).Key("PKey", 3031, dyn.BEGINSWITH)
	err = utx.Execute()
	if len(utx.GetErrors()) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(utx.GetErrors()))
	}
	for i, e := range utx.GetErrors() {
		if i == 0 && !strings.Contains(e.Error(), "cannot specify SET with Key") {
			t.Errorf(`Expected error: "cannot specify SET with Key"`)
		}
		if i == 1 && !strings.Contains(e.Error(), "cannot specify ANY with Key") {
			t.Errorf(`Expected error: "cannot specify ANY with Key"`)
		}
	}

	utx = New("MethodValidation2") // std api
	utx.NewUpdate(gtbl).Set("PKey", 3031).Append("xx", 4)
	err = utx.Execute()

	if len(utx.GetErrors()) != 1 {
		t.Errorf("Expected 1 errors, got %d", len(utx.GetErrors()))
	}
	for i, e := range utx.GetErrors() {
		if i == 0 && !strings.Contains(e.Error(), "expected an array/slice type for Append") {
			t.Errorf(`Expected error: "expected an array/slice type for Append"`)
			continue
		}
	}
}

func TestDynUIDSliceAsList(t *testing.T) {

	type out struct {
		PKey            int
		SortK           string
		BinarySliceList []uuid.UID
	}

	var outx out

	uid1, _ := uuid.MakeUID()
	uid2, _ := uuid.MakeUID()
	uid3, _ := uuid.MakeUID()
	uid4, _ := uuid.MakeUID()

	uidS := []uuid.UID{uid1, uid2, uid3, uid4}

	uid11, _ := uuid.MakeUID()
	uid12, _ := uuid.MakeUID()
	uid13, _ := uuid.MakeUID()
	uid14, _ := uuid.MakeUID()

	uidS2 := []uuid.UID{uid11, uid12, uid13, uid14}

	utx := New("UIDSliceAsList") // std api
	u := utx.NewInsert(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Attribute("BinarySliceList", uidS).Attribute("BinarySliceSet", uidS, dyn.BinarySet)
	u.Attribute("BinarySliceNoMod", uidS)
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Attribute("BinarySliceList", uidS2, mut.APPEND)
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Attribute("BinarySliceList", uidS2) //  append is default oper for array
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Prepend("BinarySliceList", uidS2)
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Attribute("BinarySliceList", uidS2, mut.PREPEND)
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Attribute("BinarySliceList", uidS2, mut.SET)
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Attribute("BinarySliceList", uidS2, mut.PREPEND)
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Set("BinarySliceList", uidS2)
	utx.NewUpdate(gtbl).Key("PKey", 3031).Key("SortK", "UIDSliceAsList").Append("BinarySliceList", uidS)
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}
	q1 := NewQuery("query", gtbl)
	q1.Select(&outx).Key("PKey", 3031).Key("SortK", "UIDSliceAsList")
	err = q1.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if len(outx.BinarySliceList) != 8 {
		t.Errorf("Expect len of 8 got %d", len(outx.BinarySliceList))
	}

	t.Logf("q1.BinarySliceList[4] %s", outx.BinarySliceList[4].Base64())
	if outx.BinarySliceList[4].Base64() != uid1.Base64() {
		t.Errorf("UID does not match expected value: %s  %s", uid1.Base64(), outx.BinarySliceList[5].Base64())
	}
	t.Logf("len: %d", len(outx.BinarySliceList))

}

func TestDynAttributeValidation(t *testing.T) {

	pk := 3040
	sk := "KeyMod"
	sk2 := "KeyMod2"
	sk3 := "KeyMod3"

	type out struct {
		PKey  int
		SortK string
		Age   int
	}

	var outx out

	utx := New("AttributeValidationMissingKey") // std api
	utx.NewInsert(gtbl).Key("PKey", pk).Attribute("Age", 33).Attribute("DOB", "12-Jan-1988")
	err = utx.Execute()

	if len(utx.GetErrors()) != 1 {
		t.Errorf("AttributeValidationMissingKey: expected 1 error got %d", len(utx.GetErrors()))
	}
	if err != nil {
		if !strings.Contains(err.Error(), "ValidationException") {
			t.Errorf(`AttributeValidationMissingKey: Expected error "ValidationException" got %q"`, err.Error())
		}
		if !strings.Contains(err.Error(), "Missing the key SortK in the item") {
			t.Errorf(`AttributeValidationMissingKey: Expected error "Missing the key SortK in the item" got %q"`, err.Error())
		}

	}

	utx = New("AttributeValidation") // std api
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 33).Attribute("DOB", "12-Jan-1988")
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk2).Attribute("Age", 38).Attribute("DOB", "4-May-1958")
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk3).Attribute("Age", 44).Attribute("DOB", "22-Jan-2012")
	err = utx.Execute()

	utx.NewUpdate(gtbl).Attribute("PKey", pk).Key("SortK", sk).Increment("PK")
	err = utx.Execute()

	if err != nil {
		for _, e := range utx.GetErrors() {
			if !strings.Contains(e.Error(), "has already been executed") {
				t.Errorf(`AttributeValidationAlready: Expected error "has already been executed" got %q`, err.Error())
			}
		}
	}

	utx.NewUpdate(gtbl).Attribute("PKey", pk).Key("SortK", sk).Increment("PK")
	err = utx.Execute()

	if err != nil {
		for _, e := range utx.GetErrors() {
			if !strings.Contains(e.Error(), "has already been executed") {
				t.Errorf(`AttributeValidationAlready2: Expected error "has already been executed" got %q`, err.Error())
			}
		}
	}

	utx = New("AttributeValidation")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk, dyn.BEGINSWITH).Attribute("Age", 34, mut.IsFilter, mut.GT).Increment("Age")
	err = utx.Execute()

	if len(utx.GetErrors()) > 1 {
		t.Errorf("Mutation Tag %s. Expected 1 error got %d ", utx.Tag, len(utx.GetErrors()))
	}

	utx = New("AttributeValidation2")
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk).Set("Age", 33).Set("DOB", "12-Jan-1988")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 32, mut.IsFilter, mut.GT).Add("Age", 10)
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 32, mut.IsFilter, mut.GE).Increment("Age")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Filter("Age", 32, mut.GT).Increment("Age")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 32, mut.GT).Increment("Age") // presume filter

	err = utx.Execute()
	q := NewQuery("query", gtbl)
	q.Select(&outx).Key("PKey", pk).Key("SortK", sk)
	err = q.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if outx.Age != 46 {
		t.Errorf("AttributeValidation2: Expected 37 got %d", outx.Age)
	}

	utx = New("AttributeValidationSET")
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 33).Attribute("DOB", "12-Jan-1988")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 12, mut.SET)

	err = utx.Execute()
	q = NewQuery("query", gtbl)
	q.Select(&outx).Key("PKey", pk).Key("SortK", sk)
	err = q.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if outx.Age != 12 {
		t.Errorf("AttributeValidationSET: Expected 12 got %d", outx.Age)
	}

	utx = New("AttributeValidationSet")
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 33).Attribute("DOB", "12-Jan-1988")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Set("Age", 22)

	err = utx.Execute()
	q = NewQuery("query", gtbl)
	q.Select(&outx).Key("PKey", pk).Key("SortK", sk)
	err = q.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if outx.Age != 22 {
		t.Errorf("AttributeValidationSet: Expected 22 got %d", outx.Age)
	}

	utx = New("AttributeValidationWhere")
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 33).Attribute("DOB", "12-Jan-1988")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Where("Age > 32").Add("Age", 10)
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Where("Age >= 32").Increment("Age")
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	q = NewQuery("query", gtbl)
	q.Select(&outx).Key("PKey", pk).Key("SortK", sk)
	err = q.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if outx.Age != 44 {
		t.Errorf("AttributeValidationWhere: Expected 44 got %d", outx.Age)
	}

	utx = New("AttributeValidationWhereFilter")

	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Where("Age >= 32").Filter("Age", 32, mut.GT).Increment("Age")
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Filter("Age", 32, mut.GT).Where("Age >= 32").Increment("Age")
	err = utx.Execute()

	for _, e := range utx.GetErrors() {
		if !strings.Contains(e.Error(), "Cannot mix Filter() and Where() methods") {
			t.Errorf(`AttributeValidation: Expected "Cannot mix Filter() and Where() methods" got %q`, e.Error())
		}
	}

	q = NewQuery("query", gtbl)
	q.Select(&outx).Key("PKey", pk).Key("SortK", sk)
	err = q.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	utx = New("AttributeValidationDelete") // std api
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk).Filter("Age", 44, mut.LE)
	err = utx.Execute()

	if err != nil {
		t.Errorf("AttributeValidationDelete: Error: %s", err)
	}

	var outy []out

	q = NewQuery("AttributeValidationQuery", gtbl)
	q.Select(&outy).Key("PKey", pk)
	err = q.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	if len(outy) != 2 {
		t.Errorf(`AttributeValidationQuery: Expected 2 got %d`, len(outy))

	}

	utx = New("AttributeValidationDeleteAll") // std api
	utx.NewDelete(gtbl).Key("PKey", pk)
	err = utx.Execute()
	if err != nil {
		if !strings.Contains(err.Error(), "Missing key specification") {
			t.Errorf(`AttributeValidationDeleteAll: Expected error "Missing key specification" got %q`, err.Error())
		}
	}

}

func TestDynAttributeValidation2(t *testing.T) {

	pk := 3040
	sk := "KeyMod"

	utx := New("AttributeValidationPK") // std api
	utx.NewUpdate(gtbl).Attribute("PKey", pk).Key("SortK", sk).Increment("PK")
	err = utx.Execute()

	if err != nil {
		for _, e := range utx.GetErrors() {
			if !strings.Contains(e.Error(), "is a key in table and cannot be updated") {
				t.Errorf(`AttributeValidationAlready3: Expected error "is a key in table and cannot be updated" got %q`, err.Error())
			}
		}
	}
	t.Logf("log: %s", err)
}

func TestDynAttributeValidation2a(t *testing.T) {

	pk := 3040
	sk := "KeyMod"

	utx := New("AttributeValidationPK") // std api
	utx.NewUpdate(gtbl).Attribute("PKey", pk, mut.IsKey).Key("SortK", sk).Increment("PK")
	err = utx.Execute()

	if err != nil {
		for _, e := range utx.GetErrors() {
			if !strings.Contains(e.Error(), "attribute that does not exist in the item") {
				t.Errorf(`AttributeValidationAlready3: Expected error "attribute that does not exist in the item" got %q`, err.Error())
			}
		}
	}
	t.Logf("log: %s", err)
}

func TestDynAttributeValidation3(t *testing.T) {

	pk := 3040
	sk := "KeyMod"

	utx := New("AttributeValidationPK") // std api
	utx.NewUpdate(gtbl).Key("PKey", pk).Key("SortK", sk).Increment("PK")
	err = utx.Execute()

	if err != nil {
		for _, e := range utx.GetErrors() {
			if !strings.Contains(e.Error(), "The provided expression refers to an attribute that does not exist in the item") {
				t.Errorf(`AttributeValidationAlready3: Expected error "The provided expression refers to an attribute that does not exist in the item" got %q`, err.Error())
			}
		}
	}
	t.Logf("log: %s", err)
}

func TestDynReturnValues(t *testing.T) {

	pk := 3050
	sk := "ReturnValue4delete"

	type out struct {
		PKey      int
		SortK     string
		Age       int
		DOB       string
		SirName   string
		FirstName string
		Salary    int
	}

	utx := New("AttributeValidationWhere")
	utx.NewInsert(gtbl).Key("PKey", pk).Key("SortK", sk).Attribute("Age", 33).Attribute("DOB", "12-Jan-1988").Attribute("SirName", "Paine").Attribute("FirstName", "Philip")
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	var rec out
	utx = New("ReturnValues") // std api
	utx.NewDelete(gtbl).Key("PKey", pk).Key("SortK", sk).Return(mut.RtnAllOld, &rec)
	err = utx.Execute()
	if err != nil {
		t.Errorf("ReturnValues Error: %s", err)
	}
	t.Logf("Deleted row: %#v", rec)
	if rec.Age != 33 {
		t.Errorf("ReturnValues error. Expected Age 33 got %d", rec.Age)
	}
	if rec.SirName != "Paine" {
		t.Errorf("ReturnValues error. Expected SirName Paine  got %q", rec.SirName)
	}

}

// IDSliceAsListDefault test default for array type using Attribute() is LIST. Can override default using modifiers,  dyn.BinarySet.
// Note: this is different to the dynamodb's Marshal API where the default for []byte arrays is SET.
func TestDynUIDSliceAsListDefault(t *testing.T) {

	type outT struct {
		PKey             int
		SortK            string
		BinarySliceList  []uuid.UID
		BinarySliceSet   []uuid.UID
		BinarySliceNoMod []uuid.UID
	}

	var out outT

	uid1, _ := uuid.MakeUID()
	uid2, _ := uuid.MakeUID()
	uid3, _ := uuid.MakeUID()
	uid4, _ := uuid.MakeUID()

	uidS := []uuid.UID{uid1, uid2, uid3, uid4}

	//
	utx := New("UIDSliceAsList") // std api
	u := utx.NewInsert(gtbl).Key("PKey", 3032).Key("SortK", "UIDSliceAsListDefault")
	u.Attribute("BinarySliceList", uidS)               // expect List
	u.Attribute("BinarySliceSet", uidS, dyn.BinarySet) // expect Set
	u.Attribute("BinarySliceNoMod", uidS, mut.SET)     // expect Set, mut.SET is "update set" not array type, is ignored as its suitable for update only
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	q1 := NewQuery("query", gtbl)
	q1.Select(&out).Key("PKey", 3032).Key("SortK", "UIDSliceAsListDefault")
	err = q1.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	for i := 0; i < 4; i++ {
		if out.BinarySliceList[i].Base64() != out.BinarySliceNoMod[i].Base64() {
			t.Errorf("Expected BinarySliceList to match BinarySliceNoMod")
		}
	}
	d := 0
	for i := 0; i < 4; i++ {
		if out.BinarySliceSet[i].Base64() == out.BinarySliceNoMod[i].Base64() {
			d++
		}
	}
	if d == 4 {
		t.Errorf("Expected BinarySliceSet not to match BinarySliceNoMod")
	}

	// utx = New("TxMarshallInput3") // std api
	// //  developer specified keys - not checked if GetTableKeys() not implemented, otherwise checked
	// //	utx.NewUpdate(gtbl).Key("PKey", 1001).Key("SortK", "Sydney").Where(`attribute_exists(MaxPopulation) and Population>MaxPopulation`).Subtract("MaxPopulation", 1)
	// utx.NewDelete(gtbl).Key("PKey", 3030).Key("SortK", "UIDSliceAsList")
	// err = utx.Execute()
	// if err != nil {
	// 	t.Errorf("Error: %s", err)
	// }
}

func TestDynModifiers(t *testing.T) {

	//var uid0 uuid.UID

	uid1, _ := uuid.MakeUID()
	uid2, _ := uuid.MakeUID()
	uid3, _ := uuid.MakeUID()
	uid4, _ := uuid.MakeUID()

	uidS := []uuid.UID{uid1, uid2, uid3, uid4}
	//uidS2 := []uuid.UID{uid1, uid2, uid0, uid4}
	uidS3 := []uuid.UID{uid1, uid2, uuid.UID{}, uid4}
	uidS4 := []uuid.UID{}
	// v := 0
	// vv := 99

	type Rec struct {
		PKey  int
		SortK string
		AA    []uuid.UID `dynamodbav:"A"`
	}

	var or []Rec

	utx := New("Modifiers") // std api

	// utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "Modifiers-1-BinarySliceList-uidS ()").Attribute("A", uidS)
	// utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "Modifiers-2-BinarySliceSet-uidS (BinaraySet").Attribute("A", uidS, dyn.BinarySet)
	// utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "Modifiers-3-BinarySlice-uidS (dyn.BinarySet, dyn.Omitemptyelem)").Attribute("A", uidS, dyn.BinarySet, dyn.Omitemptyelem)
	// utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "Modifiers-4-BinarySlice-uidS3 (dyn.BinarySet, dyn.Nullemptyelem)").Attribute("A", uidS3, dyn.BinarySet, dyn.Nullemptyelem)
	// utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "Modifiers-5-BinarySlice-uidS3(dyn.BinarySet, dyn.Omitemptyelem").Attribute("A", uidS3, dyn.BinarySet, dyn.Omitemptyelem)
	// utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "Modifiers-6-BinarySlice-uidS4(dyn.BinarySet, dyn.Omitempty)").Attribute("A", uidS4, dyn.BinarySet, dyn.Omitempty)

	utx.NewInsert(gtbl).Key("PKey", 5000).Attribute("SortK", "Modifiers-1-BinarySliceList-uidS ()").Attribute("A", uidS)
	utx.NewInsert(gtbl).Attribute("PKey", 5000).Attribute("SortK", "Modifiers-2-BinarySliceSet-uidS (BinaraySet").Attribute("A", uidS, dyn.BinarySet)
	utx.NewInsert(gtbl).Attribute("PKey", 5000).Attribute("SortK", "Modifiers-3-BinarySlice-uidS (dyn.BinarySet, dyn.Omitemptyelem)").Attribute("A", uidS, dyn.BinarySet, dyn.Omitemptyelem)
	utx.NewInsert(gtbl).Attribute("PKey", 5000).Attribute("SortK", "Modifiers-4-BinarySlice-uidS3 (dyn.BinarySet, dyn.Nullemptyelem)").Attribute("A", uidS3, dyn.BinarySet, dyn.Nullemptyelem)
	utx.NewInsert(gtbl).Attribute("PKey", 5000).Attribute("SortK", "Modifiers-5-BinarySlice-uidS3(dyn.BinarySet, dyn.Omitemptyelem").Attribute("A", uidS3, dyn.BinarySet, dyn.Omitemptyelem)
	utx.NewInsert(gtbl).Attribute("PKey", 5000).Attribute("SortK", "Modifiers-6-BinarySlice-uidS4(dyn.BinarySet, dyn.Omitempty)").Attribute("A", uidS4, dyn.BinarySet, dyn.Omitempty)

	// u.Attribute("V0 ()", v)
	// u.Attribute("V 0(dyn.Omitempty)", v, dyn.Omitempty)
	// u.Attribute("V99 ()", vv)
	// u.Attribute("V99 0(dyn.Omitempty)", vv, dyn.Omitempty)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

	q := NewQuery("Modifiers", gtbl)
	q.Select(&or).Key("PKey", 5000).Key("SortK", "Modifiers", "BEGINSWITH")
	err := q.Execute()
	if err != nil {
		t.Errorf("Error: in Execute of qlabel %s", err)
	}
	for _, v := range or {
		var e int
		t.Logf("len(or.A) %s  %d\n", v.SortK, len(v.AA))
		for i, v := range v.AA {
			if len(v) == 16 {
				e++
				t.Logf("elem: %d  %s ", i, v.Base64())
			} else {
				t.Logf("elem: %d  <%d> ", i, len(v))
			}
		}
		switch x := v.SortK[:11]; x {
		case "Modifiers-1":
			if len(v.AA) != 4 {
				t.Errorf("%s Expected 4 got %d", x, len(v.AA))
			}
			if e != 4 {
				t.Errorf("%s Expected 4 got %d", x, e)
			}
		case "Modifiers-2":
			if len(v.AA) != 4 {
				t.Errorf("%s Expected 4 got %d", x, len(v.AA))
			}
			if e != 4 {
				t.Errorf("%s Expected 4 got %d", x, e)
			}
		case "Modifiers-3":
			if len(v.AA) != 4 {
				t.Errorf("%s Expected 4 got %d", x, len(v.AA))
			}
			if e != 4 {
				t.Errorf("%s Expected 4 got %d", x, e)
			}
		case "Modifiers-4":
			if len(v.AA) != 4 {
				t.Errorf("%s Expected 4 got %d", x, len(v.AA))
			}
			if e != 3 {
				t.Errorf("%s Expected 4 got %d", x, e)
			}
		case "Modifiers-5":
			if len(v.AA) != 4 {
				t.Errorf("%s Expected 4 got %d", x, len(v.AA))
			}
			if e != 3 {
				t.Errorf("%s Expected 3 got %d", x, e)
			}
		case "Modifiers-6":
			if len(v.AA) != 0 {
				t.Errorf("%s Expected 4 got %d", x, len(v.AA))
			}
			if e != 0 {
				t.Errorf("%s Expected 0 got %d", x, e)
			}
		}
	}

	utx = New("Modifiers") // std api
	utx.NewDelete(gtbl).Key("PKey", 5000).Key("SortK", "Modifier", "EQ")
	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}
}

func TestDynEmptyBinarySetInsert(t *testing.T) {

	uidS4 := []uuid.UID{}

	utx := New("UIDSliceAsList") // std api
	utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "EmptyBinarySet").Attribute("BinarySlice4a-uidS4()", uidS4, dyn.BinarySet)

	err = utx.Execute()
	if err != nil {
		if strings.Contains(err.Error(), "Binary sets should not be empty") {
			return
		}
		t.Errorf("Error: %s", err)
	}

}

func TestDynEmptyBinaryList(t *testing.T) {

	uidS4 := []uuid.UID{}

	utx := New("EmptyBinaryList") // std api
	utx.NewInsert(gtbl).Key("PKey", 5000).Key("SortK", "EmptyBinaryList").Attribute("BinarySlice4a-uidS4()", uidS4)

	err = utx.Execute()
	if err != nil {
		t.Errorf("Error: %s", err)
	}

}

func TestDynValidateModifiers(t *testing.T) {

	uid1, _ := uuid.MakeUID()
	uid2, _ := uuid.MakeUID()
	uid3, _ := uuid.MakeUID()
	uid4, _ := uuid.MakeUID()

	uidS := []uuid.UID{uid1, uid2, uid3, uid4}

	utx := New("ValidateModifiers") // std api
	// utx.NewInsert(gtbl).Attribute("PKey", 6000).Attribute("SortK", "ValidateModifiers").Attribute("ValidateModifiers-uidS3", uidS, dyn.BinarySet, dyn.Omitemptyelem)
	utx.NewInsert(gtbl).Attribute("PKey", 6000).Key("SortKr", "ValidateModifiers").Attribute("ValidateModifiers-uidS3", uidS, dyn.BinarySet, dyn.Omitemptyelem)
	utx.NewInsert(gtbl).Key("PKey", 6000).Key("SortK", "ValidateModifiers").Attribute("ValidateModifiers-uidS3", uidS, dyn.BinarySet, dyn.Omitemptyelem, dyn.Nullemptyelem)
	// *** error detected in mutation
	err = utx.Execute()
	if err != nil {
		for _, e := range utx.GetErrors() {
			if strings.Contains(err.Error(), `SortK" is not a table key`) {
				t.Errorf("Error: %s", e)
			}
		}
	}

	utx = New("ValidateModifiers") // std api
	utx.NewInsert(gtbl).Key("PKey", 6000).Key("SortK", "ValidateModifiers").Attribute("ValidateModifiers-uidS3", uidS, dyn.BinarySet, dyn.Omitemptyelem, dyn.Nullemptyelem)
	// *** error detected in Execute
	err = utx.Execute()
	if err != nil {
		t.Logf("Error %s", err)
		if strings.Contains(err.Error(), "specify modifiers Nullempty and Omitempty together") {
			return
		}
		for _, e := range utx.GetErrors() {
			if strings.Contains(err.Error(), "specify modifiers Nullempty and Omitempty together") {
				t.Errorf("xxError: %s", e)
				//return
			}
			t.Errorf("xxError: %s", e)
		}
	}
}
