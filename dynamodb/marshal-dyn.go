//go:build dynamodb
// +build dynamodb

package dynamodb

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/ros2hp/method-db/dynamodb/internal/param"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/uuid"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	// "github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/service/dynamodb"
)

func validate(m *mut.Mutation) error {
	for _, m := range m.GetMembers() {
		for _, v := range *m.GetModifier() {
			if v == BinarySet {
				if m.GetModifier().Exists(StringSet) {
					return fmt.Errorf("Cannot specify modifiers BinarySet and StringSet together")
				}
				if m.GetModifier().Exists(NumberSet) {
					return fmt.Errorf("Cannot specify modifiers BinarySet and NumberSet together")
				}
			}
			if v == NumberSet {
				if m.GetModifier().Exists(StringSet) {
					return fmt.Errorf("Cannot specify modifiers BinarySet and StringSet together")
				}
				if m.GetModifier().Exists(BinarySet) {
					return fmt.Errorf("Cannot specify modifiers BinarySet and NumberSet together")
				}
			}
			if v == Nullempty {
				if m.GetModifier().Exists(Omitempty) {
					return fmt.Errorf("Cannot specify modifiers Nullempty and Omitempty together")
				}
			}
			if v == Nullemptyelem {
				if m.GetModifier().Exists(Omitemptyelem) {
					return fmt.Errorf("Cannot specify modifiers Nullempty and Omitempty together")
				}
			}
			if v == mut.APPEND {
				if m.GetModifier().Exists(mut.PREPEND) {
					return fmt.Errorf("Cannot specify modifiers Prepend and Append together")
				}
			}

			if v == mut.SET {
				if m.GetModifier().Exists(mut.APPEND) {
					return fmt.Errorf("Cannot specify modifiers Append and Set together")
				}
				if m.GetModifier().Exists(mut.PREPEND) {
					return fmt.Errorf("Cannot specify modifiers Prepend and Set together")
				}
			}
		}
	}
	return nil
}

func marshalAVMapKeys(m *mut.Mutation) (map[string]types.AttributeValue, error) {
	err := validate(m)
	if err != nil {
		return nil, err
	}
	return marshalAV_(m, m.GetKeys())
}

func marshalAVMap(m *mut.Mutation) (map[string]types.AttributeValue, error) {
	err := validate(m)
	if err != nil {
		return nil, err
	}
	return marshalAV_(m, m.GetMembers())
}

func marshalAV_(m *mut.Mutation, ms []*mut.Member) (map[string]types.AttributeValue, error) {

	var (
		err  error
		item map[string]types.AttributeValue
	)

	item = make(map[string]types.AttributeValue, len(m.GetMembers()))

	for _, col := range ms {

		if col.Name() == "__" {
			continue
		}

		av := marshalAvUsingValue(m, col.Value(), *col.GetModifier()...)
		if av != nil {
			item[col.Name()] = av
		}

	}
	return item, err
}

func marshalAvUsingValue(m *mut.Mutation, val interface{}, mod ...mut.Modifier) types.AttributeValue {

	mm := mut.ModifierS(mod)

	switch x := val.(type) {

	case uuid.UID:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		return &types.AttributeValueMemberB{Value: x}

	case []byte:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		return &types.AttributeValueMemberB{Value: x}

	case string:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		ss := val.(string)
		if ss == "$CURRENT_TIMESTAMP$" {
			tz, _ := time.LoadLocation(param.TZ)
			ss = time.Now().In(tz).String()
		}
		return &types.AttributeValueMemberS{Value: ss}

	case float64:
		if x == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if x == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		s := strconv.FormatFloat(x, 'g', -1, 64)
		if mm.Exists(String) {
			return &types.AttributeValueMemberS{Value: s}
		}
		return &types.AttributeValueMemberN{Value: s}

	case int64:
		if x == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if x == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		s := strconv.FormatInt(x, 10)
		if mm.Exists(String) {
			return &types.AttributeValueMemberS{Value: s}
		}
		return &types.AttributeValueMemberN{Value: s}

	case int32:
		if x == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if x == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		s := strconv.FormatInt(int64(x), 10)
		if mm.Exists(String) {
			return &types.AttributeValueMemberS{Value: s}
		}
		return &types.AttributeValueMemberN{Value: s}

	case int:
		if x == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if x == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		s := strconv.Itoa(x)
		if mm.Exists(String) {
			return &types.AttributeValueMemberS{Value: s}
		}
		return &types.AttributeValueMemberN{Value: s}

	case bool:
		if x == false && mm.Exists(Omitempty) {
			return nil
		}
		if x == false && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		bl := val.(bool)
		return &types.AttributeValueMemberBOOL{Value: bl}

	case []uuid.UID:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		if mm.Exists(BinarySet) {
			lb := make([][]byte, len(x), len(x))
			for i, v := range x {
				lb[i] = v
			}
			return &types.AttributeValueMemberBS{Value: lb}

		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			lb[i] = &types.AttributeValueMemberB{Value: v}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case [][]byte:
		if reflect.ValueOf(val).IsZero() && mm.Exists(Omitempty) {
			//	if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if reflect.ValueOf(val).IsZero() && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		if reflect.ValueOf(val).IsZero() {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		if mm.Exists(BinarySet) {
			lb := make([][]byte, len(x), len(x))
			for i, v := range x {
				if reflect.ValueOf(v).IsZero() && mm.Exists(Omitemptyelem) {
					continue
				}
				if !(reflect.ValueOf(v).IsZero() && mm.Exists(Nullemptyelem)) {
					lb[i] = v
				}
			}
			return &types.AttributeValueMemberBS{Value: lb}
		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			lb[i] = &types.AttributeValueMemberB{Value: v}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []string:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		var ss []string
		if mm.Exists(Nullemptyelem) || mm.Exists(Omitemptyelem) {
			for _, s := range x {
				if len(s) == 0 && mm.Exists(Nullemptyelem) {
					//ss = append(ss, &types.AttributeValueMemberNULL{Value: true})
					continue
				}
				if len(s) == 0 && mm.Exists(Omitemptyelem) {
					continue
				}
				ss = append(ss, s)
			}
		} else {
			ss = x
		}

		// represented as List of string
		if mm.Exists(StringSet) {
			return &types.AttributeValueMemberSS{Value: ss}
		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, s := range ss {
			lb[i] = &types.AttributeValueMemberS{Value: s}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []int64:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		// represented as List of int64
		if mm.Exists(NumberSet) {
			lb := make([]string, len(x), len(x))
			for i, v := range x {
				lb[i] = strconv.FormatInt(v, 10)
			}
			return &types.AttributeValueMemberNS{Value: lb}
		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			s := strconv.FormatInt(v, 10)
			lb[i] = &types.AttributeValueMemberN{Value: s}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []int:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		// represented as List of int64
		if mm.Exists(NumberSet) {
			lb := make([]string, len(x), len(x))
			for i, v := range x {
				vv := int64(v)
				lb[i] = strconv.FormatInt(vv, 10)
			}
			return &types.AttributeValueMemberNS{Value: lb}
		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			vv := int64(v)
			s := strconv.FormatInt(vv, 10)
			lb[i] = &types.AttributeValueMemberN{Value: s}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []int32:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		// represented as List of int64
		if mm.Exists(NumberSet) {
			lb := make([]string, len(x), len(x))
			for i, v := range x {
				vv := int64(v)
				s := strconv.FormatInt(vv, 10)
				lb[i] = s
			}
			return &types.AttributeValueMemberNS{Value: lb}
		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			s := strconv.FormatInt(int64(v), 10)
			lb[i] = &types.AttributeValueMemberN{Value: s}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []bool:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			bl := v
			lb[i] = &types.AttributeValueMemberBOOL{Value: bl}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case []float64:
		if len(x) == 0 && mm.Exists(Omitempty) {
			return nil
		}
		if len(x) == 0 && mm.Exists(Nullempty) {
			return &types.AttributeValueMemberNULL{Value: true}
		}
		if mm.Exists(NumberSet) {
			lb := make([]string, len(x), len(x))
			for i, v := range x {
				lb[i] = strconv.FormatFloat(v, 'g', -1, 64)
			}
			return &types.AttributeValueMemberNS{Value: lb}
		}
		lb := make([]types.AttributeValue, len(x), len(x))
		for i, v := range x {
			s := strconv.FormatFloat(v, 'g', -1, 64)
			lb[i] = &types.AttributeValueMemberN{Value: s}
		}
		return &types.AttributeValueMemberL{Value: lb}

	case time.Time:
		//ss := x.Format(time.UnixDate)
		ss := x.String()
		return &types.AttributeValueMemberS{Value: ss}

	default:

		v := reflect.ValueOf(x)
		if v.Type().Kind() == reflect.Ptr {
			v = v.Elem()
		}
		if v.Kind() == reflect.Invalid {
			panic(fmt.Errorf("MarshalAttributes. reflect.Kind is invalid for %T", x))
		}
		switch v.Kind() {

		case reflect.Slice:

			lb := make([]types.AttributeValue, v.Len())

			for i := 0; i < v.Len(); i++ {
				bl := v.Index(i)

				if bl.Kind() == reflect.Ptr {
					bl = bl.Elem()
				}

				if bl.Kind() != reflect.Struct {
					panic(fmt.Errorf("MarshalAttributes. expected a struct  got a %v", bl.Kind()))
				}

				attrs, err := m.MarshalAttributes(bl.Interface(), "dynamodbav")
				av, err := marshalAV_(m, attrs)
				if err != nil {
					panic(err)
				}

				lb[i] = &types.AttributeValueMemberM{Value: av}

			}
			return &types.AttributeValueMemberL{Value: lb}

		case reflect.Struct:

			attrs, err := m.MarshalAttributes(v.Interface(), "dynamodbav")
			av, err := marshalAV_(m, attrs)
			if err != nil {
				panic(err)
			}
			fmt.Printf("av %#v\n", av)
			return &types.AttributeValueMemberM{Value: av}

		}
		panic(fmt.Errorf("val type %T not supported", val))

	}
	return nil
}
