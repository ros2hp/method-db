package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ros2hp/method-db/db"
	"github.com/ros2hp/method-db/log"
	"github.com/ros2hp/method-db/query"
)

var noDataFoundErr = errors.New("no rows in result set")

type prepStmtT map[string]*sql.Stmt

var prepStmtM prepStmtT

func init() {
	prepStmtM = make(prepStmtT)
}

func crProjection(q *query.QueryHandle) *strings.Builder {

	// generate projection
	var (
		first = true
	)
	var s strings.Builder
	s.WriteString(fmt.Sprintf("select /* tag: %s %s */ ", q.GetTag(), q.GetHint()))
	for _, v := range q.GetAttr() {
		if v.IsFetch() {
			if first {
				if len(v.Literal()) > 0 {
					s.WriteString(v.Literal() + "  as " + v.Name()) // for SQL : ,col alias,
				} else {
					s.WriteString(v.Name())
				}
				first = false
			} else {
				s.WriteByte(',')
				if len(v.Literal()) > 0 {
					s.WriteString(v.Literal() + " as " + v.Name()) // for SQL : ,col alias,
				} else {
					s.WriteString(v.Name())
				}
			}
		}
	}

	return &s

}

func ClosePrepStmt(client *sql.DB, q *query.QueryHandle) (err error) {

	if q.PrepStmt() != nil {
		ps := q.PrepStmt().(*sql.Stmt)
		err = ps.Close()
		if err != nil {
			logerr(fmt.Errorf("Failed to close prepared stmt %s, %w", q.Tag, err))
			return err
		}
		log.LogDebug(fmt.Sprintf("closed prepared stmt %s", q.Tag))
	}
	return nil
}

func sqlOpr(o string) string {

	switch o {
	case "EQ":
		return "="
	case "NE":
		return "!="
	case "LE":
		return "<="
	case "GE":
		return ">="
	case "LT":
		return "<"
	case "GT":
		return ">"
	case "NOT":
		return "not"
	}
	return o
}

// executeQuery handles one stmt per tx.NewQuery*()
// the idea of multiple queries to a tx needs to be considered so tx has []QueryHandle
func ExecuteQuery(ctx context.Context, client *sql.DB, q *query.QueryHandle, opt ...db.Option) error {

	var (
		// options
		oSingleRow = false
		err        error
		row        *sql.Row
		rows       *sql.Rows
		prepStmt   *sql.Stmt
		lenVals    int // entries in sqlValues
	)

	for _, o := range opt {
		switch strings.ToLower(o.Name) {
		case "singlerow":
			if v, ok := o.Val.(bool); !ok {
				fmt.Errorf(fmt.Sprintf("Expected bool value for prepare option got %T", o.Val))
				return err
			} else {
				oSingleRow = v
			}
		}
	}
	if q.Error() != nil {
		return fmt.Errorf(fmt.Sprintf("Cannot execute query because of error %s", q.Error()))
	}

	// validate query metadata in query.QueryHandle
	err = validateInput(q)
	if err != nil {
		return err
	}
	// ********************** generate SQL statement ********************************

	// ************** Projection *************

	// define projection based on struct passed via Select()
	s := crProjection(q)

	s.WriteString(" from ")
	s.WriteString(string(q.GetTable()))

	var sqlValues []interface{}

	if len(q.GetKeyAttrs()) > 0 || len(q.GetWhere()) > 0 || len(q.GetFilterAttrs()) > 0 {
		s.WriteString(" where ")
	}

	// ************** Key() *************

	wa := len(q.GetKeyAttrs())
	wkeys := wa
	// TODO: check keys only

	for i, v := range q.GetKeyAttrs() {
		// where (key1 and key2 and key3)
		if i == 0 {
			s.WriteByte('(')
		}
		s.WriteString(v.Name())
		s.WriteString(sqlOpr(v.GetOprStr()))
		s.WriteByte('?')
		sqlValues = append(sqlValues, v.Value())

		if wa > 0 && i < wa-1 {
			s.WriteString(" and ")
		} else if i == wa-1 {
			s.WriteString(") ")
		}
	}

	// ************** Where() *************
	var p int // parenthesis counter

	if len(q.GetWhere()) > 0 {

		// TODO: check non-keys only

		if q.GetOr() > 0 || q.GetAnd() > 0 {
			panic(fmt.Errorf("Cannot mix Filter() with d Where()"))
		}
		if wkeys > 0 {
			s.WriteString(" and (")
			p++
		}

		where := q.GetWhere()
		// replace any literal (struct tag) references
		literals := q.GetLiterals()
		if len(literals) > 0 {
			for _, l := range literals {
				where = strings.ReplaceAll(where, l.Name(), l.Literal())
			}
		}
		s.WriteString(where)

		if p > 0 {
			s.WriteByte(')')
		}

		sqlValues = append(sqlValues, q.GetValues()...)

	} else {

		// ************** Filter *************

		// TODO: check non-keys only
		fltr := q.GetFilterAttrs()
		wa = len(fltr)
		lenVals = len(sqlValues)

		for i, v := range fltr {

			if q.GetOr() > 0 && q.GetAnd() > 0 {
				return fmt.Errorf("Cannot mix OrFilter, AndFilter conditions. Use Where() & Values() instead")
			}

			var found bool

			if i == 0 {
				if wkeys > 0 {
					s.WriteString(" and (")
				}
			} else if wa > 0 && i <= wa-1 {
				switch v.BoolCd() {
				case query.AND:
					s.WriteString(" and ")
				case query.OR:
					s.WriteString(" or ")
				}
			}
			// search - swap for literal tag value if Filter() attr name matches attr name in Select()
			for _, vv := range q.GetAttr() {
				if vv.IsFetch() {
					if vv.Name() == v.Name() {
						if len(vv.Literal()) > 0 {
							s.WriteString(vv.Literal())
							found = true
						}
						break
					}
				}
			}
			if !found {
				s.WriteString(v.Name())
			} else {
				found = false
			}

			s.WriteString(sqlOpr(v.GetOprStr()))
			// check value is not an attribute name, in which case don't use "?"
			if col, ok := v.Value().(string); ok {
				// check against attributes in projection
				for _, v := range q.GetAttr() {
					if col == v.Name() {
						s.WriteString(col)
						found = true
						break
					}
				}
			}
			if !found {
				s.WriteByte('?')
				sqlValues = append(sqlValues, v.Value())
			}
			// (Key and key) and (filter or filter)

			if i == wa-1 {
				if wkeys > 0 {
					s.WriteByte(')')
				}
			}
		}
	}
	//
	if len(q.GetGroupBy()) > 0 {

		s.WriteString(" group by ")
		s.WriteString(q.GetGroupBy())

		if len(q.GetHaving()) > 0 {
			s.WriteString(" having ")
			s.WriteString(q.GetHaving())
			if lenVals == 0 {
				sqlValues = append(sqlValues, q.GetValues()...)
			}
		}
	}

	// check parameters ? equals Values
	if strings.Count(s.String(), "?") != len(q.GetValues()) {
		return fmt.Errorf("Expected %d Values got %d", strings.Count(s.String(), "?"), len(q.GetValues()))
	}

	if q.HasOrderBy() {
		s.WriteString(q.OrderByString())
	}

	if l := q.GetLimit(); l > 0 {
		s.WriteString(" limit " + strconv.Itoa(l))

	}

	// slog.Log("executeQuery", fmt.Sprintf("generated sql: [%s]", s.String()))
	// fmt.Printf("generated sql: [%s]", s.String())
	if q.Prepare() {

		log.LogDebug("executeQuery : Prepared query")

		if q.PrepStmt() == nil {

			if ctx != nil {
				prepStmt, err = client.PrepareContext(ctx, s.String())
			} else {
				prepStmt, err = client.Prepare(s.String())
			}
			if err != nil {
				return err
			}
			q.SetPrepStmt(prepStmt)

		} else {

			prepStmt = q.PrepStmt().(*sql.Stmt)
		}

		if oSingleRow {
			if ctx != nil {
				row = prepStmt.QueryRowContext(ctx, sqlValues...)
			} else {
				row = prepStmt.QueryRow(sqlValues...)
			}
		} else {
			if ctx != nil {
				rows, err = prepStmt.QueryContext(ctx, sqlValues...)
			} else {
				rows, err = prepStmt.Query(sqlValues...)
			}
		}

	} else {

		// non-prepared
		log.LogDebug("executeQuery Non-prepared query")
		if oSingleRow {
			if ctx != nil {
				row = client.QueryRowContext(ctx, s.String(), sqlValues...)
			} else {
				row = client.QueryRow(s.String(), sqlValues...)
			}
		} else {
			if ctx != nil {
				rows, err = client.QueryContext(ctx, s.String(), sqlValues...)
			} else {
				rows, err = client.Query(s.String(), sqlValues...)
			}
		}
	}
	if err != nil {
		return err
	}

	if oSingleRow {
		if err = row.Scan(q.Split()...); err != nil {

			if errors.Is(err, sql.ErrNoRows) {
				err = query.NoDataFoundErr
			}
		}
	} else {
		for i := 0; rows.Next(); i++ {
			// split struct fields into individual bind vars
			if err = rows.Scan(q.Split()...); err != nil {
				logerr(err)
			}
		}
		if err := rows.Err(); err != nil {
			logerr(err)
		}
	}

	if err != nil {
		return err
	}

	if q.Prepare() {
		q.Reset()
	}
	return nil
}

func validateInput(q *query.QueryHandle) error {
	// validate Keys attributes - TODO implement

	// validate Filter attributes - TODO implement

	// validate Projection attributes- TODO implement

	return nil
}
