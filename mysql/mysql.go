package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ros2hp/method-db/db"
	mdbsql "github.com/ros2hp/method-db/internal/sql"
	"github.com/ros2hp/method-db/key"
	"github.com/ros2hp/method-db/log"
	"github.com/ros2hp/method-db/mut"
	"github.com/ros2hp/method-db/query"
	"github.com/ros2hp/method-db/tbl"

	_ "github.com/go-sql-driver/mysql"
)

const (
	logid = "mysql"
	mysql = "mysql"
)

type MySQL struct {
	options  []db.Option
	trunctbl []tbl.Name
	ctx      context.Context
	*sql.DB
}

func logerr(e error, panic_ ...bool) {

	if len(panic_) > 0 && panic_[0] {
		log.LogFail(fmt.Errorf("%s %w", logid, e))
		panic(e)
	}
	log.LogErr(fmt.Errorf("%s %w", logid, e))
}

func syslog(s string) {
	log.LogDebug(fmt.Sprintf("%s %s", logid, s))
}

func alertlog(s string) {
	log.LogAlert(fmt.Sprintf("%s %s", logid, s))
}

func Register(ctx context.Context, label string, path string) {

	client, err := newMySQL(path)
	if err != nil {
		logerr(err)
	} else {
		m := MySQL{DB: client, ctx: ctx}
		db.Register(label, "mysql", m)
	}
}

func newMySQL(dsn string) (*sql.DB, error) {

	// Note: first arg must be the same as the name registered by go-sql-driver/mysql using database/sql.Register(). Specifiy explicilty (ie. "mysql")
	// do not use a const or variables which can be changed by the developer.
	// Arg 2, the DSN can vary with the same "mysql" to get multiple sq.DB's that point to different databases.
	mdb, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(fmt.Errorf("Open database error: %w", err)) // TODO: don't panic here...
	}
	//defer mdb.Close() //TODO: when to db.close

	// Open doesn't open a connection. Validate DSN data:
	err = mdb.Ping()
	if err != nil {
		panic(err.Error()) // TODO: proper error handling
	}

	return mdb, err
}

func (h MySQL) RetryOp(e error) bool {
	return false
}

// Execute manipulates data. See ExecuteQuery()
func (h MySQL) Execute(ctx context.Context, bs []*mut.Mutations, tag string, api db.API, prepare bool, opt ...db.Option) error {

	var (
		err error
		cTx *sql.Tx // client Tx
	)

	// check API
	switch api {
	case db.TransactionAPI:

		if ctx == nil {
			syslog("0 MySQL transactional API is being used with no context passed in")
			cTx, err = h.Begin()
		} else {
			syslog("MySQL transactional API is being used with a context passed in")
			txo := sql.TxOptions{Isolation: sql.LevelReadCommitted} // TODO: use options argument in .DB(?, options...)
			cTx, err = h.BeginTx(ctx, &txo)
		}
		if err != nil {
			err := fmt.Errorf("Error in BeginTx(): %w", err)
			logerr(err)
			return err
		}
	case db.StdAPI:
		// non-transactional
		syslog("MySQL non-transactional API is being used.")
	case db.BatchAPI:
		syslog(fmt.Sprintf("MySQL does not support a Batch API. Std API will be used instead. Tag: %s", tag))
	default:
		panic(fmt.Errorf("MySQL Execute(): no api specified"))
	}

	err = mdbsql.Execute(ctx, h.DB, bs, tag, cTx, prepare, opt...)

	return err

}

// func (h MySQL) Truncate(tbs []tbl.Name) error {
// 	h.trunctbl = append(h.trunctbl, tbs...)
// 	// ctx := context.Background() // TODO Implement
// 	// for _, t := range tbs {
// 	// 	_, err := h.ExecContext(ctx, "truncate "+string(t))
// 	// 	if err != nil {
// 	// 		panic(err)
// 	// 	}
// 	// }
// 	return nil
// }

func (h MySQL) ExecuteQuery(ctx context.Context, q *query.QueryHandle, o ...db.Option) error {

	if ctx == nil {
		ctx = h.ctx
	}
	return mdbsql.ExecuteQuery(ctx, h.DB, q, o...)

}

func (h MySQL) Close(q *query.QueryHandle) error {

	return mdbsql.ClosePrepStmt(h.DB, q)

}

func (h MySQL) Ctx() context.Context {

	return h.ctx

}

func (h MySQL) CloseTx(bs []*mut.Mutations) {

	for _, j := range bs {
		for _, m := range *j {

			m := m.(*mut.Mutation)

			if m.PrepStmt() != nil {
				m.PrepStmt().(*sql.DB).Close()
			}
		}
	}

}

func (h MySQL) String() string {

	return "mysql [not default]"

}

func (h MySQL) GetTableKeys(ctx context.Context, table string) ([]key.TableKey, error) {
	return nil, nil
}

// 	var pk, sk string

// 	dto, err := tabCache.fetchTableDesc(ctx, h.Client, table)

// 	for _, vv := range dto.Table.KeySchema {
// 		for _, v := range qkeys {
// 			if *vv.AttributeName == v {
// 				if vv.KeyType == types.KeyTypeHash {
// 					pk = v
// 				} else {
// 					sk = v
// 				}
// 			}
// 		}
// 	}

// 	return []string{pk, sk}

// }
