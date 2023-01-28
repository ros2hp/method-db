# What is method-db-4-Go?

> "So out of the GoGraph project arose a sub-project, whose raison d'etre is to replace the confusing concoction of database boilerplate for SQL and NOSQL, with a single, method based approach, which is more readable, more concise and more productive and adds value to database operations, like recoverable queries and asynchronous non-blocking database reads, while facilitating higher throughput with zero code effort. I call it MethodDB-4-Go, or MethodDB for short,..."

That excerpt comes from an article in up coming article in medium.com (in three parts) on method-db.

Currently __Method-db__ supports AWS's __Dynamodb__ as its only NoSQL offering and any SQL database that supports Go's standard library package __database/SQL__. Over the next months it is planned to support Google's __Spanner__, and the Oracle and __Postgres__ drivers.

Othere databases that are planned to support are the cloud-native platforms such as CockroachDB, YugaByte. 

For examples of it use I direct you to GoGraph (github.com/ros2hp/gograph) where method-db is used exclusively for all communication with Dynamodb and MySQL.

In the meantime here are some code snippets from GoGraph.

A Dynamodb query using **Method-db**

```
    import (
     "github.com/ros2hp/method-db/tx"
     "github.com/ros2hp/method-db/dynamodb"
    )
    type graphMeta struct {
        SortK string
    }
    var sk []graphMeta


    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()      
   
    // register the Dynamodb database
    
    dynamodb.Register(ctx, "default", &wpEnd, \
        []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)

    // define and execute a query
    
    txq := tx.NewQueryContext(ctx, "GraphName", typesTblN)

    txq.Select(&sk).Key("PKey", "#Graph").Filter("Name", graphNm)

    err := txq.Execute()
    
    // process data 
    
    for _,v:=range sk {
     . . .
    }   
```

A MySQL query:

```
    import (
      "github.com/ros2hp/method-db/tx"
      "github.com/ros2hp/method-db/dynamodb"
      "github.com/ros2hp/method-db/mysql" 
      )
      type pNodeBid struct {
       Bid  int
       Puid uuid.UID
       Cnt  int64
      }
      
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel() 
      
      // register dynamodb and MySQL databases

      dynamodb.Register(ctx, "default", &wpEnd,
        []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)

     mysql.Register(ctx, "mysql-GoGraph", "admin:????@tcp(mysql8.???.?.rds.amazonaws.com:3306)/GoGraph")  
     
     // define a prepared query handle
     
     qtx := tx.NewQueryContext(ctx, "EdgeParent", tblEdge).DB("mysql-GoGraph").Prepare()
     defer qtx.Close()
     
     . . .
     for {
       var result := []pNodeBid{}

        // define and execute the query
        
        qtx.Select(&result).Key("Bid", bid).Filter("Cnt", 0, "GT").OrderBy("Cnt", query.Desc) 

        err = qtx.Execute()

        // process data in bind variable, result
        
        for _,v:=range result {
          . . .
        }
. . .
```

## Advanced Query Methods

**Method-db's** more advanced query methods are tailored towards multi-row queries and long running database operations, such as large  data loads. These methods aim to deliver higher throughput with full restart ability with little to no coding effort. 

* ***Select()*** accepts multiple bind variable arguments enabling the database to write to one bind variable while the application reads from the other enabling non-blocking database reads.
* ***ExecutebyChannel()***, as an alternative to ***Execute()***, returns a query result as a Go Channel or slice of Go Channels, enabling asynchronous communication between the database and the application. 
* orchestrate parallel processing of a table (or index) using the ***Worker()*** method
* pass in a worker function to ***ExecuteByFunc()*** and Method-bd will orchestrate a channel to pass the database results to the function
* maintain the state of a paginated query with ***Paginate()*** method. Currently supported on Dynamodb only. Provides full recovery from application failures, enabling the application to restart a paginated query from last page of the previously failed query. Particularly useful for implementing fully recoverable long running database operations.

The example below of ***ExecutebyChannel()*** comes from __githum.com/ros2hp/gograph/dp2__:

```
    // DP passes a propagate function to index query using key with node type
    func DP(ctx context.Context, ty string, id uuid.UID, restart bool, has11 map[string]struct{}) error {

	var buf1, buf2 []UnprocRec

	slog.LogAlert(logid, fmt.Sprintf("started. paginate id: %s. restart: %v, grmgr-parallel: %d ", id.Base64(), restart, *parallel))

	ptx := tx.NewQueryContext(ctx, "dpQuery", tbl.Block, "TyIX")
	ptx.Select(&buf1, &buf2).Key("Ty", types.GraphSN()+"|"+ty)
	ptx.Limit(param.DPbatch).Paginate(id, restart) // not a scan so no worker possible

	limiterDP := grmgr.New("dp", *parallel)

	// blocking call..
	err := ptx.ExecuteByFunc(func(ch_ interface{}) error {

		ch := ch_.(chan []UnprocRec)

		for qs := range ch {
			// page (aka buffer) of UnprocRec{}

			for _, u := range qs {

				ty := u.Ty[strings.Index(u.Ty, "|")+1:]

				limiterDP.Control()

				go Propagate(ctx, limiterDP, u.PKey, ty, has11)

				if elog.Errors() {
					panic(fmt.Errorf("Error in an asynchronous routine - see system log"))
				}
			}
			// wait for dp ascyn routines to finish
			// alternate solution, extend to three bind variables. Requires MethodDB change.
			limiterDP.Wait()
		}
		return nil
	})

	limiterDP.Unregister()

	return err

    }
```

## Data Mutations

Below is a Dynamodb transaction defined musing Method-db

```
    import (
        ...
        "github.com/ros2hp/method-db/tx"
    )
    // create a "transaction" handle and assign up to 25 mutations to it.
    txh := tx.NewTx("SaveNode")

    for _, nv := range nodev {
         ...
         // assign an insert mutation to transaction handle
          n = txh.NewInsert(tbl.Block)
          // add members (attributes/columns) to insert mutation
          n.AddMember("PKey", UID)
          n.AddMember("SortK", "A#A#T")
          n.AddMember("Graph", types.GraphSN())
          n.AddMember("Ty", types.GraphSN()+"|"+s)
          n.AddMember("IsNode", "Y")
          n.AddMember("IX", UID[:8])
          ...
          // assing an update mutation to transaction handle
          m := txh.NewUpdate(tbl.NodeScalar)
          m.Key("PKey", UID)
          // use AddMember this time instead of Key() 
          m.AddMember("SortK", sortk, mut.IsKey)  
          m.AddMember("N", float64(i), mut.Set)
          m.Increment("Cnt")
          m.Set("Ty", aTy)
          ...
    }
    // execute mutations using database transaction API
    err = txh.Execute()
```

MySQL example of data mutations:

```
    import (
        ...
        "github.com/ros2hp/method-db/tx"
        "github.com/ros2hp/method-db/mysql"
    )
    ...
    var ftx tx.TxHandle

    mysql.Register(ctx, "mysql-GoGraph", "admin:???(mysql8.???.us-east-1.rds.amazonaws.com:3306)/GoGraph")

    // create a mutation handle with a MySQL database as the target
    ftx = tx.New("setRunStatus").DB("mysql-goGraph")

    switch len(runid) > 0 {
         case true: 
          // add an insert mutation to the MySQL mutation handle
          m := ftx.NewInsert("Run$Operation")
          m.AddMember("Graph", types.GraphSN(), mut.IsKey)
          m.AddMember("TableName", *table)
          m.AddMember("Operation", "DP", mut.IsKey)
          m.AddMember("Status", status)
          m.AddMember("RunId", runid[0])
          m.AddMember("Created", "$CURRENT_TIMESTAMP$")

         case false:
          // add a merge mutation to the MySQL mutation handle
          m := ftx.NewMerge("Run$Operation").
          m.AddMember("Graph", types.GraphSN(), mut.IsKey)
          m.AddMember("Operation", "DP", mut.IsKey)
          m.AddMember("TableName", *table)
          m.AddMember("Status", status)
          m.AddMember("LastUpdated", "$CURRENT_TIMESTAMP$")
     }
    // execute mutation in MySQL
    err = ftx.Execute()
```

### MergeMutation() Method

Finally, the last type of mutation available is ***MergeMutation()***, not to be confused with ***NewMerge()***, which handles a single mutation's update-or-insert semantics. MergeMutation, on the other hand, is designed to handle the circumstance where multiple mutations target the same item/row, and use the merge semantics explained below, to merge each mutation into an in-memory copy of the original mutation, based on table name and primary key values. At execute time only the unique mutations, some of which represent the cumulative affect of many mutations, are sent to the database saving significant network traffic, database resources, cloud costs and time.