# What is method-db-4-Go?

The purpose of **Method-db-4-Go**, or **Method-db** for short, is to provide Go developers with a single api that caters to SQL and NoSQL databases.   

Currently __Method-db__ supports AWS's __Dynamodb__ and any database that supports the __database/SQL__ package of Go's standard library. The next database to be added will be Google's __Spanner__, followed by Postgres's own driver.

**Method-db** uses a methods based approach to defining querier and data manipulations and when chained together exudes an almost SQL like readability. Some of the methods are simple setters of an underlying variable, while other methods may orchestrate groutines and channels to implement asychronous communicaiton with the database.

Some simple code examples...

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

    // context is passed to all underlying mysql methods 
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()      
    // Register Dynamodb site. Use a label of "default" to set as the default database.   
    dynamodb.Register(ctx, "default", &wpEnd, \
        []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)

    // MethodDB Part 1: create a query handle. 
    // Supply a context, query label and table and optional index name.
    txq := tx.NewQueryContext(ctx, "GraphName", typesTblN)

    // MethodDB Part 2: define a query.
    txq.Select(&sk).Key("PKey", "#Graph").Filter("Name", graphNm)

    // MethodDB Part 3: execute query. 
    // Query results will appear in sk variable if successful.
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
      "github.com/ros2hp/method-db/mysql" // contains MySQL's implementation of MethodDB interfaces 
      )
      type pNodeBid struct {
       Bid  int
       Puid uuid.UID
       Cnt  int64
      }
      
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel() 
      
      // Register Dynamodb site. Use a label of "default" to set as the default database.   
      // ctx is a context and is used in all database communications.
      dynamodb.Register(ctx, "default", &wpEnd,
        []db.Option{db.Option{Name: "throttler", Val: grmgr.Control}, db.Option{Name: "Region", Val: "us-east-1"}}...)
        
     // Register your MySQL instance and give it a label e.g. "mysql-GoGraph"
     mysql.Register(ctx, "mysql-GoGraph", "admin:????@tcp(mysql8.???.?.rds.amazonaws.com:3306)/GoGraph")  
     
     // MethodDB Part 1: define a query handle. 
     //  NewQuery..() : creates a tx Query Handle. Supply context, query label and table name. 
     //  DB()  : as this is not for the default db must use DB() with a registered db label.
     //  Prepare(): optional. MySQL only. Defined query will be handled as a prepared stmt.
     //  Close(): mandatory for databases using /database/sql package
     
     qtx := tx.NewQueryContext(ctx, "EdgeParent", tblEdge)
     qtx.DB("mysql-GoGraph").Prepare()
     defer qtx.Close()
     
     . . .
     for {
       var result := []pNodeBid{}
        // MethodDB Part 2: query specification.  Only one can be defined for a NewQuery().
        //   MethodDB will generate the following SQL at execute time (Part 3): 
        //   "select Bid,Puid,Cnt from <tblEdge> where Bid = :bid and Cnt > 0 order by Cnt desc"
        
        qtx.Select(&result).Key("Bid", bid).Filter("Cnt", 0, "GT").OrderBy("Cnt", query.Desc) 

        // MethodDB Part 3: execute the query. Query results will appear in result variable.
        
        err = qtx.Execute()
        . . .
        // process data in bind variable, result
        
        for _,v:=range result {
          . . .
        }
. . .
