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
      "github.com/ros2hp/method-db/mysql" // contains MySQL's implementation of MethodDB interfaces 
      )
      type pNodeBid struct {
       Bid  int
       Puid uuid.UID
       Cnt  int64
      }
      
      ctx, cancel := context.WithCancel(context.Background())
      defer cancel() 
      
      // register databases

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
