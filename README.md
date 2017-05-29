sparkworker: R Worker for Apache Spark
================

`sparkworker` provides support to execute arbitrary distributed r code, as any other `sparklyr` extension, load `sparkworker`, `sparklyr` and connecto to Apache Spark:

``` r
library(sparkworker)
library(sparklyr)

sc <- spark_connect(master = "local", version = "2.1.0")
iris_tbl <- sdf_copy_to(sc, iris)
```

To execute arbitrary functions use `spark_apply` as follows:

``` r
spark_apply(iris_tbl, function(rows) {
  rows$Petal_Width <- rows$Petal_Width + 1
  rows
})
```

    ## # Source:   table<sparklyr_tmp_9a9f46d79f05> [?? x 5]
    ## # Database: spark_connection
    ##    Sepal_Length Sepal_Width Petal_Length Petal_Width Species
    ##           <dbl>       <dbl>        <dbl>       <dbl>   <chr>
    ##  1          5.1         3.5          1.4         0.2  setosa
    ##  2          4.9         3.0          1.4         0.2  setosa
    ##  3          4.7         3.2          1.3         0.2  setosa
    ##  4          4.6         3.1          1.5         0.2  setosa
    ##  5          5.0         3.6          1.4         0.2  setosa
    ##  6          5.4         3.9          1.7         0.4  setosa
    ##  7          4.6         3.4          1.4         0.3  setosa
    ##  8          5.0         3.4          1.5         0.2  setosa
    ##  9          4.4         2.9          1.4         0.2  setosa
    ## 10          4.9         3.1          1.5         0.1  setosa
    ## # ... with 140 more rows

**Note:** Applying function closures is NYI.

Notice that `spark_log` shows `sparklyr` performing the following operations:

1.  The `Gateway` receives a request to execute custom `RDD` of type `WorkerRDD`.
2.  The `WorkerRDD` is evaluated on the worker node which initializes a new `sparklyr` backend tracked as `Worker` in the logs.
3.  The backend initializes an `RScript` process that connects back to the backend, retrieves data, performs the clossure and updates the result.

``` r
spark_log(sc, filter = "sparklyr:")
```

    ## 17/05/29 11:18:39 INFO sparklyr: Session (558) is starting under 172.16.2.54/127.0.0.1 port 8880
    ## 17/05/29 11:18:40 INFO sparklyr: Session (558) found port 8880 is available
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) accepted connection
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) received command 0
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) found requested session matches current session
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) is creating backend and allocating system resources
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) created the backend
    ## 17/05/29 11:18:40 INFO sparklyr: Gateway (558) is waiting for r process to end
    ## 17/05/29 11:18:54 INFO sparklyr: Worker (6337) RDD compute starting
    ## 17/05/29 11:18:54 INFO sparklyr: Worker (6337) Backend starting
    ## 17/05/29 11:18:54 INFO sparklyr: Worker (6337) RScript starting
    ## 17/05/29 11:18:54 INFO sparklyr: Worker (6337) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/10dd4076-8e18-44bf-ab63-227ef042a526/sparkworker.R
    ## 17/05/29 11:18:54 INFO sparklyr: Worker (6337) R process starting
    ## 17/05/29 11:18:54 INFO sparklyr: RScript (6337) is starting 
    ## 17/05/29 11:18:54 INFO sparklyr: RScript (6337) is connecting to backend using port 8880 
    ## 17/05/29 11:18:54 INFO sparklyr: Gateway (558) accepted connection
    ## 17/05/29 11:18:54 INFO sparklyr: Gateway (558) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:18:55 INFO sparklyr: Gateway (558) received command 0
    ## 17/05/29 11:18:55 INFO sparklyr: Gateway (558) is searching for session 6337
    ## 17/05/29 11:18:59 INFO sparklyr: Session (6337) is starting under 172.16.2.54/127.0.0.1 port 8880
    ## 17/05/29 11:18:59 INFO sparklyr: Session (6337) found port 8880 is not available
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) is registering session in gateway
    ## 17/05/29 11:18:59 INFO sparklyr: Gateway (558) accepted connection
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) is waiting for registration in gateway
    ## 17/05/29 11:18:59 INFO sparklyr: Gateway (558) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:18:59 INFO sparklyr: Gateway (558) received command 1
    ## 17/05/29 11:18:59 INFO sparklyr: Gateway (558) received session 6337 registration request
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) finished registration in gateway with status 0
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) is waiting for sparklyr client to connect to port 54120
    ## 17/05/29 11:18:59 INFO sparklyr: Gateway (558) found mapping for session 6337
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) accepted connection
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) is waiting for sparklyr client to connect to port 54120
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) received command 0
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) found requested session matches current session
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) is creating backend and allocating system resources
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) created the backend
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) is waiting for r process to end
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) is connected to backend 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) is connecting to backend session 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) is connected to backend session 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) created connection 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) is connected 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) retrieved worker context 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) context has 150 rows 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) retrieved 150 rows 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) updated 150 rows 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) finished apply 
    ## 17/05/29 11:18:59 INFO sparklyr: RScript (6337) finished 
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) R process completed
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) Backend starting
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) RScript starting
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/c1be82a6-7c1e-45b4-8f4c-d6f6a9a2a154/sparkworker.R
    ## 17/05/29 11:18:59 INFO sparklyr: Worker (6337) R process starting
    ## 17/05/29 11:19:00 INFO sparklyr: RScript (6337) is starting 
    ## 17/05/29 11:19:00 INFO sparklyr: RScript (6337) is connecting to backend using port 8880 
    ## 17/05/29 11:19:00 INFO sparklyr: Gateway (558) accepted connection
    ## 17/05/29 11:19:00 INFO sparklyr: Gateway (558) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:19:00 INFO sparklyr: Gateway (558) received command 0
    ## 17/05/29 11:19:00 INFO sparklyr: Gateway (558) is searching for session 6337
    ## 17/05/29 11:19:00 INFO sparklyr: Gateway (558) found mapping for session 6337
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) accepted connection
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) is waiting for sparklyr client to connect to port 54120
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) received command 0
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) found requested session matches current session
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) is creating backend and allocating system resources
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) created the backend
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) is waiting for r process to end
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) is connected to backend 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) is connecting to backend session 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) is connected to backend session 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) created connection 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) is connected 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) retrieved worker context 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) context has 150 rows 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) retrieved 150 rows 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) updated 150 rows 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) finished apply 
    ## 17/05/29 11:19:01 INFO sparklyr: RScript (6337) finished 
    ## 17/05/29 11:19:01 INFO sparklyr: Worker (6337) R process completed

Finally, we disconnect:

``` r
spark_disconnect(sc)
```
