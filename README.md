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

    ## # Source:   table<sparklyr_tmp_989511e66bdf> [?? x 5]
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

Notice that `spark_log` shows `sparklyr` performing the following operations: 1. The `Gateway` receives a request to execute custom `RDD` of type `WorkerRDD`. 2. The `WorkerRDD` is evaluated on the worker node which initializes a new `sparklyr` backend tracked as `Worker` in the logs. 3. The backend initializes an `RScript` process that connects back to the backend, retrieves data, performs the clossure and updates the result.

``` r
spark_log(sc, filter = "sparklyr:")
```

    ## 17/05/29 11:13:54 INFO sparklyr: Session (825) is starting under 172.16.2.54/127.0.0.1 port 8880
    ## 17/05/29 11:13:54 INFO sparklyr: Session (825) found port 8880 is available
    ## 17/05/29 11:13:54 INFO sparklyr: Gateway (825) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:13:54 INFO sparklyr: Gateway (825) accepted connection
    ## 17/05/29 11:13:54 INFO sparklyr: Gateway (825) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:13:54 INFO sparklyr: Gateway (825) received command 0
    ## 17/05/29 11:13:54 INFO sparklyr: Gateway (825) found requested session matches current session
    ## 17/05/29 11:13:54 INFO sparklyr: Gateway (825) is creating backend and allocating system resources
    ## 17/05/29 11:13:55 INFO sparklyr: Gateway (825) created the backend
    ## 17/05/29 11:13:55 INFO sparklyr: Gateway (825) is waiting for r process to end
    ## 17/05/29 11:14:08 INFO sparklyr: Worker (122) RDD compute starting
    ## 17/05/29 11:14:08 INFO sparklyr: Worker (122) Backend starting
    ## 17/05/29 11:14:08 INFO sparklyr: Worker (122) RScript starting
    ## 17/05/29 11:14:08 INFO sparklyr: Worker (122) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/9f9113bf-7772-49d3-80a9-e788be009d9a/sparkworker.R
    ## 17/05/29 11:14:08 INFO sparklyr: Worker (122) R process starting
    ## 17/05/29 11:14:09 INFO sparklyr: RScript (122) is starting 
    ## 17/05/29 11:14:09 INFO sparklyr: RScript (122) is connecting to backend using port 8880 
    ## 17/05/29 11:14:09 INFO sparklyr: Gateway (825) accepted connection
    ## 17/05/29 11:14:09 INFO sparklyr: Gateway (825) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:14:09 INFO sparklyr: Gateway (825) received command 0
    ## 17/05/29 11:14:09 INFO sparklyr: Gateway (825) is searching for session 122
    ## 17/05/29 11:14:13 INFO sparklyr: Session (122) is starting under 172.16.2.54/127.0.0.1 port 8880
    ## 17/05/29 11:14:13 INFO sparklyr: Session (122) found port 8880 is not available
    ## 17/05/29 11:14:13 INFO sparklyr: Worker (122) is registering session in gateway
    ## 17/05/29 11:14:13 INFO sparklyr: Gateway (825) accepted connection
    ## 17/05/29 11:14:13 INFO sparklyr: Worker (122) is waiting for registration in gateway
    ## 17/05/29 11:14:13 INFO sparklyr: Gateway (825) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:14:13 INFO sparklyr: Gateway (825) received command 1
    ## 17/05/29 11:14:13 INFO sparklyr: Gateway (825) received session 122 registration request
    ## 17/05/29 11:14:13 INFO sparklyr: Worker (122) finished registration in gateway with status 0
    ## 17/05/29 11:14:13 INFO sparklyr: Worker (122) is waiting for sparklyr client to connect to port 53532
    ## 17/05/29 11:14:13 INFO sparklyr: Gateway (825) found mapping for session 122
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) accepted connection
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) is waiting for sparklyr client to connect to port 53532
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) received command 0
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) found requested session matches current session
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) is creating backend and allocating system resources
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) created the backend
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) is waiting for r process to end
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) is connected to backend 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) is connecting to backend session 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) is connected to backend session 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) created connection 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) is connected 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) retrieved worker context 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) context has 150 rows 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) retrieved 150 rows 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) updated 150 rows 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) finished apply 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) finished 
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) R process completed
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) Backend starting
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) RScript starting
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/a481fb4e-fbd5-4e0f-b986-c6186ad552da/sparkworker.R
    ## 17/05/29 11:14:14 INFO sparklyr: Worker (122) R process starting
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) is starting 
    ## 17/05/29 11:14:14 INFO sparklyr: RScript (122) is connecting to backend using port 8880 
    ## 17/05/29 11:14:14 INFO sparklyr: Gateway (825) accepted connection
    ## 17/05/29 11:14:14 INFO sparklyr: Gateway (825) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 11:14:15 INFO sparklyr: Gateway (825) received command 0
    ## 17/05/29 11:14:15 INFO sparklyr: Gateway (825) is searching for session 122
    ## 17/05/29 11:14:15 INFO sparklyr: Gateway (825) found mapping for session 122
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) accepted connection
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) is waiting for sparklyr client to connect to port 53532
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) received command 0
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) found requested session matches current session
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) is creating backend and allocating system resources
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) created the backend
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) is waiting for r process to end
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) is connected to backend 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) is connecting to backend session 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) is connected to backend session 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) created connection 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) is connected 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) retrieved worker context 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) context has 150 rows 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) retrieved 150 rows 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) updated 150 rows 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) finished apply 
    ## 17/05/29 11:14:15 INFO sparklyr: RScript (122) finished 
    ## 17/05/29 11:14:15 INFO sparklyr: Worker (122) R process completed

Finally, we disconnect:

``` r
spark_disconnect(sc)
```
