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
spark_apply(iris_tbl, function(row) {
  row$Petal_Width <- row$Petal_Width + rgamma(1, 2)
  row
})
```

    ## # Source:   table<sparklyr_tmp_d5987b68f1dd> [?? x 5]
    ## # Database: spark_connection
    ##    Sepal_Length Sepal_Width Petal_Length Petal_Width Species
    ##           <dbl>       <dbl>        <dbl>       <dbl>   <chr>
    ##  1          5.1         3.5          1.4    3.356862  setosa
    ##  2          4.9         3.0          1.4    5.500614  setosa
    ##  3          4.7         3.2          1.3    1.426935  setosa
    ##  4          4.6         3.1          1.5    1.015859  setosa
    ##  5          5.0         3.6          1.4    1.094363  setosa
    ##  6          5.4         3.9          1.7    1.409504  setosa
    ##  7          4.6         3.4          1.4    2.276311  setosa
    ##  8          5.0         3.4          1.5    1.170719  setosa
    ##  9          4.4         2.9          1.4    2.520450  setosa
    ## 10          4.9         3.1          1.5    2.131878  setosa
    ## # ... with 140 more rows

Notice that `spark_log` shows `sparklyr` performing the following operations:

1.  The `Gateway` receives a request to execute custom `RDD` of type `WorkerRDD`.
2.  The `WorkerRDD` is evaluated on the worker node which initializes a new `sparklyr` backend tracked as `Worker` in the logs.
3.  The backend initializes an `RScript` process that connects back to the backend, retrieves data, performs the clossure and updates the result.

``` r
spark_log(sc, filter = "sparklyr:")
```

    ## 17/05/29 22:00:22 INFO sparklyr: Session (1798) is starting under 192.168.0.17/127.0.0.1 port 8880
    ## 17/05/29 22:00:22 INFO sparklyr: Session (1798) found port 8880 is available
    ## 17/05/29 22:00:22 INFO sparklyr: Gateway (1798) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 22:00:22 INFO sparklyr: Gateway (1798) accepted connection
    ## 17/05/29 22:00:22 INFO sparklyr: Gateway (1798) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 22:00:22 INFO sparklyr: Gateway (1798) received command 0
    ## 17/05/29 22:00:22 INFO sparklyr: Gateway (1798) found requested session matches current session
    ## 17/05/29 22:00:22 INFO sparklyr: Gateway (1798) is creating backend and allocating system resources
    ## 17/05/29 22:00:23 INFO sparklyr: Gateway (1798) created the backend
    ## 17/05/29 22:00:23 INFO sparklyr: Gateway (1798) is waiting for r process to end
    ## 17/05/29 22:00:37 INFO sparklyr: Worker (3373) RDD compute starting
    ## 17/05/29 22:00:37 INFO sparklyr: Worker (3373) Backend starting
    ## 17/05/29 22:00:37 INFO sparklyr: Worker (3373) RScript starting
    ## 17/05/29 22:00:37 INFO sparklyr: Worker (3373) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/a15752f3-2c47-47e1-b96a-6fe077dfa1a8/sparkworker.R
    ## 17/05/29 22:00:37 INFO sparklyr: Worker (3373) R process starting
    ## 17/05/29 22:00:37 INFO sparklyr: RScript (3373) is starting 
    ## 17/05/29 22:00:37 INFO sparklyr: RScript (3373) is connecting to backend using port 8880 
    ## 17/05/29 22:00:37 INFO sparklyr: Gateway (1798) accepted connection
    ## 17/05/29 22:00:37 INFO sparklyr: Gateway (1798) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 22:00:38 INFO sparklyr: Gateway (1798) received command 0
    ## 17/05/29 22:00:38 INFO sparklyr: Gateway (1798) is searching for session 3373
    ## 17/05/29 22:00:42 INFO sparklyr: Session (3373) is starting under 192.168.0.17/127.0.0.1 port 8880
    ## 17/05/29 22:00:42 INFO sparklyr: Session (3373) found port 8880 is not available
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) is registering session in gateway
    ## 17/05/29 22:00:42 INFO sparklyr: Gateway (1798) accepted connection
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) is waiting for registration in gateway
    ## 17/05/29 22:00:42 INFO sparklyr: Gateway (1798) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 22:00:42 INFO sparklyr: Gateway (1798) received command 1
    ## 17/05/29 22:00:42 INFO sparklyr: Gateway (1798) received session 3373 registration request
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) finished registration in gateway with status 0
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) is waiting for sparklyr client to connect to port 60554
    ## 17/05/29 22:00:42 INFO sparklyr: Gateway (1798) found mapping for session 3373
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) accepted connection
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) is waiting for sparklyr client to connect to port 60554
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) received command 0
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) found requested session matches current session
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) is creating backend and allocating system resources
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) created the backend
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) is waiting for r process to end
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) is connected to backend 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) is connecting to backend session 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) is connected to backend session 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) created connection 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) is connected 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) retrieved worker context 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) found 150 rows 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) retrieved 150 rows 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) retrieved 5 column names 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) applied clossure to 150 rows 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) updated 150 rows 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) finished apply 
    ## 17/05/29 22:00:42 INFO sparklyr: RScript (3373) finished 
    ## 17/05/29 22:00:42 INFO sparklyr: Worker (3373) R process completed
    ## 17/05/29 22:00:43 INFO sparklyr: Worker (3373) Backend starting
    ## 17/05/29 22:00:43 INFO sparklyr: Worker (3373) RScript starting
    ## 17/05/29 22:00:43 INFO sparklyr: Worker (3373) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/3ed0d8ea-5b37-44a2-b7bb-313898674ee2/sparkworker.R
    ## 17/05/29 22:00:43 INFO sparklyr: Worker (3373) R process starting
    ## 17/05/29 22:00:43 INFO sparklyr: RScript (3373) is starting 
    ## 17/05/29 22:00:43 INFO sparklyr: RScript (3373) is connecting to backend using port 8880 
    ## 17/05/29 22:00:43 INFO sparklyr: Gateway (1798) accepted connection
    ## 17/05/29 22:00:43 INFO sparklyr: Gateway (1798) is waiting for sparklyr client to connect to port 8880
    ## 17/05/29 22:00:44 INFO sparklyr: Gateway (1798) received command 0
    ## 17/05/29 22:00:44 INFO sparklyr: Gateway (1798) is searching for session 3373
    ## 17/05/29 22:00:44 INFO sparklyr: Gateway (1798) found mapping for session 3373
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) accepted connection
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) is waiting for sparklyr client to connect to port 60554
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) received command 0
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) found requested session matches current session
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) is creating backend and allocating system resources
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) created the backend
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) is waiting for r process to end
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) is connected to backend 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) is connecting to backend session 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) is connected to backend session 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) created connection 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) is connected 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) retrieved worker context 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) found 150 rows 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) retrieved 150 rows 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) retrieved 5 column names 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) applied clossure to 150 rows 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) updated 150 rows 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) finished apply 
    ## 17/05/29 22:00:44 INFO sparklyr: RScript (3373) finished 
    ## 17/05/29 22:00:44 INFO sparklyr: Worker (3373) R process completed

Finally, we disconnect:

``` r
spark_disconnect(sc)
```
