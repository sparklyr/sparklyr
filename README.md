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

    ## # Source:   table<sparklyr_tmp_10e78179546a1> [?? x 5]
    ## # Database: spark_connection
    ##    Sepal_Length Sepal_Width Petal_Length Petal_Width Species
    ##           <dbl>       <dbl>        <dbl>       <dbl>   <chr>
    ##  1          5.1         3.5          1.4   4.0620505  setosa
    ##  2          4.9         3.0          1.4   1.0869843  setosa
    ##  3          4.7         3.2          1.3   2.9272488  setosa
    ##  4          4.6         3.1          1.5   1.8834738  setosa
    ##  5          5.0         3.6          1.4   2.0481534  setosa
    ##  6          5.4         3.9          1.7   3.1012484  setosa
    ##  7          4.6         3.4          1.4   0.6176524  setosa
    ##  8          5.0         3.4          1.5   0.9280065  setosa
    ##  9          4.4         2.9          1.4   1.6219857  setosa
    ## 10          4.9         3.1          1.5   1.2929423  setosa
    ## # ... with 140 more rows

We can calculate π using `dplyr` and `spark_apply` as follows:

``` r
library(dplyr)

sdf_len(sc, 10000) %>%
  spark_apply(function() sum(runif(2, min = -1, max = 1) ^ 2) < 1) %>%
  filter(id) %>% count() %>% collect() * 4 / 10000
```

    ##        n
    ## 1 3.1628

Notice that `spark_log` shows `sparklyr` performing the following operations:

1.  The `Gateway` receives a request to execute custom `RDD` of type `WorkerRDD`.
2.  The `WorkerRDD` is evaluated on the worker node which initializes a new `sparklyr` backend tracked as `Worker` in the logs.
3.  The backend initializes an `RScript` process that connects back to the backend, retrieves data, performs the clossure and updates the result.

``` r
spark_log(sc, filter = "sparklyr:", n = 30)
```

    ## 17/06/04 00:11:15 INFO sparklyr: Gateway (1438) found mapping for session 1289
    ## 17/06/04 00:11:15 INFO sparklyr: Worker (1289) accepted connection
    ## 17/06/04 00:11:15 INFO sparklyr: Worker (1289) is waiting for sparklyr client to connect to port 61010
    ## 17/06/04 00:11:15 INFO sparklyr: Worker (1289) received command 0
    ## 17/06/04 00:11:15 INFO sparklyr: Worker (1289) found requested session matches current session
    ## 17/06/04 00:11:15 INFO sparklyr: Worker (1289) is creating backend and allocating system resources
    ## 17/06/04 00:11:15 INFO sparklyr: Worker (1289) created the backend
    ## 17/06/04 00:11:15 INFO sparklyr: Worker (1289) is waiting for r process to end
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) is connected to backend 
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) is connecting to backend session 
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) is connected to backend session 
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) created connection 
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) is connected 
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) retrieved worker context id 124 
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) retrieved worker context 
    ## 17/06/04 00:11:15 INFO sparklyr: RScript (1289) found 5000 rows 
    ## 17/06/04 00:11:16 INFO sparklyr: RScript (1289) retrieved 5000 rows 
    ## 17/06/04 00:11:16 INFO sparklyr: RScript (1289) updated 5000 rows 
    ## 17/06/04 00:11:16 INFO sparklyr: Worker (1289) Wait using lock for RScript completed
    ## 17/06/04 00:11:16 INFO sparklyr: RScript (1289) finished apply 
    ## 17/06/04 00:11:16 INFO sparklyr: RScript (1289) finished 
    ## 17/06/04 00:11:16 INFO sparklyr: Worker (1289) R process completed
    ## 17/06/04 00:11:16 INFO sparklyr: Worker (1289) is unregistering session in gateway
    ## 17/06/04 00:11:16 INFO sparklyr: Gateway (1438) accepted connection
    ## 17/06/04 00:11:16 INFO sparklyr: Worker (1289) is waiting for unregistration in gateway
    ## 17/06/04 00:11:16 INFO sparklyr: Gateway (1438) is waiting for sparklyr client to connect to port 8880
    ## 17/06/04 00:11:16 INFO sparklyr: Gateway (1438) received command 2
    ## 17/06/04 00:11:16 INFO sparklyr: Gateway (1438) received session 1289 unregistration request
    ## 17/06/04 00:11:16 INFO sparklyr: Gateway (1438) found session 1289 during unregistration request
    ## 17/06/04 00:11:16 INFO sparklyr: Worker (1289) finished unregistration in gateway with status 0

Finally, we disconnect:

``` r
spark_disconnect(sc)
```
