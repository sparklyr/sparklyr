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

    ## # Source:   table<sparklyr_tmp_1237f2ce5d260> [?? x 5]
    ## # Database: spark_connection
    ##    Sepal_Length Sepal_Width Petal_Length Petal_Width Species
    ##           <dbl>       <dbl>        <dbl>       <dbl>   <chr>
    ##  1          5.1         3.5          1.4   1.7039286  setosa
    ##  2          4.9         3.0          1.4   1.3998011  setosa
    ##  3          4.7         3.2          1.3   0.9800857  setosa
    ##  4          4.6         3.1          1.5   2.5242787  setosa
    ##  5          5.0         3.6          1.4   0.2391819  setosa
    ##  6          5.4         3.9          1.7   2.7948977  setosa
    ##  7          4.6         3.4          1.4   2.5383136  setosa
    ##  8          5.0         3.4          1.5   0.8165440  setosa
    ##  9          4.4         2.9          1.4   5.0286428  setosa
    ## 10          4.9         3.1          1.5   4.1336981  setosa
    ## # ... with 140 more rows

We can calculate π using `dplyr` and `spark_apply` as follows:

``` r
library(dplyr)

sdf_len(sc, 10000) %>%
  spark_apply(function() sum(runif(2, min = -1, max = 1) ^ 2) < 1) %>%
  filter(id) %>% count() %>% collect() * 4 / 10000
```

    ##        n
    ## 1 3.1268

Notice that `spark_log` shows `sparklyr` performing the following operations:

1.  The `Gateway` receives a request to execute custom `RDD` of type `WorkerRDD`.
2.  The `WorkerRDD` is evaluated on the worker node which initializes a new `sparklyr` backend tracked as `Worker` in the logs.
3.  The backend initializes an `RScript` process that connects back to the backend, retrieves data, performs the clossure and updates the result.

``` r
spark_log(sc, filter = "sparklyr:", n = 30)
```

    ## 17/06/02 22:57:36 INFO sparklyr: RScript (6620) retrieved 10000 rows 
    ## 17/06/02 22:57:37 INFO sparklyr: RScript (6620) updated 10000 rows 
    ## 17/06/02 22:57:37 INFO sparklyr: RScript (6620) finished apply 
    ## 17/06/02 22:57:37 INFO sparklyr: RScript (6620) finished 
    ## 17/06/02 22:57:37 INFO sparklyr: Worker (6620) R process completed
    ## 17/06/02 22:57:37 INFO sparklyr: Worker (6620) Backend starting
    ## 17/06/02 22:57:37 INFO sparklyr: Worker (6620) RScript starting
    ## 17/06/02 22:57:37 INFO sparklyr: Worker (6620) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/219a155c-4243-4c17-9838-9f01e304ab89/sparkworker.R
    ## 17/06/02 22:57:37 INFO sparklyr: Worker (6620) R process starting
    ## 17/06/02 22:57:37 INFO sparklyr: RScript (6620) is starting 
    ## 17/06/02 22:57:37 INFO sparklyr: RScript (6620) is connecting to backend using port 8880 
    ## 17/06/02 22:57:38 INFO sparklyr: Worker (6620) accepted connection
    ## 17/06/02 22:57:38 INFO sparklyr: Worker (6620) is waiting for sparklyr client to connect to port 51291
    ## 17/06/02 22:57:38 INFO sparklyr: Worker (6620) received command 0
    ## 17/06/02 22:57:38 INFO sparklyr: Worker (6620) found requested session matches current session
    ## 17/06/02 22:57:38 INFO sparklyr: Worker (6620) is creating backend and allocating system resources
    ## 17/06/02 22:57:38 INFO sparklyr: Worker (6620) created the backend
    ## 17/06/02 22:57:38 INFO sparklyr: Worker (6620) is waiting for r process to end
    ## 17/06/02 22:57:38 INFO sparklyr: RScript (6620) is connected to backend 
    ## 17/06/02 22:57:38 INFO sparklyr: RScript (6620) is connecting to backend session 
    ## 17/06/02 22:57:38 INFO sparklyr: RScript (6620) is connected to backend session 
    ## 17/06/02 22:57:38 INFO sparklyr: RScript (6620) created connection 
    ## 17/06/02 22:57:38 INFO sparklyr: RScript (6620) is connected 
    ## 17/06/02 22:57:38 INFO sparklyr: RScript (6620) retrieved worker context 
    ## 17/06/02 22:57:38 INFO sparklyr: RScript (6620) found 10000 rows 
    ## 17/06/02 22:57:39 INFO sparklyr: RScript (6620) retrieved 10000 rows 
    ## 17/06/02 22:57:40 INFO sparklyr: RScript (6620) updated 10000 rows 
    ## 17/06/02 22:57:40 INFO sparklyr: RScript (6620) finished apply 
    ## 17/06/02 22:57:40 INFO sparklyr: RScript (6620) finished 
    ## 17/06/02 22:57:40 INFO sparklyr: Worker (6620) R process completed

Finally, we disconnect:

``` r
spark_disconnect(sc)
```
