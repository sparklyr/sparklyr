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

    ## # Source:   table<sparklyr_tmp_115985170bf13> [?? x 5]
    ## # Database: spark_connection
    ##    Sepal_Length Sepal_Width Petal_Length Petal_Width Species
    ##           <dbl>       <dbl>        <dbl>       <dbl>   <chr>
    ##  1          5.1         3.5          1.4   2.0399916  setosa
    ##  2          4.9         3.0          1.4   0.8443701  setosa
    ##  3          4.7         3.2          1.3   0.5678453  setosa
    ##  4          4.6         3.1          1.5   0.6218222  setosa
    ##  5          5.0         3.6          1.4   1.3831671  setosa
    ##  6          5.4         3.9          1.7   1.1859260  setosa
    ##  7          4.6         3.4          1.4   3.3954184  setosa
    ##  8          5.0         3.4          1.5   2.7779661  setosa
    ##  9          4.4         2.9          1.4   1.8604742  setosa
    ## 10          4.9         3.1          1.5   2.0882468  setosa
    ## # ... with 140 more rows

We can calculate *p**i* using `dplyr` and `spark_apply` as follows:

``` r
library(dplyr)
```

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

``` r
sdf_len(sc, 10000) %>%
  spark_apply(function() sum(runif(2, min = -1, max = 1) ^ 2) < 1) %>%
  filter(id) %>% count() %>% collect() * 4 / 10000
```

    ##        n
    ## 1 3.1252

Notice that `spark_log` shows `sparklyr` performing the following operations:

1.  The `Gateway` receives a request to execute custom `RDD` of type `WorkerRDD`.
2.  The `WorkerRDD` is evaluated on the worker node which initializes a new `sparklyr` backend tracked as `Worker` in the logs.
3.  The backend initializes an `RScript` process that connects back to the backend, retrieves data, performs the clossure and updates the result.

``` r
spark_log(sc, filter = "sparklyr:", n = 30)
```

    ## 17/06/02 22:48:52 INFO sparklyr: RScript (9197) retrieved 10000 rows 
    ## 17/06/02 22:48:53 INFO sparklyr: RScript (9197) updated 10000 rows 
    ## 17/06/02 22:48:53 INFO sparklyr: RScript (9197) finished apply 
    ## 17/06/02 22:48:53 INFO sparklyr: RScript (9197) finished 
    ## 17/06/02 22:48:53 INFO sparklyr: Worker (9197) R process completed
    ## 17/06/02 22:48:53 INFO sparklyr: Worker (9197) Backend starting
    ## 17/06/02 22:48:53 INFO sparklyr: Worker (9197) RScript starting
    ## 17/06/02 22:48:53 INFO sparklyr: Worker (9197) Path to source file /var/folders/fz/v6wfsg2x1fb1rw4f6r0x4jwm0000gn/T/sparkworker/d05bee72-eb0c-4610-b001-c10ae94edc91/sparkworker.R
    ## 17/06/02 22:48:53 INFO sparklyr: Worker (9197) R process starting
    ## 17/06/02 22:48:53 INFO sparklyr: RScript (9197) is starting 
    ## 17/06/02 22:48:53 INFO sparklyr: RScript (9197) is connecting to backend using port 8880 
    ## 17/06/02 22:48:54 INFO sparklyr: Worker (9197) accepted connection
    ## 17/06/02 22:48:54 INFO sparklyr: Worker (9197) is waiting for sparklyr client to connect to port 50950
    ## 17/06/02 22:48:54 INFO sparklyr: Worker (9197) received command 0
    ## 17/06/02 22:48:54 INFO sparklyr: Worker (9197) found requested session matches current session
    ## 17/06/02 22:48:54 INFO sparklyr: Worker (9197) is creating backend and allocating system resources
    ## 17/06/02 22:48:54 INFO sparklyr: Worker (9197) created the backend
    ## 17/06/02 22:48:54 INFO sparklyr: Worker (9197) is waiting for r process to end
    ## 17/06/02 22:48:54 INFO sparklyr: RScript (9197) is connected to backend 
    ## 17/06/02 22:48:54 INFO sparklyr: RScript (9197) is connecting to backend session 
    ## 17/06/02 22:48:54 INFO sparklyr: RScript (9197) is connected to backend session 
    ## 17/06/02 22:48:54 INFO sparklyr: RScript (9197) created connection 
    ## 17/06/02 22:48:54 INFO sparklyr: RScript (9197) is connected 
    ## 17/06/02 22:48:54 INFO sparklyr: RScript (9197) retrieved worker context 
    ## 17/06/02 22:48:54 INFO sparklyr: RScript (9197) found 10000 rows 
    ## 17/06/02 22:48:55 INFO sparklyr: RScript (9197) retrieved 10000 rows 
    ## 17/06/02 22:48:56 INFO sparklyr: RScript (9197) updated 10000 rows 
    ## 17/06/02 22:48:56 INFO sparklyr: RScript (9197) finished apply 
    ## 17/06/02 22:48:56 INFO sparklyr: RScript (9197) finished 
    ## 17/06/02 22:48:56 INFO sparklyr: Worker (9197) R process completed

Finally, we disconnect:

``` r
spark_disconnect(sc)
```
