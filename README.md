sparkworker: R Worker for Apache Spark
================

``` r
library(sparkworker)
library(sparklyr)

sc <- spark_connect(master = "local", version = "2.0.1")
iris_tbl <- copy_to(sc, iris)

spark_lapply(iris_tbl, function(rows) {
  rows$Petal_Width <- rows$Petal_Width + 1
  rows
})
```

    ## Source:     table<sparklyr_tmp_e8341338123e> [?? x 5]
    ## Database:   spark connection master=local[8] app=sparklyr local=TRUE
    ## 
    ## # ... with 5 variables: Sepal_Length <dbl>, Sepal_Width <dbl>,
    ## #   Petal_Length <dbl>, Petal_Width <dbl>, Species <chr>

``` r
spark_disconnect(sc)
```
