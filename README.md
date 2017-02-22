sparkworker: R Worker for Apache Spark
================

``` r
library(sparkworker)
library(sparklyr)

sc <- spark_connect(master = "local", version = "2.0.1")
spark_run(sc)
```

    ## NULL

``` r
spark_disconnect(sc)
```
