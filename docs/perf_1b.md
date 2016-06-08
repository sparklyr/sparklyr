RSpark Performance: 1B Rows
================

Setup
-----

``` r
library(rspark)
library(magrittr)

spark_install(version = "2.0.0-preview", reset = TRUE, logging = "WARN")
sc <- spark_connect(master = "local", version = "2.0.0-preview", memory = "12G ")

spark_conf <- function(scon, config, value) {
  spark_context(scon) %>%
    spark_invoke("conf") %>%
    spark_invoke("set", config, value)
}

spark_sum_range <- function(scon) {
  billion <- spark_invoke_static_ctor(sc, "java.math.BigInteger", "1000000000") %>%
    spark_invoke("longValue")
  
  ses <- spark_invoke_static_ctor(sc, "org.apache.spark.sql.SparkSession", spark_context(sc)) %>%
    spark_invoke("builder") %>%
    spark_invoke("master", "local") %>%
    spark_invoke("appName", "spark session example") %>%
    spark_invoke("getOrCreate")
  
  result <- ses %>%
    spark_invoke("range", as.integer(billion)) %>%
    spark_invoke("toDF", list("x")) %>%
    spark_invoke("selectExpr", list("sum(x)"))
  
  result
}
```

Spark 1.0
---------

``` r
system.time({
  spark_conf(sc, "spark.sql.codegen.wholeStage", "false")
  result <- spark_sum_range(sc)
  sum <- spark_invoke(result, "collect")[[1]]
})
```

    ##    user  system elapsed 
    ##   0.008   0.000  13.309

``` r
sum
```

    ## <jobj[17]>
    ##   class org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
    ##   [499999999500000000]

Spark 2.0
---------

``` r
system.time({
  spark_conf(sc, "spark.sql.codegen.wholeStage", "true")
  result <- spark_sum_range(sc)
  sum <- spark_invoke(result, "collect")[[1]]
})
```

    ##    user  system elapsed 
    ##   0.007   0.000   8.281

``` r
sum
```

    ## <jobj[30]>
    ##   class org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
    ##   [499999999500000000]

Cleanup
=======

``` r
spark_disconnect(sc)
```
