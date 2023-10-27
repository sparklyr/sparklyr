## Reverse dependencies

|Package|Version|Error|Warning|Note|OK|
|:---|:---|:---|:---|:---|:---|
|[apache.sedona](#apache.sedona)|1.5.0|0|0|0|40|
|[catalog](#catalog)|0.1.1|0|0|0|42|
|[geospark](#geospark)|0.3.1|0|0|2|42|
|[graphframes](#graphframes)|0.1.2|0|0|1|43|
|[pysparklyr](#pysparklyr)|0.1.0|0|1|0|39|
|[rsparkling](#rsparkling)|0.2.19|0|0|2|34|
|[s3.resourcer](#s3.resourcer)|1.1.1|0|0|0|42|
|[shinyML](#shinyML)|1.0.1|0|0|1|40|
|[spark.sas7bdat](#spark.sas7bdat)|1.4|0|0|0|47|
|[sparkavro](#sparkavro)|0.3.0|0|0|1|43|
|[sparkbq](#sparkbq)|0.1.1|0|0|1|41|
|[sparkhail](#sparkhail)|0.1.1|0|0|1|43|
|[sparklyr.flint](#sparklyr.flint)|0.2.2|0|0|0|47|
|[sparklyr.nested](#sparklyr.nested)|0.0.4|0|0|0|43|
|[sparktf](#sparktf)|0.1.0|0|0|1|43|
|[sparkwarc](#sparkwarc)|0.1.6|0|0|1|48|
|[sparkxgb](#sparkxgb)|0.1.1|0|0|1|41|
|[variantspark](#variantspark)|0.1.1|0|0|1|43|

## Details
###  apache.sedona
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/apache.sedona.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘apache.sedona’ version ‘1.5.0’
* package encoding: UTF-8
* DONE
Status: OK
```

###  catalog
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/catalog.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘catalog’ version ‘0.1.1’
* package encoding: UTF-8
  Running ‘tinytest.R’
* DONE
Status: OK
```

###  geospark
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/geospark.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘geospark’ version ‘0.3.1’
* package encoding: UTF-8
* checking dependencies in R code ... NOTE
Namespace in Imports field not imported from: ‘dbplyr’
  All declared Imports should be used.
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
  Running ‘testthat.R’
* DONE
Status: 2 NOTEs
```

###  graphframes
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/graphframes.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘graphframes’ version ‘0.1.2’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
  Running ‘testthat.R’
* DONE
Status: 1 NOTE
```

###  pysparklyr
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/pysparklyr.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* this is package ‘pysparklyr’ version ‘0.1.0’
* package encoding: UTF-8
* checking S3 generic/method consistency ... WARNING
spark_connect_method:
  function(x, method, master, spark_home, config, app_name, version,
           extensions, scala_version, hadoop_version, ...)
spark_connect_method.spark_method_databricks_connect:
  function(x, method, master, spark_home, config, app_name, version,
           hadoop_version, extensions, scala_version, ...)

spark_connect_method:
  function(x, method, master, spark_home, config, app_name, version,
           extensions, scala_version, hadoop_version, ...)
spark_connect_method.spark_method_spark_connect:
  function(x, method, master, spark_home, config, app_name, version,
           hadoop_version, extensions, scala_version, ...)
See section ‘Generic functions and methods’ in the ‘Writing R
Extensions’ manual.
* checking examples ... NONE
  Running ‘testthat.R’
* DONE
Status: 1 WARNING
```

###  rsparkling
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/rsparkling.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* this is package ‘rsparkling’ version ‘0.2.19’
* package encoding: UTF-8
* checking dependencies in R code ... NOTE
Namespace in Imports field not imported from: ‘h2o’
  All declared Imports should be used.
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
* checking examples ... NONE
  Running ‘testthat.R’
* DONE
Status: 2 NOTEs
```

###  s3.resourcer
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/s3.resourcer.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘s3.resourcer’ version ‘1.1.1’
* package encoding: UTF-8
* checking examples ... NONE
  Running ‘testthat.R’
* DONE
Status: OK
```

###  shinyML
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/shinyML.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘shinyML’ version ‘1.0.1’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
* DONE
Status: 1 NOTE
```

###  spark.sas7bdat
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/spark.sas7bdat.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘spark.sas7bdat’ version ‘1.4’
* checking running R code from vignettes ... NONE
  ‘spark_sas7bdat_examples.Rmd’ using ‘UTF-8’... OK
* DONE
Status: OK
```

###  sparkavro
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkavro.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkavro’ version ‘0.3.0’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
  Running ‘testthat.R’
* DONE
Status: 1 NOTE
```

###  sparkbq
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkbq.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkbq’ version ‘0.1.1’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
* DONE
Status: 1 NOTE
```

###  sparkhail
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkhail.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkhail’ version ‘0.1.1’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
  Running ‘testthat.R’
* DONE
Status: 1 NOTE
```

###  sparklyr.flint
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparklyr.flint.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparklyr.flint’ version ‘0.2.2’
* package encoding: UTF-8
* checking running R code from vignettes ... NONE
  ‘importing-time-series-data.Rmd’ using ‘UTF-8’... OK
  ‘overview.Rmd’ using ‘UTF-8’... OK
  ‘working-with-time-series-rdd.Rmd’ using ‘UTF-8’... OK
* DONE
Status: OK
```

###  sparklyr.nested
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparklyr.nested.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* this is package ‘sparklyr.nested’ version ‘0.0.4’
* package encoding: UTF-8
  Running ‘testthat.R’
* DONE
Status: OK
```

###  sparktf
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparktf.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparktf’ version ‘0.1.0’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
  Running ‘testthat.R’
* DONE
Status: 1 NOTE
```

###  sparkwarc
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkwarc.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkwarc’ version ‘0.1.6’
* package encoding: UTF-8
* used C++ compiler: ‘Apple clang version 15.0.0 (clang-1500.0.40.1)’
* used SDK: ‘’
* checking C++ specification ... NOTE
  Specified C++11: please drop specification unless essential
* DONE
Status: 1 NOTE
```

###  sparkxgb
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkxgb.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkxgb’ version ‘0.1.1’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
* checking examples ... NONE
  Running ‘testthat.R’
* DONE
Status: 1 NOTE
```

###  variantspark
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/variantspark.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.0
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘variantspark’ version ‘0.1.1’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
  Running ‘testthat.R’
* DONE
Status: 1 NOTE
```

