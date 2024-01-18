## Reverse dependencies

| Package                             | Version | Error | Warning | Note | OK  |
|:------------------------------------|:--------|:------|:--------|:-----|:----|
| [apache.sedona](#apache.sedona)     | 1.5.1   | 0     | 0       | 0    | 41  |
| [catalog](#catalog)                 | 0.1.1   | 0     | 0       | 0    | 42  |
| [geospark](#geospark)               | 0.3.1   | 0     | 0       | 2    | 42  |
| [graphframes](#graphframes)         | 0.1.2   | 0     | 0       | 1    | 43  |
| [pathling](#pathling)               | 6.4.2   | 0     | 0       | 0    | 45  |
| [pysparklyr](#pysparklyr)           | 0.1.3   | 0     | 0       | 0    | 41  |
| [rsparkling](#rsparkling)           | 0.2.19  | 0     | 0       | 2    | 34  |
| [s3.resourcer](#s3.resourcer)       | 1.1.1   | 0     | 0       | 0    | 42  |
| [shinyML](#shinyML)                 | 1.0.1   | 0     | 0       | 1    | 40  |
| [spark.sas7bdat](#spark.sas7bdat)   | 1.4     | 0     | 0       | 0    | 47  |
| [sparkavro](#sparkavro)             | 0.3.0   | 0     | 0       | 1    | 43  |
| [sparkbq](#sparkbq)                 | 0.1.1   | 0     | 0       | 1    | 41  |
| [sparkhail](#sparkhail)             | 0.1.1   | 0     | 0       | 1    | 43  |
| [sparklyr.flint](#sparklyr.flint)   | 0.2.2   | 1     | 0       | 0    | 46  |
| [sparklyr.nested](#sparklyr.nested) | 0.0.4   | 0     | 0       | 0    | 43  |
| [sparktf](#sparktf)                 | 0.1.0   | 0     | 0       | 1    | 43  |
| [sparkwarc](#sparkwarc)             | 0.1.6   | 0     | 0       | 1    | 48  |
| [sparkxgb](#sparkxgb)               | 0.1.1   | 0     | 0       | 1    | 42  |
| [variantspark](#variantspark)       | 0.1.1   | 0     | 0       | 1    | 43  |

## Details

### apache.sedona {#apache.sedona}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/apache.sedona.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘apache.sedona’ version ‘1.5.1’
* package encoding: UTF-8
* DONE
Status: OK
```

### catalog {#catalog}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/catalog.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘catalog’ version ‘0.1.1’
* package encoding: UTF-8
  Running ‘tinytest.R’
* DONE
Status: OK
```

### geospark {#geospark}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/geospark.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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

### graphframes {#graphframes}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/graphframes.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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

### pathling {#pathling}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/pathling.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘pathling’ version ‘6.4.2’
* package encoding: UTF-8
Examples with CPU (user + system) or elapsed time > 5s
                           user system elapsed
pathling_encode_bundle    0.386  0.064  11.793
pathling_connect          0.325  0.116  11.185
pathling_read_datasets    0.365  0.065  11.348
pathling_encode           0.361  0.064  11.684
pathling_example_resource 0.345  0.060  10.843
tx_subsumes               0.322  0.068  17.587
tx_designation            0.321  0.064  19.180
tx_subsumed_by            0.303  0.063  17.825
tx_translate              0.301  0.063  18.645
tx_display                0.297  0.066  18.386
tx_property_of            0.298  0.063  18.193
ds_aggregate              0.260  0.085  12.618
tx_to_loinc_coding        0.283  0.062   9.318
tx_to_coding              0.281  0.063   9.319
tx_to_snomed_coding       0.266  0.053   9.388
ds_read                   0.257  0.061   8.991
pathling_read_ndjson      0.258  0.060   8.575
ds_extract                0.243  0.063  11.043
pathling_read_delta       0.225  0.052  11.494
pathling_read_bundles     0.220  0.050   9.147
pathling_read_parquet     0.192  0.054   7.952
pathling_read_tables      0.186  0.051   9.459
ds_write_delta            0.169  0.058  16.319
ds_write_tables           0.174  0.049  25.997
ds_write_ndjson           0.157  0.056  10.733
ds_write_parquet          0.158  0.049  10.118
* DONE
Status: OK
```

### pysparklyr {#pysparklyr}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/pysparklyr.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* this is package ‘pysparklyr’ version ‘0.1.3’
* package encoding: UTF-8
* checking examples ... NONE
  Running ‘testthat.R’
* DONE
Status: OK
```

### rsparkling {#rsparkling}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/rsparkling.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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

### s3.resourcer {#s3.resourcer}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/s3.resourcer.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘s3.resourcer’ version ‘1.1.1’
* package encoding: UTF-8
* checking examples ... NONE
  Running ‘testthat.R’
* DONE
Status: OK
```

### shinyML {#shinyml}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/shinyML.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘shinyML’ version ‘1.0.1’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
* DONE
Status: 1 NOTE
```

### spark.sas7bdat {#spark.sas7bdat}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/spark.sas7bdat.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘spark.sas7bdat’ version ‘1.4’
* checking running R code from vignettes ... NONE
  ‘spark_sas7bdat_examples.Rmd’ using ‘UTF-8’... OK
* DONE
Status: OK
```

### sparkavro {#sparkavro}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkavro.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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

### sparkbq {#sparkbq}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkbq.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkbq’ version ‘0.1.1’
* package encoding: UTF-8
* checking LazyData ... NOTE
  'LazyData' is specified without a 'data' directory
* DONE
Status: 1 NOTE
```

### sparkhail {#sparkhail}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkhail.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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

### sparklyr.flint {#sparklyr.flint}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparklyr.flint.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparklyr.flint’ version ‘0.2.2’
* package encoding: UTF-8
* checking examples ... ERROR
Running examples in ‘sparklyr.flint-Ex.R’ failed
The error most likely occurred in:

> base::assign(".ptime", proc.time(), pos = "CheckExEnv")
> ### Name: asof_future_left_join
> ### Title: Temporal future left join
> ### Aliases: asof_future_left_join
> 
> ### ** Examples
> 
> 
> library(sparklyr)

Attaching package: ‘sparklyr’

The following object is masked from ‘package:stats’:

    filter

> library(sparklyr.flint)
> 
> sc <- try_spark_connect(master = "local")
> if (!is.null(sc)) {
+   ts_1 <- copy_to(sc, tibble::tibble(t = seq(10), u = seq(10))) %>%
+     from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
+   ts_2 <- copy_to(sc, tibble::tibble(t = seq(10) + 1, v = seq(10) + 1L)) %>%
+     from_sdf(is_sorted = TRUE, time_unit = "SECONDS", time_column = "t")
+   future_left_join_ts <- asof_future_left_join(ts_1, ts_2, tol = "1s")
+ } else {
+   message("Unable to establish a Spark connection!")
+ }
Error:
! java.lang.AbstractMethodError: Receiver class
  org.apache.spark.sql.NanosToTimestamp does not define or inherit an
  implementation of the resolved method 'abstract
  org.apache.spark.sql.catalyst.trees.TreeNode child()' of interface
  org.apache.spark.sql.catalyst.trees.UnaryLike.

Run ]8;;x-r-run:sparklyr::spark_last_error()`sparklyr::spark_last_error()`]8;; to see the full Spark error (multiple lines)
To use the previous style of error message set
`options("sparklyr.simple.errors" = TRUE)`
Backtrace:
     ▆
  1. ├─... %>% ...
  2. └─sparklyr.flint::from_sdf(...)
  3.   └─builder$fromSDF(sdf)
  4.     ├─sparklyr.flint:::new_ts_rdd(invoke(builder, "fromDF", spark_dataframe(sdf)))
  5.     │ └─base::structure(list(.jobj = jobj), class = "ts_rdd")
  6.     ├─sparklyr::invoke(builder, "fromDF", spark_dataframe(sdf))
  7.     └─sparklyr:::invoke.shell_jobj(builder, "fromDF", spark_dataframe(sdf))
  8.       ├─sparklyr::invoke_method(...)
  9.       └─sparklyr:::invoke_method.spark_shell_connection(...)
 10.         └─sparklyr:::core_invoke_method(...)
 11.           └─sparklyr:::core_invoke_method_impl(...)
 12.             └─sparklyr:::spark_error(msg)
 13.               └─rlang::abort(message = msg, use_cli_format = TRUE, call = NULL)
Execution halted
* checking running R code from vignettes ... NONE
  ‘importing-time-series-data.Rmd’ using ‘UTF-8’... OK
  ‘overview.Rmd’ using ‘UTF-8’... OK
  ‘working-with-time-series-rdd.Rmd’ using ‘UTF-8’... OK
* DONE
Status: 1 ERROR
```

### sparklyr.nested {#sparklyr.nested}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparklyr.nested.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* this is package ‘sparklyr.nested’ version ‘0.0.4’
* package encoding: UTF-8
  Running ‘testthat.R’
* DONE
Status: OK
```

### sparktf {#sparktf}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparktf.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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

### sparkwarc {#sparkwarc}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkwarc.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkwarc’ version ‘0.1.6’
* package encoding: UTF-8
* used C++ compiler: ‘Apple clang version 15.0.0 (clang-1500.0.40.1)’
* used SDK: ‘MacOSX14.2.sdk’
* checking C++ specification ... NOTE
  Specified C++11: please drop specification unless essential
* DONE
Status: 1 NOTE
```

### sparkxgb {#sparkxgb}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkxgb.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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

### variantspark {#variantspark}

```         
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/variantspark.Rcheck’
* using R version 4.3.2 (2023-10-31)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Sonoma 14.2.1
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