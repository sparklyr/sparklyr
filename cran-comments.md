# Submission

- Addresses issues found by CRAN checks. Fixes `db_connection_describe()` 
S3 consistency error 

- Addresses new error from `dbplyr` that fails when you try to access 
components from a remote `tbl` using `$`

- Bumps the version of `dbplyr` to switch between the two methods to create
temporary tables 

- Addresses new `translate_sql()` hard requirement to pass a `con` object. Done
by passing the current connection or `simulate_hive()` 

- Removes dependency on the following packages:
  - `digest`
  - `base64enc`
  - `ellipsis`
  
- Converts `ml_fit()` into a S3 method for `pysparklyr` compatibility

## Test environments

- Ubuntu 22.04, R 4.3.1, Spark 3.3 (GH Actions)
- Ubuntu 22.04, R 4.3.1, Spark 3.2 (GH Actions)
- Ubuntu 22.04, R 4.3.1, Spark 2.4 (GH Actions)

## R CMD check environments

- Local Mac OS M1 (aarch64-apple-darwin20), R 4.3.1
- Mac OS x86_64-apple-darwin17.0 (64-bit), R 4.3.1
- Windows  x86_64-w64-mingw32 (64-bit), R 4.3.1
- Linux x86_64-pc-linux-gnu (64-bit), R 4.3.1


## R CMD check results

0 errors ✔ | 0 warnings ✔ | 2 notes ✖

Notes:

```
❯ checking package dependencies ... NOTE
  Imports includes 21 non-default packages.
  Importing from so many packages makes the package vulnerable to any of
  them becoming unavailable.  Move as many as possible to Suggests and
  use conditionally.

❯ checking installed package size ... NOTE
    installed size is  7.1Mb
    sub-directories of 1Mb or more:
      R      2.0Mb
      java   3.8Mb

0 errors ✔ | 0 warnings ✔ | 2 notes ✖
```

## Reverse dependencies

|Package|Version|Error|Warning|Note|OK|
|:---|:---|:---|:---|:---|:---|
|[apache.sedona](#apache.sedona)|1.4.1|0|0|0|40|
|[catalog](#catalog)|0.1.1|0|0|0|42|
|[geospark](#geospark)|0.3.1|0|0|2|42|
|[graphframes](#graphframes)|0.1.2|0|0|1|43|
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
