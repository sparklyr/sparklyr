# Submission

- Spark error message relays are now cached instead of the entire content
displayed as an R error. This used to overwhelm the interactive session's 
console or Notebook, because of the amount of lines returned by the
Spark message.  Now, by default, it will return the top of the Spark 
error message, which is typically the most relevant part. The full error can
still be accessed using a new function called `spark_last_error()`

- Reduces redundancy on several tests

- Handles SQL quoting when the table reference contains multiple levels. The 
common time someone would encounter an issue is when a table name is passed
using `in_catalog()`, or `in_schema()`. 

- Adds Scala scripts to handle changes in the upcoming version of Spark (3.5)

- Adds new JAR file to handle Spark 3.0 to 3.4

- Adds new JAR file to handle Spark 3.5 and above

- It prevents an error when `na.rm = TRUE` is explicitly set within `pmax()` and
`pmin()`. It will now also purposely fail if `na.rm` is set to `FALSE`. The
default of these functions in base R is for `na.rm` to be `FALSE`, but ever
since these functions were released, there has been no warning or error. For now,
we will keep that behavior until a better approach can be figured out. (#3353)

- `spark_install()` will now properly match when a partial version is passed 
to the function. The issue was that passing '2.3' would match to '3.2.3', instead
of '2.3.x' (#3370)

- Adds functionality to allow other packages to provide `sparklyr` additional 
back-ends. This effort is mainly focused on adding the ability to integrate 
with Spark Connect and Databricks Connect through a new package. 

- New exported functions to integrate with the RStudio IDE. They all have the
same `spark_ide_` prefix

- Modifies several read functions to become exported methods, such as 
`sdf_read_column()`. 

- Adds `spark_integ_test_skip()` function. This is to allow other packages 
to use `sparklyr`'s test suite. It enables a way to the external package to 
indicate if a given test should run or be skipped.

- If installed, `sparklyr` will load the `pysparklyr` package

## Test environments

- Ubuntu 22.04, R 4.3.1, Spark 3.3 (GH Actions)
- Ubuntu 22.04, R 4.3.1, Spark 3.2 (GH Actions)
- Ubuntu 22.04, R 4.3.1, Spark 3.1 (GH Actions)
- Ubuntu 22.04, R 4.3.1, Spark 2.4 (GH Actions)
- Ubuntu 22.04, R 4.3.1, Spark 2.2 (GH Actions)
  
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
  Imports includes 24 non-default packages.
  Importing from so many packages makes the package vulnerable to any of
  them becoming unavailable.  Move as many as possible to Suggests and
  use conditionally.

❯ checking installed package size ... NOTE
    installed size is  7.1Mb
    sub-directories of 1Mb or more:
      R      2.0Mb
      java   3.8Mb
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
