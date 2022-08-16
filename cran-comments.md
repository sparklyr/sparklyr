# Submission

## Release summary

### Misc

- Addresses new CRAN HTML check NOTEs. It also adds a new GHA action to run the 
  same checks to make sure we avoid new issues with this in the future.

### New features

- Adds new metric extraction functions: `ml_metrics_binary()`, 
  `ml_metrics_regression()` and `ml_metrics_multiclass()`. They work closer to 
  how `yardstick` metric extraction functions work. They expect a table with 
  the predictions and actual values, and returns a concise `tibble` with the 
  metrics. (#3281)
  
- Adds new `spark_insert_table()` function. This allows one to insert data into 
  an existing table definition without redefining the table, even when overwriting 
  the existing data. (#3272 @jimhester)

### Bug Fixes

- Restores "validator" functions to regression models. Removing them in a previous
  version broke `ml_cross_validator()` for regression models. (#3273)

### Spark 

- Adds support to Spark 3.3 local installation. This includes the ability to 
  enable and setup log4j version 2. (#3269)

- Updates the JSON file that `sparklyr` uses to find and download Spark for
  local use.  It is worth mentioning that starting with Spark 3.3, the Hadoop
  version number is no longer using a minor version for its download link. So, 
  instead of requesting 3.2, the version to request is 3. 

### Internal functionality

- Removes workaround for older versions of `arrow`. Bumps `arrow` version 
  dependency, from 0.14.0 to 0.17.0 (#3283 @nealrichardson)

- Removes code related to backwards compatibility with `dbplyr`. `sparklyr`
  requires `dbplyr` version 2.2.1 or above, so the code is no longer needed. 
  (#3277)

- Begins centralizing ML parameter validation into a single function that will
  run the proper `cast` function for each Spark parameter.  It also starts using
  S3 methods, instead of searching for a concatenated function name, to find the
  proper parameter validator.  Regression models are the first ones to use this
  new method. (#3279)

- `sparklyr` compilation routines have been improved and simplified.  
  `spark_compile()` now provides more informative output when used.  It also adds
  tests to compilation to make sure. It also adds a step to install Scala in the 
  corresponding GHAs. This is so that the new JAR build tests are able to run. 
  (#3275)

- Stops using package environment variables directly. Any package level variable
  will be handled by a `genv` prefixed function to set and retrieve values.  This
  avoids the risk of having the exact same variable initialized on more than on
  R script. (#3274)

- Adds more tests to improve coverage.


## Test environments

- Ubuntu 20.04, R 4.2.1, Spark 3.3 (GH Actions)
- Ubuntu 20.04, R 4.2.1, Spark 3.2 (GH Actions)
- Ubuntu 20.04, R 4.2.1, Spark 3.1 (GH Actions)
- Ubuntu 20.04, R 4.2.1, Spark 2.4 (GH Actions)
- Ubuntu 20.04, R 4.2.1, Spark 2.2 (GH Actions)
  
## R CMD check environments

- Local Mac OS M1 (aarch64-apple-darwin20), R 4.2.1
- Mac OS x86_64-apple-darwin17.0 (64-bit), R 4.2.1
- Windows  x86_64-w64-mingw32 (64-bit), R 4.2.1
- Linux x86_64-pc-linux-gnu (64-bit), R 4.2.1


## R CMD check results

0 errors ✓ | 0 warnings ✓ | 2 notes x

Notes:

```
❯ checking package dependencies ... NOTE
  Imports includes 28 non-default packages.
  Importing from so many packages makes the package vulnerable to any of
  them becoming unavailable.  Move as many as possible to Suggests and
  use conditionally.

❯ checking installed package size ... NOTE
    installed size is  6.7Mb
    sub-directories of 1Mb or more:
      R      2.0Mb
      java   3.4Mb

```

###Reverse dependencies

|Package|Version|Error|Warning|Note|OK|
|:---|:---|:---|:---|:---|:---|
|[apache.sedona](#apache.sedona)|1.1.1|0|0|1|39|
|[catalog](#catalog)|0.1.0|0|0|1|42|
|[geospark](#geospark)|0.3.1|0|0|2|41|
|[graphframes](#graphframes)|0.1.2|0|0|1|42|
|[apache.sedona](#apache.sedona)|1.1.1|0|0|1|39|
|[catalog](#catalog)|0.1.0|0|0|1|42|
|[geospark](#geospark)|0.3.1|0|0|2|41|
|[graphframes](#graphframes)|0.1.2|0|0|1|42|
|[rsparkling](#rsparkling)|0.2.19|0|0|2|33|
|[s3.resourcer](#s3.resourcer)|1.0.1|0|0|0|41|
|[shinyML](#shinyML)|1.0.1|0|0|1|40|
|[spark.sas7bdat](#spark.sas7bdat)|1.4|0|0|0|46|
|[sparkavro](#sparkavro)|0.3.0|0|0|1|42|
|[sparkbq](#sparkbq)|0.1.1|0|0|1|40|
|[sparkhail](#sparkhail)|0.1.1|0|0|1|42|
|[sparklyr.flint](#sparklyr.flint)|0.2.2|0|0|0|46|
|[sparklyr.nested](#sparklyr.nested)|0.0.3|0|0|1|41|
|[sparktf](#sparktf)|0.1.0|0|0|1|42|
|[sparkwarc](#sparkwarc)|0.1.6|0|0|0|47|
|[sparkxgb](#sparkxgb)|0.1.1|0|0|1|41|
|[variantspark](#variantspark)|0.1.1|0|0|1|42|
|[rsparkling](#rsparkling)|0.2.19|0|0|2|33|
|[s3.resourcer](#s3.resourcer)|1.0.1|0|0|0|41|
|[shinyML](#shinyML)|1.0.1|0|0|1|40|
|[spark.sas7bdat](#spark.sas7bdat)|1.4|0|0|0|46|
|[sparkavro](#sparkavro)|0.3.0|0|0|1|42|
|[sparkbq](#sparkbq)|0.1.1|0|0|1|40|
|[sparkhail](#sparkhail)|0.1.1|0|0|1|42|
|[sparklyr.flint](#sparklyr.flint)|0.2.2|0|0|0|46|
|[sparklyr.nested](#sparklyr.nested)|0.0.3|0|0|1|41|
|[sparktf](#sparktf)|0.1.0|0|0|1|42|
|[sparkwarc](#sparkwarc)|0.1.6|0|0|0|47|
|[sparkxgb](#sparkxgb)|0.1.1|0|0|1|41|
|[variantspark](#variantspark)|0.1.1|0|0|1|42|

