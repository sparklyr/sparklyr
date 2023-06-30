# Submission

- Fix various rlang deprecation warnings (@mgirlich, #3333).

- Adds Azure Synapse Analytics connectivity (@Bob-Chou , #3336)

- Adds support for "parameterized" queries now available in Spark 3.4 (@gregleleu #3335)

- Adds new DBI methods: `dbValid` and `dbDisconnect` (@alibell, #3296)

- Adds `overwrite` parameter to `dbWriteTable()` (@alibell, #3296)

- Adds `database` parameter to `dbListTables()` (@alibell, #3296)

- Fixes Spark download locations (#3331)

- Switches upper version of Spark to 3.4, and updates JARS (#3334)

- Adds ability to turn off predicate support (where(), across()) using 
  options("sparklyr.support.predicates" = FALSE). Defaults to TRUE. This should
  accelerate `dplyr` commands because it won't need to process column types
  for every single piped command
  
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
  Packages suggested but not available for checking:
    'janeaustenr', 'Lahman', 'mlbench', 'RCurl', 'reshape2'
  
  Imports includes 24 non-default packages.
  Importing from so many packages makes the package vulnerable to any of
  them becoming unavailable.  Move as many as possible to Suggests and
  use conditionally.

❯ checking installed package size ... NOTE
    installed size is  6.8Mb
    sub-directories of 1Mb or more:
      R      2.1Mb
      java   3.4Mb
```

## Reverse dependencies

1 Warning in sparkwarc, not related sparklyr]

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


###  sparkwarc
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkwarc.Rcheck’
* using R version 4.3.1 (2023-06-16)
* using platform: aarch64-apple-darwin20 (64-bit)
* R was compiled by
    Apple clang version 14.0.0 (clang-1400.0.29.202)
    GNU Fortran (GCC) 12.2.0
* running under: macOS Ventura 13.4.1
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkwarc’ version ‘0.1.6’
* package encoding: UTF-8
* used C++ compiler: ‘Apple clang version 14.0.3 (clang-1403.0.22.14.1)’
* used SDK: ‘MacOSX13.3.sdk’
* checking C++ specification ... NOTE
  Specified C++11: please drop specification unless essential
* DONE
Status: 1 NOTE
```
