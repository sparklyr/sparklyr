# Submission

- Addresses Warning from CRAN checks

- Addresses option(stringsAsFactors) usage

- Fixes root cause of issue processing pivot wider and distinct (#3317 & #3320)


## Test environments

- Ubuntu 20.04, R 4.2.2, Spark 3.3 (GH Actions)
- Ubuntu 20.04, R 4.2.2, Spark 3.2 (GH Actions)
- Ubuntu 20.04, R 4.2.2, Spark 3.1 (GH Actions)
- Ubuntu 20.04, R 4.2.2, Spark 2.4 (GH Actions)
- Ubuntu 20.04, R 4.2.2, Spark 2.2 (GH Actions)
  
## R CMD check environments

- Local Mac OS M1 (aarch64-apple-darwin20), R 4.2.1
- Mac OS x86_64-apple-darwin17.0 (64-bit), R 4.2.2
- Windows  x86_64-w64-mingw32 (64-bit), R 4.2.2
- Linux x86_64-pc-linux-gnu (64-bit), R 4.2.2


## R CMD check results

0 errors ✔ | 0 warnings ✔ | 2 notes ✖

Notes:

```
❯ checking package dependencies ... NOTE
  Imports includes 28 non-default packages.
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

1 Warning in sparkwarc, not related sparklyr

|Package|Version|Error|Warning|Note|OK|
|:---|:---|:---|:---|:---|:---|
|[apache.sedona](#apache.sedona)|1.3.1|0|0|1|39|
|[catalog](#catalog)|0.1.1|0|0|0|42|
|[geospark](#geospark)|0.3.1|0|0|2|41|
|[graphframes](#graphframes)|0.1.2|0|0|1|41|
|[apache.sedona](#apache.sedona)|1.3.0|0|0|1|39|
|[catalog](#catalog)|0.1.1|0|0|0|42|
|[geospark](#geospark)|0.3.1|0|0|2|41|
|[graphframes](#graphframes)|0.1.2|0|0|1|41|
|[rsparkling](#rsparkling)|0.2.19|0|0|2|33|
|[s3.resourcer](#s3.resourcer)|1.1.0|0|0|0|41|
|[shinyML](#shinyML)|1.0.1|0|0|1|40|
|[spark.sas7bdat](#spark.sas7bdat)|1.4|0|0|0|46|
|[sparkavro](#sparkavro)|0.3.0|0|0|1|42|
|[sparkbq](#sparkbq)|0.1.1|0|0|1|40|
|[sparkhail](#sparkhail)|0.1.1|0|0|1|42|
|[sparklyr.flint](#sparklyr.flint)|0.2.2|0|0|0|46|
|[sparklyr.nested](#sparklyr.nested)|0.0.3|0|0|1|41|
|[sparktf](#sparktf)|0.1.0|0|0|1|42|
|[sparkwarc](#sparkwarc)|0.1.6|0|1|0|46|
|[sparkxgb](#sparkxgb)|0.1.1|0|0|1|41|
|[variantspark](#variantspark)|0.1.1|0|0|1|42|
|[rsparkling](#rsparkling)|0.2.19|0|0|2|33|
|[s3.resourcer](#s3.resourcer)|1.1.0|0|0|0|41|
|[shinyML](#shinyML)|1.0.1|0|0|1|40|
|[spark.sas7bdat](#spark.sas7bdat)|1.4|0|0|0|46|
|[sparkavro](#sparkavro)|0.3.0|0|0|1|42|
|[sparkbq](#sparkbq)|0.1.1|0|0|1|40|
|[sparkhail](#sparkhail)|0.1.1|0|0|1|42|
|[sparklyr.flint](#sparklyr.flint)|0.2.2|0|0|0|46|
|[sparklyr.nested](#sparklyr.nested)|0.0.4|0|0|0|41|
|[sparktf](#sparktf)|0.1.0|0|0|1|42|
|[sparkwarc](#sparkwarc)|0.1.6|0|0|0|47|
|[sparkxgb](#sparkxgb)|0.1.1|0|0|1|41|
|[variantspark](#variantspark)|0.1.1|0|0|1|42|

###  sparkwarc
```
* using log directory ‘/Users/edgar/r_projects/sparklyr/revdep/sparkwarc.Rcheck’
* using R version 4.2.1 (2022-06-23)
* using platform: aarch64-apple-darwin20 (64-bit)
* using session charset: UTF-8
* checking extension type ... Package
* this is package ‘sparkwarc’ version ‘0.1.6’
* package encoding: UTF-8
* checking whether package ‘sparkwarc’ can be installed ... WARNING
Found the following significant warnings:
  /Library/Frameworks/R.framework/Versions/4.2-arm64/Resources/library/Rcpp/include/Rcpp/internal/r_coerce.h:255:7: warning: 'sprintf' is deprecated: This function is provided for compatibility reasons only.  Due to security concerns inherent in the design of sprintf(3), it is highly recommended that you use snprintf(3) instead. [-Wdeprecated-declarations]
See ‘/Users/edgar/r_projects/sparklyr/revdep/sparkwarc.Rcheck/00install.out’ for details.
* DONE
Status: 1 WARNING
```

