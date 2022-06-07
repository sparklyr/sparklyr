## Patch Re-submission

### Release summary

- Restores missing 'java' folder 
  
### Test environments

- Ubuntu 20.04, R 4.2.0, Spark 3.2 (GH Actions)
- Ubuntu 20.04, R 4.2.0, Spark 3.1 (GH Actions)
- Ubuntu 20.04, R 4.2.0, Spark 2.4 (GH Actions)
- Ubuntu 20.04, R 4.2.0, Spark 2.2 (GH Actions)
  
### R CMD check environments

- Local Mac OS M1 (aarch64-apple-darwin20), R 4.2.0
- Mac OS x86_64-apple-darwin17.0 (64-bit), R 4.2.0
- Windows  x86_64-w64-mingw32 (64-bit), R 4.2.0
- Linux x86_64-pc-linux-gnu (64-bit), R 4.2.0


### R CMD check results

0 errors ✓ | 0 warnings ✓ | 2 notes x

Notes:

```
❯ checking package dependencies ... NOTE
  Imports includes 29 non-default packages.
  Importing from so many packages makes the package vulnerable to any of
  them becoming unavailable.  Move as many as possible to Suggests and
  use conditionally.

❯ checking installed package size ... NOTE
    installed size is  6.7Mb
    sub-directories of 1Mb or more:
      R      2.0Mb
      java   3.4Mb
```

### Reverse dependencies

|Package|Version|Error|Warning|Note|OK|
|:---|:---|:---|:---|:---|:---|
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
