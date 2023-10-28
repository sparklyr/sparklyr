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

## revdepcheck results

We checked 27 reverse dependencies (26 from CRAN + 1 from Bioconductor), comparing
R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages

