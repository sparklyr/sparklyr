# Submission

- Adds support for Spark 4 and Scala 2.13 (#3479):

  - Adds new Java folder for Spark 4.0.0 with updated code
  
  - Adds new JAR file to handle Spark 4+
  
  - Updates to different spots in the R code to start handling version 4, 
  as well as releases marked as "preview" by the Spark project
  
  - Removes JARs using Scala 2.11

- Updates the Spark versions to use for CI

- `sdf_sql()` now returns nothing, including an error, when the query outputs an
empty dataset (#3439)

## Test environments

- Ubuntu 24.04, R 4.4.3, Spark 3.4 (GH Actions)
- Ubuntu 24.04, R 4.4.3, Spark 3.5 (GH Actions)
- Ubuntu 24.04, R 4.4.3, Spark 4.0 (GH Actions)

## R CMD check environments

- Mac OS aarch64-apple-darwin20, R 4.4.3
- Windows x86_64-w64-mingw32 (64-bit), R 4.4.3
- Linux x86_64-pc-linux-gnu, R 4.4.3
- Linux x86_64-pc-linux-gnu, R devel

## R CMD check results

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## revdepcheck results

We checked 30 reverse dependencies (29 from CRAN + 1 from Bioconductor), 
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 1 problem
 * We failed to check 0 packages

Issues with CRAN packages are summarised below.

### Problems
(This reports the first line of each new failure)

* sparklyr.flint
  checking examples ... ERROR



