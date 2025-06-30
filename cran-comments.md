## Submission

- Restores support for Spark 2.4 with Scala 2.11 (#3485)

- Addresses changes in Spark 4.0 from when it was in preview to now.

- `ml_load()` now uses Spark's file read to obtain the metadata for the
models instead of R's file read. This approach accounts for when the
Spark Context is reading different mounted file protocols and mounted paths 
(#3478).

- Removes use of `%||%` in worker's R scripts to avoid reference error (#3487)

## Test environments

- Ubuntu 24.04, R 4.5.1, Spark 2.4 (GH Actions)
- Ubuntu 24.04, R 4.5.1, Spark 3.4 (GH Actions)
- Ubuntu 24.04, R 4.5.1, Spark 3.5 (GH Actions)
- Ubuntu 24.04, R 4.5.1, Spark 4.0 (GH Actions)

## R CMD check environments

- Mac OS aarch64-apple-darwin20, R 4.5.1
- Windows x86_64-w64-mingw32 (64-bit), R 4.5.1
- Linux x86_64-pc-linux-gnu, R 4.5.1
- Linux x86_64-pc-linux-gnu, R devel

## R CMD check results

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## revdepcheck results

We checked 29 reverse dependencies (28 from CRAN + 1 from Bioconductor), 
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 1 new problems
 * We failed to check 0 packages

Issues with CRAN packages are summarised below.

### New problems
(This reports the first line of each new failure)

* sparklyr.flint
  checking examples ... ERROR





