# Submission

- Addresses issues with R 4.4.0. The root cause was that version checking functions
changed how the work. 
  - `package_version()` no longer accepts `numeric_version()` output. Wrapped
  the `package_version()` function to coerce the argument if it's a
  `numeric_version` class
  - Comparison operators (`<`, `>=`, etc.) for `packageVersion()` do no longer 
  accept numeric values. The changes were to pass the version as a character

- Added support for Databricks "autoloader" (format: `cloudFiles`) for streaming
ingestion of files(`stream_read_cloudfiles`):
  - `stream_write_table()`
  - `stream_read_table()`

## Test environments

- Ubuntu 22.04, R 4.4.0, Spark 3.3 (GH Actions)
- Ubuntu 22.04, R 4.4.0, Spark 3.2 (GH Actions)
- Ubuntu 22.04, R 4.4.0, Spark 3.1 (GH Actions)
- Ubuntu 22.04, R 4.4.0, Spark 2.4 (GH Actions)

## R CMD check environments

- Mac OS x86_64-apple-darwin20.0 (64-bit), R 4.4.0
- Windows  x86_64-w64-mingw32 (64-bit), R 4.4.0
- Linux x86_64-pc-linux-gnu (64-bit), R 4.4.0


## R CMD check results

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## revdepcheck results

We checked 28 reverse dependencies (27 from CRAN + 1 from Bioconductor), 
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 1 new problems
 * We failed to check 0 packages

Issues with CRAN packages are summarised below.

### New problems
(This reports the first line of each new failure)

* sparklyr.flint
  checking examples ... ERROR


