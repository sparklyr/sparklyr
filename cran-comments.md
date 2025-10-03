## Submission

- Avoids the cross-wire when pulling an object from a lazy table instead of pulling
a field (#3494)

- Converts spark_write_delta() to method 

- In simulate_vars_spark(), it avoids calling a function named 'unknown' in
case a package has added such a function name in its environment (#3497)

## Test environments

- Spark 4.0: Ubuntu 24.04.3 LTS (x86_64, linux-gnu), R version 4.5.1 (2025-06-13)
- Spark 4.0 w Arrow: Ubuntu 24.04.3 LTS (x86_64, linux-gnu), R version 4.5.1 (2025-06-13)
- Spark 3.5: Ubuntu 24.04.3 LTS (x86_64, linux-gnu), R version 4.5.1 (2025-06-13)

## R CMD check environments

- Ubuntu 24.04.3 LTS (x86_64, linux-gnu), R version 4.5.1 (2025-06-13)
- Windows Server 2022 x64 (build 26100) (x86_64, mingw32), R version 4.5.1 (2025-06-13 ucrt)
- Ubuntu 24.04.3 LTS (x86_64, linux-gnu), R Under development (unstable) (2025-10-02 r88897)
- macOS Sequoia 15.6.1 (aarch64, darwin20), R version 4.5.1 (2025-06-13)

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
