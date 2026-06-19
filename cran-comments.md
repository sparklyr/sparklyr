## Submission

- Restores compatibility with dbplyr (>= 2.6.0), which changed the Hive
backend to quote identifiers with ANSI double-quotes and rebuilt the lazy
table `src`. `sparklyr` now renders SQL fragments through the Spark SQL
backend (`dbplyr::simulate_spark_sql()`) and retrieves the connection from a
lazy table via `dbplyr::remote_con()`.

- `spark_install()` adds a second attempt at downloading from the Spark
archive if the link from the primary download location fails.

## Test environments

- Spark 3.5: Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.6.0 (2026-04-24)
- Spark 4.1: Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.6.0 (2026-04-24)
- Spark 4.1 w Arrow: Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.6.0 (2026-04-24)

## R CMD check environments

- Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.6.0 (2026-04-24)
- Windows Server 2022 x64 (build 26100) (x86_64, mingw32), R version 4.6.0 (2026-04-24 ucrt)
- macOS Sequoia 15.7.7 (aarch64, darwin23), R version 4.6.0 (2026-04-24)
- Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R Under development (unstable) (2026-06-17 r90169)

## R CMD check results

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## revdepcheck results

We checked 34 reverse dependencies (33 from CRAN + 1 from Bioconductor),
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages
