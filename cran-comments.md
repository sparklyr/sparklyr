## Submission

- Adds `tune_grid_spark()` method in order for `pysparklyr` to provide the
functionality

- Fixes `sdf_bind_rows()` `id` argument incorrectly overwriting named arguments
when mixed named/unnamed inputs were provided

## Test environments

- Spark 3.5: Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.5.3 (2026-03-11)
- Spark 4.1: Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.5.3 (2026-03-11)
- Spark 4.1 w Arrow: Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.5.3 (2026-03-11)

## R CMD check environments

- Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R version 4.5.3 (2026-03-11)
- Ubuntu 24.04.4 LTS (x86_64, linux-gnu), R Under development (unstable) (2026-04-15 r89888)
- macOS Sequoia 15.7.4 (aarch64, darwin20), R version 4.5.3 (2026-03-11)
- Windows Server 2022 x64 (build 26100) (x86_64, mingw32), R version 4.5.3 (2026-03-11 ucrt)

## R CMD check results

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## revdepcheck results

We checked 33 reverse dependencies (32 from CRAN + 1 from Bioconductor),
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages
