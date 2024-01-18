# Submission

- Removes Scala code, and JARs for Spark versions 2.3 and below. Spark 2.3 is 
no longer considered maintained as of September 2019
  - Removes Java folder for versions 2.3 and below
  - Merges Scala file sets into Spark version 2.4
  - Re-compiles JARs for version 2.4 and above

- Removes dependency on `tibble` 
  - All calls to `tibble()` are now redirected to `dplyr`

- Removes dependency on `rapddirs`: 
  - Deprecating backwards compatibility with `sparklyr` 0.5 is no longer needed
  - Replicates selection of cache directory 

- Updates Delta-to-Spark version matching when using `delta` as one of the
`packages` when connecting

## Test environments

- Ubuntu 22.04, R 4.3.2, Spark 3.3 (GH Actions)
- Ubuntu 22.04, R 4.3.2, Spark 3.2 (GH Actions)
- Ubuntu 22.04, R 4.3.2, Spark 2.4 (GH Actions)

## R CMD check environments

- Local Mac OS M1 (aarch64-apple-darwin20), R 4.3.2
- Mac OS x86_64-apple-darwin17.0 (64-bit), R 4.3.2
- Windows  x86_64-w64-mingw32 (64-bit), R 4.3.2
- Linux x86_64-pc-linux-gnu (64-bit), R 4.3.2


## R CMD check results

0 errors ✔ | 0 warnings ✔ | 2 notes ✖

Notes:

```
❯ checking installed package size ... NOTE
    installed size is  5.1Mb
    sub-directories of 1Mb or more:
      R      2.1Mb
      java   1.8Mb

0 errors ✔ | 0 warnings ✔ | 1 note ✖
```

## revdepcheck results

We checked 27 reverse dependencies (26 from CRAN + 1 from Bioconductor), comparing
R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages

