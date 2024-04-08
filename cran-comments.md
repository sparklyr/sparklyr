# Submission

- The main motivation of the timing for this release is to address an bug in
translating SQL introduced when 'dbplyr' was upgraded to 2.5.0.

- There are two other noteworthy improvements in this release:
  - After constantly cleaning up dependencies across multiple releases, and after
  removing two additional Imports, R checks do not longer return a NOTE warning
  of too many dependencies.
  - Started an effort to reduce size of the package itself. This is the reason
  for the remaining NOTE in the package. The major reduction in size, in this 
  release, was from removing support for Spark 1.6 (No longer supported by the
  Spark project itself). This reduced the size of that directory almost 2
  megabytes.

- Was able to run reverse dependency checks for all packages but 1. 'pathling' 
timed out when I attempted to run the checks. It seems that the package is trying
to start a Spark session, and it has a configuration that does not seem to 
be able to run on my machine. Judging by the types of changes I made in this release,
I doubt that checks would fail in CRAN, so this is why I decided to submit
despite this failure. 

## Test environments

- Ubuntu 22.04, R 4.3.3, Spark 3.3 (GH Actions)
- Ubuntu 22.04, R 4.3.3, Spark 3.2 (GH Actions)
- Ubuntu 22.04, R 4.3.3, Spark 3.1 (GH Actions)
- Ubuntu 22.04, R 4.3.3, Spark 2.4 (GH Actions)

## R CMD check environments

- Local Mac OS M1 (aarch64-apple-darwin20), R 4.3.3
- Mac OS x86_64-apple-darwin20.0 (64-bit), R 4.3.3
- Windows  x86_64-w64-mingw32 (64-bit), R 4.3.3
- Linux x86_64-pc-linux-gnu (64-bit), R 4.3.3


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

We checked 28 reverse dependencies (27 from CRAN + 1 from Bioconductor), 
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 1 packages

Issues with CRAN packages are summarised below.

### Failed to check

* pathling (NA)


