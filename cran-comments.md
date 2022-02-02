## Re-submission

- Addresses pre-check NOTE by adding cran-comments.md to the build ignore file

### Release summary

- Addresses both CRAN Check Results warnings:
  - Un-exported object `rland::is_env()`
  - `pivot_wider()` S3 consistency  issue
  
### Test environments

- Local Mac OS M1 (aarch64-apple-darwin20), R 4.1.2
- Ubuntu 20, R 4.0.5 (GH Actions)

### R CMD check results

0 errors ✓ | 0 warnings ✓ | 2 notes x

Notes:

```
> checking package dependencies ... NOTE
  Imports includes 31 non-default packages.
```

```
> checking installed package size ... NOTE
    installed size is  6.7Mb
    sub-directories of 1Mb or more:
      R      2.1Mb
      java   3.4Mb
```
