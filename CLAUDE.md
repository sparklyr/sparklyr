# sparklyr — Claude Code guidance

## Release process

### 1. Update local R packages
Before starting, make sure all local packages are up to date to avoid masking
failures that will show up in CI:
```r
update.packages(ask = FALSE)
```

### 2. Run local tests and R CMD check
Confirm the package is in a green state before making any release changes:
```r
devtools::test()
devtools::check()
```
`devtools::test()` gives faster feedback on failures. `devtools::check()` must
be `0 errors | 0 warnings | 0 notes` before submitting to CRAN.

### 3. Update version in DESCRIPTION
Change `Version:` to the new release version (e.g. `1.9.4`). Remove the `.9000` dev suffix.

### 4. Update NEWS.md
- Move any items sitting under `# Sparklyr (dev)` into the new release section
- Add a new `# Sparklyr (dev)` header at the top (empty, for the next cycle)
- The top header should always be the release version at submission time

### 5. Run reverse dependency checks
```r
revdepcheck::revdep_reset()
revdepcheck::revdep_check(num_workers = 4)
```
Results are written to `revdep/README.md`. Check for new problems (pre-existing
errors in both old and new are not regressions).

### 6. Submit to win-builder
```r
devtools::check_win_devel()
```
Results arrive at edgar@rstudio.com in 15–30 minutes. Address any new NOTEs/WARNINGs.

### 7. Prepare cran-comments.md
Populate the three sections using:

- **Test environments** (Spark CI jobs): `cranjobs::github_action_run("Spark-Tests")`
- **R CMD check environments**: `cranjobs::github_action_run()`
- **R CMD check results**: from `devtools::check()` above
- **revdepcheck results**: from `revdep/README.md` — number of packages checked, 0 new problems, 0 failed

Template:
```markdown
## Submission

- <bullet points matching NEWS.md entries for this release>

## Test environments

- <output of cranjobs::github_action_run("Spark-Tests")>

## R CMD check environments

- <output of cranjobs::github_action_run()>

## R CMD check results

0 errors ✔ | 0 warnings ✔ | 0 notes ✔

## revdepcheck results

We checked N reverse dependencies (N from CRAN + N from Bioconductor),
comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages
```

### 8. Submit to CRAN
```r
devtools::submit_cran()
```

## Common issues to watch for

- **`.claude` directory and `CLAUDE.md` in package**: ensure `^\.claude$` and `^CLAUDE\.md$` are in `.Rbuildignore`
- **Undocumented exported functions**: add a roxygen title + `@keywords internal` to stub methods
- **Broken URLs in docs**: R CMD check on win-builder validates URLs; fix dead links before submission
- **revdep previous run exists**: always call `revdep_reset()` before `revdep_check()`
