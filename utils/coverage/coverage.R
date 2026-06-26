# Single-file coverage helpers
#
# Source this file (from anywhere — paths resolve via here::here()), then call
# the functions at will:
#
#   source("utils/coverage/coverage.R")
#   coverage_lines("dplyr_verbs")     # opens the HTML report AND prints the
#                                     #   covered/uncovered text annotation, both
#                                     #   from the same run (browse = FALSE for
#                                     #   text only)
#   coverage_report("dplyr_verbs")    # interactive HTML report only
#   coverage_summary("dplyr_verbs")   # one-line % summary
#
# All share an in-session cache, so the (slow) instrumentation runs only once per
# file. Pass `refresh = TRUE` to force a re-run, or `spark_version=` to test
# against a different Spark.
#
# Why this dance: `covr::file_coverage()` is faster but does not load the
# testthat helper-*/setup-* files, so tests that need `testthat_spark_connection()`
# etc. fail. `covr::package_coverage()` with `load_package = "installed"` runs the
# real test harness against an instrumented install, which is the only reliable
# path for this package.

library(covr)
library(withr)
library(here)   # non-package util, so here::here() is fair game for path resolution

# ---------------------- Internal ----------------

# In-session cache: file base name -> coverage object
.coverage_cache <- new.env(parent = emptyenv())

## -------- Run (or fetch cached) coverage for one R script
coverage_run <- function(file,
                         spark_version = "4.1.0",
                         refresh = FALSE) {
  file <- sub("[.]R$", "", basename(file))      # accept "dplyr_verbs" or "R/dplyr_verbs.R"

  if (!refresh && !is.null(.coverage_cache[[file]])) {
    return(.coverage_cache[[file]])
  }

  test_dir <- here::here("tests", "testthat")
  code <- c(
    "library(testthat)",
    "library(sparklyr)",
    sprintf(
      'testthat::test_dir("%s", filter = "%s", package = "sparklyr", load_package = "installed")',
      test_dir, file
    )
  )

  cov <- withr::with_envvar(
    c(CODE_COVERAGE = "true", SPARK_VERSION = spark_version),
    covr::package_coverage(type = "none", code = code)
  )

  .coverage_cache[[file]] <- cov
  cov
}

## -------- Per-line table (line, hits) for just the target file
.coverage_table <- function(file, cov) {
  tc <- covr::tally_coverage(cov)
  pat <- paste0(sub("[.]R$", "", basename(file)), "[.]R$")
  dv <- tc[grepl(pat, tc$filename), c("line", "value")]
  dv[order(dv$line), ]
}

# ---------------------- Public ----------------

## -------- One-line summary
coverage_summary <- function(file, spark_version = "4.1.0", refresh = FALSE) {
  cov <- coverage_run(file, spark_version, refresh)
  dv <- .coverage_table(file, cov)
  covered <- sum(dv$value > 0)
  cat(sprintf(
    "R/%s.R: %.1f%% | %d executable lines | %d covered | %d NOT covered\n",
    sub("[.]R$", "", basename(file)),
    100 * covered / nrow(dv), nrow(dv), covered, nrow(dv) - covered
  ))
  invisible(dv)
}

## -------- Text annotation of covered/uncovered lines (easy to read in a terminal/log)
##   show = "uncovered" (default) prints only uncovered blocks with `context` lines
##          around them; show = "all" prints the whole file annotated.
##   browse = TRUE also opens the interactive HTML report in the browser, so the
##          text output and the visual report come from the same (cached) run.
coverage_lines <- function(file,
                           show = c("uncovered", "all"),
                           context = 1,
                           browse = TRUE,
                           spark_version = "4.1.0",
                           refresh = FALSE) {
  show <- match.arg(show)
  cov <- coverage_run(file, spark_version, refresh)
  html <- if (browse) coverage_report(file, spark_version) else NULL
  dv <- .coverage_table(file, cov)
  status <- setNames(dv$value, dv$line)

  base <- sub("[.]R$", "", basename(file))
  src <- readLines(here::here("R", paste0(base, ".R")))

  mark <- function(i) {
    s <- status[as.character(i)]
    if (is.na(s)) "    " else if (s == 0) "  x " else "  . "  # x = uncovered, . = covered
  }
  emit <- function(i) cat(sprintf("%4d%s| %s\n", i, mark(i), src[i]))

  cat(sprintf("R/%s.R   (x = uncovered, . = covered, blank = non-executable)\n\n", base))
  coverage_summary(file, spark_version)
  if (!is.null(html)) cat(sprintf("HTML report: %s\n", html))
  cat("\n")

  if (show == "all") {
    for (i in seq_along(src)) emit(i)
    return(invisible(dv))
  }

  unc <- sort(as.integer(names(status)[status == 0]))
  if (!length(unc)) {
    cat("No uncovered lines.\n")
    return(invisible(dv))
  }
  blocks <- split(unc, cumsum(c(1, diff(unc) != 1)))   # group consecutive lines
  for (b in blocks) {
    lo <- max(1, b[1] - context)
    hi <- min(length(src), b[length(b)] + context)
    for (i in lo:hi) emit(i)
    cat("\n")
  }
  invisible(dv)
}

## -------- Interactive HTML report written to a timestamped file.
##   Returns the path (does NOT open a browser) so the report can be shared and
##   re-opened without re-running. Files land in utils/coverage/reports/ .
coverage_report <- function(file, spark_version = "4.1.0", refresh = FALSE) {
  cov <- coverage_run(file, spark_version, refresh)

  base <- sub("[.]R$", "", basename(file))
  out_dir <- here::here("utils", "coverage", "reports")
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  out <- file.path(
    out_dir,
    sprintf("%s-%s.html", base, format(Sys.time(), "%Y%m%d-%H%M%S"))
  )

  covr::report(cov, file = out, browse = FALSE)
  invisible(out)
}
