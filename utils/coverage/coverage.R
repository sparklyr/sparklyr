# Single-file coverage helpers
#
# Source this file (from anywhere — paths resolve via here::here()), then call
# the functions at will:
#
#   source("utils/coverage/coverage.R")
#   coverage_lines("dplyr_verbs")     # runs coverage, writes a MATCHED PAIR of
#                                     #   report files, and prints the text
#                                     #   annotation. Both files come from the
#                                     #   same run:
#                                     #     reports/<file>-<stamp>.html  (for humans)
#                                     #     reports/<file>-<stamp>.txt   (for Claude)
#   coverage_report("dplyr_verbs")    # HTML report only
#   coverage_summary("dplyr_verbs")   # one-line % summary
#
# Each call re-runs the (slow ~5 min) instrumentation, because the point of this
# tool is to *improve* coverage: you edit the paired test, re-run, and compare.
# Re-using a stale snapshot would defeat that, so there is no persistent cache —
# only an in-session memo so a single call doesn't compute twice. The persisted
# artifacts are the timestamped report files themselves: keep the .txt + .html
# from a run and you can both look at the exact same information later.
#
# Why this dance: `covr::file_coverage()` is faster but does not load the
# testthat helper-*/setup-* files, so tests that need `testthat_spark_connection()`
# etc. fail. `covr::package_coverage()` with `load_package = "installed"` runs the
# real test harness against an instrumented install, which is the only reliable
# path for this package. Note the number is coverage attributable to the *paired*
# test file alone (filter = the file's base name) — by design, so a script's
# coverage never depends on unrelated test files.

library(covr)
library(withr)
library(here)   # non-package util, so here::here() is fair game for path resolution

# ---------------------- Internal ----------------

# In-session memo: file base name -> coverage object. Prevents a single
# coverage_lines() call (which needs the object for both text and HTML) from
# instrumenting twice. NOT a persistent cache; cleared when the session ends.
.coverage_memo <- new.env(parent = emptyenv())

.coverage_base <- function(file) sub("[.]R$", "", basename(file))

.coverage_reports_dir <- function() {
  dir <- here::here("utils", "coverage", "reports")
  dir.create(dir, showWarnings = FALSE, recursive = TRUE)
  dir
}

## -------- Run coverage for one R script (memoized within the session only)
coverage_run <- function(file,
                         spark_version = "3.5.8",
                         refresh = FALSE) {
  base <- .coverage_base(file)
  if (!refresh && !is.null(.coverage_memo[[base]])) {
    return(.coverage_memo[[base]])
  }

  test_dir <- here::here("tests", "testthat")
  code <- c(
    "library(testthat)",
    "library(sparklyr)",
    sprintf(
      'testthat::test_dir("%s", filter = "%s", package = "sparklyr", load_package = "installed")',
      test_dir, base
    )
  )

  cov <- withr::with_envvar(
    c(CODE_COVERAGE = "true", SPARK_VERSION = spark_version),
    covr::package_coverage(type = "none", code = code)
  )

  .coverage_memo[[base]] <- cov
  cov
}

## -------- Per-line table (line, hits) for just the target file
.coverage_table <- function(file, cov) {
  tc <- covr::tally_coverage(cov)
  pat <- paste0(.coverage_base(file), "[.]R$")
  dv <- tc[grepl(pat, tc$filename), c("line", "value")]
  dv[order(dv$line), ]
}

## -------- Build the text annotation as a character vector (so it can be both
##          printed and written to a file verbatim).
.coverage_annotation <- function(file, cov, show, context) {
  base <- .coverage_base(file)
  dv <- .coverage_table(file, cov)
  status <- setNames(dv$value, dv$line)
  src <- readLines(here::here("R", paste0(base, ".R")))
  covered <- sum(dv$value > 0)

  mark <- function(i) {
    s <- status[as.character(i)]
    if (is.na(s)) "    " else if (s == 0) "  x " else "  . "  # x = uncovered, . = covered
  }
  line_at <- function(i) sprintf("%4d%s| %s", i, mark(i), src[i])

  out <- c(
    sprintf("R/%s.R   (x = uncovered, . = covered, blank = non-executable)", base),
    sprintf(
      "R/%s.R: %.1f%% | %d executable lines | %d covered | %d NOT covered",
      base, 100 * covered / nrow(dv), nrow(dv), covered, nrow(dv) - covered
    ),
    ""
  )

  if (show == "all") {
    return(c(out, vapply(seq_along(src), line_at, character(1))))
  }

  unc <- sort(as.integer(names(status)[status == 0]))
  if (!length(unc)) {
    return(c(out, "No uncovered lines."))
  }
  blocks <- split(unc, cumsum(c(1, diff(unc) != 1)))   # group consecutive lines
  for (b in blocks) {
    lo <- max(1, b[1] - context)
    hi <- min(length(src), b[length(b)] + context)
    out <- c(out, vapply(lo:hi, line_at, character(1)), "")
  }
  out
}

# ---------------------- Public ----------------

## -------- One-line summary
coverage_summary <- function(file, spark_version = "3.5.8", refresh = FALSE) {
  cov <- coverage_run(file, spark_version, refresh)
  dv <- .coverage_table(file, cov)
  covered <- sum(dv$value > 0)
  cat(sprintf(
    "R/%s.R: %.1f%% | %d executable lines | %d covered | %d NOT covered\n",
    .coverage_base(file), 100 * covered / nrow(dv), nrow(dv), covered, nrow(dv) - covered
  ))
  invisible(dv)
}

## -------- Text annotation of covered/uncovered lines.
##   Writes a MATCHED PAIR of report files (same stem) so the .txt Claude reads
##   and the .html a human opens come from the exact same run, then prints the
##   text to the console.
##   show = "uncovered" (default) prints only uncovered blocks with `context`
##          lines around them; show = "all" annotates the whole file.
##   html = FALSE skips the HTML half (text artifact only).
coverage_lines <- function(file,
                           show = c("uncovered", "all"),
                           context = 1,
                           html = TRUE,
                           spark_version = "3.5.8",
                           refresh = FALSE) {
  show <- match.arg(show)
  cov <- coverage_run(file, spark_version, refresh)

  base <- .coverage_base(file)
  stem <- file.path(
    .coverage_reports_dir(),
    sprintf("%s-%s", base, format(Sys.time(), "%Y%m%d-%H%M%S"))
  )

  txt <- .coverage_annotation(file, cov, show, context)
  html_path <- NULL
  if (html) {
    html_path <- paste0(stem, ".html")
    covr::report(cov, file = html_path, browse = FALSE)
  }

  # Footer pointing each reader at the matching artifact.
  txt <- c(
    txt,
    "---",
    sprintf("text report: %s", paste0(stem, ".txt")),
    if (!is.null(html_path)) sprintf("html report: %s", html_path)
  )
  writeLines(txt, paste0(stem, ".txt"))

  cat(txt, sep = "\n")
  cat("\n")
  invisible(list(txt = paste0(stem, ".txt"), html = html_path))
}

## -------- HTML report only, written to a timestamped file. Returns the path
##   (does NOT open a browser) so it can be shared / re-opened without re-running.
coverage_report <- function(file, spark_version = "3.5.8", refresh = FALSE) {
  cov <- coverage_run(file, spark_version, refresh)
  out <- file.path(
    .coverage_reports_dir(),
    sprintf("%s-%s.html", .coverage_base(file), format(Sys.time(), "%Y%m%d-%H%M%S"))
  )
  covr::report(cov, file = out, browse = FALSE)
  invisible(out)
}
