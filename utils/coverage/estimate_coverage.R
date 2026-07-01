# Canonical coverage figures, read straight from covr's HTML reports.
#
# covr::report() (what coverage_lines()/coverage_report() write as the .html)
# embeds a summary DataTable with one row per instrumented file and the columns
#   File | Lines | Relevant | Covered | Missed | Hits / Line | Coverage
# Those are covr's own numbers, so we read them directly rather than counting
# lines in the .txt annotation (which counts every line a multi-line expression
# spans, over-counting "missed" versus covr -- e.g. connection_livy 154 vs 117).
#
#   source("utils/coverage/estimate_coverage.R")
#   read_coverage_report("connection_livy-20260626-155809.html")
#   #>  relevant  covered    missed coverage
#   #>       546      429       117    78.57
#   compile_coverage_estimates()   # dedup + summarize all reports

library(here)

# Parse a covr report .html and return covr's canonical totals for the file the
# report is named after: c(relevant, covered, missed, coverage).
read_coverage_report <- function(html_file) {
  path <- if (file.exists(html_file)) {
    html_file
  } else {
    here::here("utils", "coverage", "reports", basename(html_file))
  }
  if (!file.exists(path)) {
    stop("Report not found: ", html_file, call. = FALSE)
  }

  html <- paste(readLines(path, warn = FALSE), collapse = "\n")

  # covr embeds several htmlwidget payloads; the summary table is the one whose
  # header row contains a "Coverage" column.
  payloads <- regmatches(
    html,
    gregexpr('application/json"[^>]*>\\{.*?\\}</script>', html)
  )[[1]]
  widget <- NULL
  for (p in payloads) {
    js <- sub('^application/json"[^>]*>', "", p)
    js <- sub("</script>$", "", js)
    w <- tryCatch(
      jsonlite::fromJSON(js, simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (!is.null(w$x$container) && grepl("<th>Coverage</th>", w$x$container)) {
      widget <- w$x
      break
    }
  }
  if (is.null(widget)) {
    stop("No covr summary table in ", basename(path), call. = FALSE)
  }

  # column names, in order, from the table header
  cols <- regmatches(
    widget$container,
    gregexpr("<th>[^<]+</th>", widget$container)
  )[[1]]
  cols <- sub("</th>$", "", sub("^<th>", "", cols))
  col <- function(name) widget$data[[which(cols == name)]]

  # locate the row for the file the report is named after (<base>.R)
  base <- sub("-[0-9]{8}-[0-9]{6}[.]html$", "", basename(path))
  files <- vapply(col("File"), as.character, character(1))
  idx <- grep(paste0("/", base, "[.]R<"), files)
  if (length(idx) != 1) {
    stop("R/", base, ".R not found uniquely in ", basename(path), call. = FALSE)
  }

  num <- function(name) as.numeric(gsub("[^0-9.]", "", col(name)[[idx]]))
  c(
    lines = num("Lines"),
    relevant = num("Relevant"),
    covered = num("Covered"),
    missed = num("Missed"),
    coverage = num("Coverage")
  )
}

# Compile covr's canonical coverage across every report. All report .html files
# in `reports_dir` are deduplicated by script name (most recent date-time wins),
# each chosen report is read with read_coverage_report(), and the accumulated
# results are written to a timestamped file in utils/coverage/compiled/ (the
# timestamp marks which compiled run is newest). Returns the output path.
compile_coverage_estimates <- function(
  reports_dir = here::here("utils", "coverage", "reports"),
  compiled_dir = here::here("utils", "coverage", "compiled")
) {
  pattern <- "-[0-9]{8}-[0-9]{6}[.]html$"
  htmls <- list.files(reports_dir, pattern = pattern, full.names = TRUE)
  if (!length(htmls)) {
    stop("No .html reports found in ", reports_dir, call. = FALSE)
  }

  info <- data.frame(
    path = htmls,
    base = sub(pattern, "", basename(htmls)),
    stamp = sub(
      paste0("^.*-([0-9]{8}-[0-9]{6})[.]html$"),
      "\\1",
      basename(htmls)
    ),
    stringsAsFactors = FALSE
  )
  # dedup by script, keeping the most recent report
  info <- info[order(info$base, info$stamp), ]
  info <- info[!duplicated(info$base, fromLast = TRUE), ]

  rows <- lapply(seq_len(nrow(info)), function(i) {
    vals <- tryCatch(
      read_coverage_report(info$path[i]),
      error = function(e) {
        warning("Skipping ", info$base[i], ": ", conditionMessage(e))
        NULL
      }
    )
    if (is.null(vals)) {
      return(NULL)
    }
    data.frame(
      script = info$base[i],
      report = info$stamp[i],
      relevant = vals[["relevant"]],
      covered = vals[["covered"]],
      missed = vals[["missed"]],
      coverage = vals[["coverage"]],
      stringsAsFactors = FALSE
    )
  })
  result <- do.call(rbind, rows)
  result <- result[order(result$coverage, result$script), ]

  agg_rel <- sum(result$relevant)
  agg_cov <- sum(result$covered)
  agg_pct <- round(100 * agg_cov / agg_rel, 2)

  dir.create(compiled_dir, showWarnings = FALSE, recursive = TRUE)
  stamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
  out <- file.path(compiled_dir, paste0(stamp, ".txt"))

  writeLines(
    c(
      paste0(
        "# Coverage compiled ",
        stamp,
        " (covr canonical figures, read from HTML)"
      ),
      paste0("# ", nrow(result), " scripts, most recent report each"),
      "",
      utils::capture.output(print(result, row.names = FALSE)),
      "",
      sprintf(
        "TOTAL: %d scripts | %d relevant | %d covered | %d missed | %.2f%% coverage",
        nrow(result),
        agg_rel,
        agg_cov,
        agg_rel - agg_cov,
        agg_pct
      )
    ),
    out
  )

  out
}
