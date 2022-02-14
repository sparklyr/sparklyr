library(magrittr)
library(stringr)
library(purrr)
library(fs)

# devtools::build(path = "revdep")
# revdeps <- tools::check_packages_in_dir("revdep", reverse =  list())

res <- map(
  dir_ls(path = "revdep", glob = "*.Rcheck"),
  ~ {
    # Read log
    raw_log <- readLines(path(.x, "00check.log"))

    # Get package name and version
    pkg_line <- raw_log[str_detect(raw_log, pattern = "this is package")]
    pkg_entry <- substr(pkg_line, 20, nchar(pkg_line) - 1)
    pkg_info <- str_split(pkg_entry, "’ version ‘")[[1]]

    # Get lines with results
    results <- map(
      raw_log,
      ~ ifelse(str_detect(.x, " ... "), str_split(.x, " \\.\\.\\. "), "")
    ) %>%
      map(~ .x[[1]]) %>%
      keep(~ length(.x) == 2) %>%
      map_chr(~ .x[[2]])

    # Tabulate results
    totals <- map_chr(
      c("ERROR", "WARNING", "NOTE", "OK"),
      ~ sum(results == .x)
    ) %>%
      paste(collapse = "|")

    # Summary entry
    summary <- paste0(
      "|[", pkg_info[1], "](#", pkg_info[1], ")|", pkg_info[2], "|", totals, "|"
    )

    # Extract lines that are not OK
    is_ok <- map_lgl(raw_log, ~ str_detect(.x, " ... OK"))
    not_ok <- raw_log[!is_ok]
    pkg_title <- paste("### ", pkg_info[1], collapse = "")
    details <- c(pkg_title, "```" ,not_ok, "```", "")

    list(summary = summary, details = details)
  }
)

md_contents <- c(
  "## Reverse dependencies", "",
  "|Package|Version|Error|Warning|Note|OK|",
  "|:---|:---|:---|:---|:---|:---|",
  map_chr(res, ~ .x[[1]]), "",
  "## Details",
  reduce(map(res, ~ .x[[2]]), c)
)

writeLines(md_contents, con = "revdep/README.md")
