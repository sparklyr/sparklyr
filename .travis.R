args <- commandArgs(trailingOnly=TRUE)

if (length(args) == 0) {
  stop("Missing arguments")
} else if (args[[1]] == "--testthat") {
  parent_dir <- dir(".", full.names = TRUE)
  sparklyr_package <- parent_dir[grepl("sparklyr_", parent_dir)]
  install.packages(sparklyr_package, repos = NULL, type = "source")

  on.exit(setwd(".."))
  setwd("tests")
  source("testthat.R")
} else if (args[[1]] == "--coverage") {
  covr::codecov(quiet = FALSE, type = "none", code = "setwd('tests'); source('testthat.R')")
} else if (args[[1]] == "--arrow") {
  install.packages("devtools")
  devtools::install_github("javierluraschi/arrow", subdir = "r", ref = "bugfix/r-empty-character")
} else {
  stop("Unsupported arguments")
}
