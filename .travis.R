args <- commandArgs(trailingOnly=TRUE)

if (length(args) == 0) {
  stop("Missing arguments")
} else if (args[[1]] == "--testthat") {
  if (package_version(paste(R.Version()$major, R.Version()$minor, sep = ".")) >= "3.3") {
    install.packages("sparklyr.nested")
  }

  parent_dir <- dir(".", full.names = TRUE)
  sparklyr_package <- parent_dir[grepl("sparklyr_", parent_dir)]
  install.packages(sparklyr_package, repos = NULL, type = "source")

  on.exit(setwd(".."))
  setwd("tests")
  source("testthat.R")
} else if (args[[1]] == "--coverage") {
  devtools::install_github("javierluraschi/covr", ref = "feature/no-batch")
  covr::codecov(type = "none", code = "setwd('tests'); source('testthat.R')", batch = FALSE)
} else if (args[[1]] == "--arrow") {
  install.packages("devtools")
  devtools::install_github("javierluraschi/arrow", subdir = "r")
} else {
  stop("Unsupported arguments")
}
