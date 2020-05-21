args <- commandArgs(trailingOnly=TRUE)

ensure_pkgs <- function(pkgs) {
  for (pkg in pkgs)
    if (!require(pkg, character.only = TRUE))
      install.packages(pkg)
}

if (length(args) == 0) {
  stop("Missing arguments")
} else if (args[[1]] == "--install_pkgs") {
  if (package_version(paste(R.Version()$major, R.Version()$minor, sep = ".")) >= "3.3") {
    ensure_pkgs("sparklyr.nested")
  }
  parent_dir <- dir(".", full.names = TRUE)
  sparklyr_package <- parent_dir[grepl("sparklyr_", parent_dir)]
  install.packages(sparklyr_package, repos = NULL, type = "source")
} else if (args[[1]] == "--testthat") {
  on.exit(setwd(normalizePath("..")))
  setwd("tests")
  source("testthat.R")
} else if (args[[1]] == "--coverage") {
  ensure_pkgs("devtools")

  devtools::install_github("javierluraschi/covr", ref = "feature/no-batch")
  covr::codecov(type = "none", code = "setwd('tests'); source('testthat.R')", batch = FALSE)
} else if (args[[1]] == "--arrow") {
  ensure_pkgs("devtools")

  if (length(args) >= 2) {
    devtools::install_github("apache/arrow", subdir = "r", ref = args[2])
  }
  else {
    devtools::install_github("apache/arrow", subdir = "r")
  }
} else if (args[[1]] == "--verify-embedded-srcs") {
  ensure_pkgs(c("diffobj", "stringr"))

  sparklyr:::spark_verify_embedded_sources()
}else {
  stop("Unsupported arguments")
}
