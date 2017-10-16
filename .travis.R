parent_dir <- dir("../", full.names = TRUE)
sparklyr_package <- parent_dir[grepl("sparklyr_", parent_dir)]
install.packages(sparklyr_package, repos = NULL, type = "source")

source("testthat.R")
