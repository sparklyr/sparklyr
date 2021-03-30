#!/bin/bash

export MAKEFLAGS="-j$(($(nproc) + 1))"

TEST_DEPS="broom \
           diffobj \
           e1071 \
           foreach \
           glmnet \
           ggplot2 \
           iterators \
           janeaustenr \
           Lahman \
           mlbench \
           nnet \
           nycflights13 \
           qs \
           R6 \
           RCurl \
           reshape2 \
           shiny \
           sparklyr.nested \
           stringr \
           testthat"
SEP='"\\s+"'

R_REMOTES_NO_ERRORS_FROM_WARNINGS=true Rscript <(
cat << _RSCRIPT_EOF_
  install.packages("remotes")
  remotes::install_deps(dependencies = c("Imports"))

  test_deps <- strsplit("$TEST_DEPS", $SEP)[[1]]
  for (pkg in test_deps)
    if (!require(pkg, character.only = TRUE))
      install.packages(pkg)
  if (Sys.getenv("ARROW_ENABLED") == "true") {
    if (Sys.getenv("ARROW_VERSION") == "devel") {
      # Add the arrow nightly repository
      options(repos = c("https://dl.bintray.com/ursalabs/arrow-r", getOption("repos")))
    }
    install.packages("arrow")
  }
  if (!identical(Sys.getenv("DBPLYR_VERSION"), "")) {
    remotes::install_version("dbplyr", version = Sys.getenv("DBPLYR_VERSION"))
  }
_RSCRIPT_EOF_
)
