skip_unless_databricks_connect <- function() {
  if (!is_testing_databricks_connect()) {
    skip("Test only runs on Databricks Connect")
  }
}

skip_databricks_connect <- function() {
  if (is_testing_databricks_connect()) {
    skip("Test is skipped on Databricks Connect")
  }
}
skip_arrow_devel <- function(message) {
  is_arrow_devel <- identical(Sys.getenv("ARROW_VERSION"), "devel")
  if (is_arrow_devel) skip(message)
}

skip_slow <- function(message) {
  skip_covr(message)
  skip_arrow_devel(message)
}

skip_on_spark_master <- function() {
  is_on_master <- identical(Sys.getenv("SPARK_VERSION"), "master")
  if (is_on_master) skip("Test skipped on spark master")
}

skip_unless_verbose <- function(message = NULL) {
  message <- message %||% "Verbose test skipped"
  verbose <- Sys.getenv("SPARKLYR_TESTS_VERBOSE", unset = NA)
  if (is.na(verbose)) skip(message)
  invisible(TRUE)
}
skip_on_arrow <- function() {
  if (using_arrow()) skip("Test unsupported in Apache Arrow")
}

skip_on_windows <- function() {
  if (identical(.Platform$OS.type, "windows")) {
    skip("Test will be skipped on Windows")
  }
}

skip_covr <- function(message) {
  is_covr <- identical(Sys.getenv("CODE_COVERAGE"), "true")
  if (is_covr) skip(message)
}

skip_livy <- function() {
  livy_version <- Sys.getenv("LIVY_VERSION")
  if (nchar(livy_version) > 0 && !identical(livy_version, "NONE")) {
    skip("Test unsupported under Livy.")
  }
}

test_requires_version <- function(min_version, comment = NULL, max_version = NULL) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) < min_version) {
    msg <- paste0("test requires Spark version ", min_version)
    if (!is.null(comment)) {
      msg <- paste0(msg, ": ", comment)
    }
    skip(msg)
  } else if (!is.null(max_version)) {
    if (spark_version(sc) >= max_version) {
      msg <- paste0("test is not needed with Spark version ", max_version)
      if (!is.null(comment)) {
        msg <- paste0(msg, ": ", comment)
      }
      skip(msg)
    }
  }
}

test_requires <- function(...) {
  suppressPackageStartupMessages({
    for (pkg in list(...)) {
      if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
        fmt <- "test requires '%s' but '%s' is not installed"
        skip(sprintf(fmt, pkg, pkg))
      }
    }
  })

  invisible(TRUE)
}

test_requires_latest_spark <- function() {
  test_requires_version(testthat_latest_spark())
}
