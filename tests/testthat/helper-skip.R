skip_unless_local <- function() {
  if(testthat_spark_connection_type() != "local") {
    skip("Test only run on local Spark connection")
  }
}

skip_unless_databricks_connect <- function() {
  if (!using_databricks()) {
    skip("Test only runs on Databricks Connect")
  }
}

skip_databricks_connect <- function() {
  if (using_databricks()) {
    skip("Test is skipped on Databricks Connect")
  }
}

skip_unless_synapse_connect <- function() {
  if (testthat_spark_connection_type() != "synapse") {
    skip("Test only runs on Synapse connection")
  }
}

skip_on_arrow_devel <- function(message = "Test is skipped on Arrow development version") {
  if (using_arrow_version() == "devel") skip(message)
}

skip_slow <- function(message) {
  skip_covr(message)
  skip_on_arrow_devel(message)
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

skip_on_livy <- function() {
  if(using_livy()) {
    skip("Test unsupported under Livy.")
  }
}

skip_unless_livy <- function() {
  if(!using_livy()) {
    skip("Test only runs on Livy")
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

test_requires_package_version <- function(pkg, min_version) {
  if (packageVersion(pkg) < min_version) {
    skip(paste0("Test requires ", pkg," ", min_version," or above"))
  }
}
