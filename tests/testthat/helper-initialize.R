testthat_spark_connection <- function() {
  version <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")

  if (exists(".testthat_livy_connection", envir = .GlobalEnv)) {
    spark_disconnect_all()
    Sys.sleep(3)
    livy_service_stop()
    remove(".testthat_livy_connection", envir = .GlobalEnv)
  }

  spark_installed <- spark_installed_versions()
  if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
    options(sparkinstall.verbose = TRUE)
    spark_install(version)
  }

  expect_gt(nrow(spark_installed_versions()), 0)

  # generate connection if none yet exists
  connected <- FALSE
  if (exists(".testthat_spark_connection", envir = .GlobalEnv)) {
    sc <- get(".testthat_spark_connection", envir = .GlobalEnv)
    connected <- connection_is_open(sc)
  }

  if (!connected) {
    config <- spark_config()

    options(sparklyr.sanitize.column.names.verbose = TRUE)
    options(sparklyr.verbose = TRUE)
    options(sparklyr.na.omit.verbose = TRUE)
    options(sparklyr.na.action.verbose = TRUE)

    setwd(tempdir())
    sc <- spark_connect(master = "local", version = version, config = config)
    assign(".testthat_spark_connection", sc, envir = .GlobalEnv)
  }

  # retrieve spark connection
  get(".testthat_spark_connection", envir = .GlobalEnv)
}

testthat_tbl <- function(name) {
  sc <- testthat_spark_connection()
  tbl <- tryCatch(dplyr::tbl(sc, name), error = identity)
  if (inherits(tbl, "error")) {
    data <- eval(as.name(name), envir = parent.frame())
    tbl <- dplyr::copy_to(sc, data, name = name)
  }
  tbl
}

skip_unless_verbose <- function(message = NULL) {
  message <- message %||% "Verbose test skipped"
  verbose <- Sys.getenv("SPARKLYR_TESTS_VERBOSE", unset = NA)
  if (is.na(verbose)) skip(message)
  invisible(TRUE)
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

# helper functions for testing from broom
# test the basics of tidy/augment/glance output: is a data frame, no row names
check_tidiness <- function(o) {
  expect_is(o, "data.frame")
  expect_equal(rownames(o), as.character(seq_len(nrow(o))))
}

# check the output of a tidy function
check_tidy <- function(o, exp.row = NULL, exp.col = NULL, exp.names = NULL) {
  check_tidiness(o)

  if (!is.null(exp.row)) {
    expect_equal(nrow(o), exp.row)
  }
  if (!is.null(exp.col)) {
    expect_equal(ncol(o), exp.col)
  }
  if (!is.null(exp.names)) {
    expect_true(all(exp.names %in% colnames(o)))
  }
}

sdf_query_plan <- function(x) {
  x %>%
    spark_dataframe() %>%
    invoke("queryExecution") %>%
    invoke("optimizedPlan") %>%
    invoke("toString") %>%
    strsplit("\n") %>%
    unlist()
}

testthat_livy_connection <- function() {
  version <- Sys.getenv("SPARK_VERSION", unset = "2.2.0")

  if (exists(".testthat_spark_connection", envir = .GlobalEnv)) {
    spark_disconnect_all()
    remove(".testthat_spark_connection", envir = .GlobalEnv)
    Sys.sleep(3)
  }

  spark_installed <- spark_installed_versions()
  if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
    spark_install(version)
  }

  if (nrow(livy_installed_versions()) == 0) {
    livy_install("0.3.0", spark_version = version)
  }

  expect_gt(nrow(livy_installed_versions()), 0)

  # generate connection if none yet exists
  connected <- FALSE
  if (exists(".testthat_livy_connection", envir = .GlobalEnv)) {
    sc <- get(".testthat_livy_connection", envir = .GlobalEnv)
    connected <- TRUE
  }

  if (!connected) {
    livy_service_start(
      version = "0.3.0",
      spark_version = version,
      stdout = FALSE,
      stderr = FALSE)

    sc <- spark_connect(master = "http://localhost:8998", method = "livy")
    assign(".testthat_livy_connection", sc, envir = .GlobalEnv)
  }

  get(".testthat_livy_connection", envir = .GlobalEnv)
}

get_default_args <- function(fn, exclude = NULL) {
  formals(fn) %>%
    (function(x) x[setdiff(names(x), c(exclude, c("x", "uid", "...", "formula")))])
}

test_requires_version <- function(min_version, comment = NULL) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) < min_version) {
    msg <- paste0("test requires Spark version ", min_version)
    if (!is.null(comment))
      msg <- paste0(msg, ": ", comment)
    skip(msg)
  }
}

param_filter_version <- function(args, min_version, params) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) < min_version)
    args[params] <- NULL
  args
}

param_add_version <- function(args, min_version, ...) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) >= min_version)
    c(args, list(...))
  else
    args
}

output_file <- function(filename) file.path("output", filename)
