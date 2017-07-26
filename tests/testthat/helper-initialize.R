testthat_spark_connection <- function(version = NULL) {
  if (nrow(spark_installed_versions()) == 0) {
    spark_install("2.1.0")
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

    version <- version %||% Sys.getenv("SPARK_VERSION", unset = "2.1.0")
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

  for (pkg in list(...)) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      fmt <- "test requires '%s' but '%s' is not installed"
      skip(sprintf(fmt, pkg, pkg))
    }
  }

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
