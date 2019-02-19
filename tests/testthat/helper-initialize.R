library(sparklyr)
library(dplyr)

spark_install_winutils <- function(version) {
  hadoop_version <- if (version < "2.0.0") "2.6" else "2.7"
  spark_dir <- paste("spark-", version, "-bin-hadoop", hadoop_version, sep = "")
  winutils_dir <- file.path(Sys.getenv("LOCALAPPDATA"), "spark", spark_dir, "tmp", "hadoop", "bin", fsep = "\\")

  if (!dir.exists(winutils_dir)) {
    message("Installing winutils...")

    dir.create(winutils_dir, recursive = TRUE)
    winutils_path <- file.path(winutils_dir, "winutils.exe", fsep = "\\")

    download.file(
      "https://github.com/steveloughran/winutils/raw/master/hadoop-2.6.0/bin/winutils.exe",
      winutils_path,
      mode = "wb"
    )

    message("Installed winutils in ", winutils_path)
  }
}

testthat_spark_connection <- function() {
  if (!exists(".testthat_latest_spark", envir = .GlobalEnv))
    assign(".testthat_latest_spark", "2.3.0", envir = .GlobalEnv)
  livy_version <- Sys.getenv("LIVY_VERSION")
  if (nchar(livy_version) > 0)
    testthat_livy_connection()
  else
    testthat_shell_connection()
}

testthat_latest_spark <- function() get(".testthat_latest_spark", envir = .GlobalEnv)

testthat_shell_connection <- function() {
  version <- Sys.getenv("SPARK_VERSION", unset = testthat_latest_spark())

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

  stopifnot(nrow(spark_installed_versions()) > 0)

  # generate connection if none yet exists
  connected <- FALSE
  if (exists(".testthat_spark_connection", envir = .GlobalEnv)) {
    sc <- get(".testthat_spark_connection", envir = .GlobalEnv)
    connected <- connection_is_open(sc)
  }

  if (Sys.getenv("INSTALL_WINUTILS") == "true") {
    spark_install_winutils(version)
  }

  if (!connected) {
    config <- spark_config()

    options(sparklyr.sanitize.column.names.verbose = TRUE)
    options(sparklyr.verbose = TRUE)
    options(sparklyr.na.omit.verbose = TRUE)
    options(sparklyr.na.action.verbose = TRUE)

    config[["sparklyr.shell.driver-memory"]] <- "3G"
    config[["sparklyr.apply.env.foo"]] <- "env-test"

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
  version <- Sys.getenv("SPARK_VERSION", unset = testthat_latest_spark())
  livy_version <- Sys.getenv("LIVY_VERSION", "0.5.0")

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
    cat("Installing Livy.")
    livy_install(livy_version, spark_version = version, )
    cat("Livy installed.")
  }

  expect_gt(nrow(livy_installed_versions()), 0)

  # generate connection if none yet exists
  connected <- FALSE
  if (exists(".testthat_livy_connection", envir = .GlobalEnv)) {
    sc <- get(".testthat_livy_connection", envir = .GlobalEnv)
    connected <- TRUE
  }

  if (Sys.getenv("INSTALL_WINUTILS") == "true") {
    spark_install_winutils(version)
  }

  if (!connected) {
    livy_service_start(
      version = livy_version,
      spark_version = version,
      stdout = FALSE,
      stderr = FALSE)

    sc <- spark_connect(
      master = "http://localhost:8998",
      method = "livy",
      config = list(
        sparklyr.verbose = TRUE,
        sparklyr.connect.timeout = 120,
        sparklyr.log.invoke = "cat"
      ),
      sources = TRUE
    )

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

test_requires_latest_spark <- function() {
  test_requires_version(testthat_latest_spark())
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

skip_livy <- function() {
  livy_version <- Sys.getenv("LIVY_VERSION")
  if (nchar(livy_version) > 0) skip("Test unsupported under Livy.")
}

check_params <- function(test_args, params) {
  purrr::iwalk(
    test_args,
    ~ expect_equal(params[[.y]], .x, info = .y)
  )
}

test_param_setting <- function(sc, fn, test_args) {
  collapse_sublists <- function(x) purrr::map_if(x, rlang::is_bare_list, unlist)

  params1 <- do.call(fn, c(list(x = sc), test_args)) %>%
    ml_params() %>%
    collapse_sublists()

  params2 <- do.call(fn, c(list(x = ml_pipeline(sc)), test_args)) %>%
    ml_stage(1) %>%
    ml_params() %>%
    collapse_sublists()

  test_args <- collapse_sublists(test_args)
  check_params(test_args, params1)
  check_params(test_args, params2)
}

test_default_args <- function(sc, fn) {
  default_args <- rlang::fn_fmls(fn) %>%
    as.list() %>%
    purrr::discard(~ is.symbol(.x) || is.language(.x)) %>%
    rlang::modify(uid = NULL) %>%
    purrr::compact()

  params <- do.call(fn, list(x = sc)) %>%
    ml_params()
  check_params(default_args, params)
}

expect_coef_equal <- function(lhs, rhs) {
  nm <- names(lhs)
  lhs <- lhs[nm]
  rhs <- rhs[nm]

  expect_true(all.equal(lhs, rhs, tolerance = 0.01))
}

skip_on_arrow <- function() {
  r_arrow <- isTRUE(as.logical(Sys.getenv("R_ARROW")))
  if (r_arrow) skip("Test unsupported in Apache Arrow")
}

skip_covr <- function(message) {
  is_covr <- identical(Sys.getenv("CODE_COVERAGE"), "true")
  if (is_covr) skip(message)
}

using_arrow <- function() {
  "package:arrow" %in% search()
}

skip_arrow_devel <- function(message) {
  is_arrow_devel <- identical(Sys.getenv("ARROW_VERSION"), "devel")
  if (is_arrow_devel) skip(message)
}

skip_slow <- function(message) {
  skip_covr(message)
  skip_arrow_devel(message)
}
