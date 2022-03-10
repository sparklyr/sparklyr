library(sparklyr)
library(dplyr)

if (isTRUE(as.logical(Sys.getenv("ARROW_ENABLED")))) {
  library(arrow)
}

get_spark_warehouse_dir <- function() {
  ifelse(.Platform$OS.type == "windows", Sys.getenv("TEMP"), tempfile())
}

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
  if (!exists(".testthat_latest_spark", envir = .GlobalEnv)) {
    assign(".testthat_latest_spark", "3.0.0", envir = .GlobalEnv)
  }

  livy_version <- Sys.getenv("LIVY_VERSION")
  test_databricks_connect <- Sys.getenv("TEST_DATABRICKS_CONNECT")

  if (nchar(livy_version) > 0 && !identical(livy_version, "NONE")) {
    testthat_livy_connection()
  } else if (test_databricks_connect == "true") {
    testthat_shell_connection(method = "databricks")
  } else {
    testthat_shell_connection()
  }
}

testthat_latest_spark <- function() get(".testthat_latest_spark", envir = .GlobalEnv)

testthat_shell_connection <- function(method = "shell") {
  spark_home <- Sys.getenv("SPARK_HOME")
  if(spark_home != "") {
    version <- spark_version_from_home(spark_home)
  } else {
    version <- Sys.getenv("SPARK_VERSION", unset = testthat_latest_spark())

    if (exists(".testthat_livy_connection", envir = .GlobalEnv)) {
      spark_disconnect_all()
      Sys.sleep(3)
      livy_service_stop()
      remove(".testthat_livy_connection", envir = .GlobalEnv)
    }

    spark_installed <- spark_installed_versions()
    if (!is.null(version) && version == "master") {
      assign(".test_on_spark_master", TRUE, envir = .GlobalEnv)
      spark_installed <- spark_installed[with(spark_installed, order(spark, decreasing = TRUE)), ]
      version <- spark_installed[1, ]$spark
    }

    if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
      options(sparkinstall.verbose = TRUE)
      spark_install(version)
    }

    stopifnot(nrow(spark_installed_versions()) > 0)
  }

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
    config[["spark.sql.warehouse.dir"]] <- get_spark_warehouse_dir()
    if (identical(.Platform$OS.type, "windows")) {
      # TODO: investigate why there are Windows-specific timezone portability issues
      config[["spark.sql.session.timeZone"]] <- "UTC"
    }
    config$`sparklyr.sdf_collect.persistence_level` <- "NONE"

    packages <- if (version >= "2.4.0") "avro" else NULL
    if (version >= "2.4.2") packages <- c(packages, "delta")

    sc <- spark_connect(
      master = "local",
      method = method,
      version = version,
      config = config,
      packages = packages
    )
    assign(".testthat_spark_connection", sc, envir = .GlobalEnv)
    test_that(paste0("Starting new Spark connection, version:", sc$home_version), NULL)
  }

  # retrieve spark connection
  get(".testthat_spark_connection", envir = .GlobalEnv)
}

testthat_tbl <- function(name, data = NULL, repartition = 0L) {
  sc <- testthat_spark_connection()

  tbl <- tryCatch(dplyr::tbl(sc, name), error = identity)
  if (inherits(tbl, "error")) {
    if (is.null(data)) data <- eval(as.name(name), envir = parent.frame())
    tbl <- dplyr::copy_to(sc, data, name = name, repartition = repartition)
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

sdf_query_plan <- function(x, plan_type = c("optimizedPlan", "analyzed")) {
  plan_type <- match.arg(plan_type)

  x %>%
    spark_dataframe() %>%
    invoke("queryExecution") %>%
    invoke(plan_type) %>%
    invoke("toString") %>%
    strsplit("\n") %>%
    unlist()
}

wait_for_svc <- function(svc_name, port, timeout_s) {
  suppressWarnings({
    socket <- NULL
    on.exit(
      if (!is.null(socket)) {
        close(socket)
      }
    )
    for (t in 1:timeout_s) {
      try(
        socket <- socketConnection(
          host = "localhost", port = port, server = FALSE, open = "r+"
        ),
        silent = TRUE
      )
      if (is.null(socket)) {
        sprintf("Waiting for %s socket to be in listening state...", svc_name)
        Sys.sleep(1)
      } else {
        break
      }
    }
  })
}

testthat_livy_connection <- function() {
  spark_home <- Sys.getenv("SPARK_HOME")

  if(spark_home != "") {
    version <- spark_version_from_home(spark_home)
  } else {
    version <- Sys.getenv("SPARK_VERSION", unset = testthat_latest_spark())

    if (exists(".testthat_spark_connection", envir = .GlobalEnv)) {
      spark_disconnect_all()
      remove(".testthat_spark_connection", envir = .GlobalEnv)
      Sys.sleep(3)
    }

    spark_installed <- spark_installed_versions()
    if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
      spark_install(version)
    }
  }

  livy_version <- Sys.getenv("LIVY_VERSION", "0.5.0")

  if (nrow(livy_installed_versions()) == 0) {
    cat("Installing Livy.")
    livy_install(livy_version, spark_version = version, )
    cat("Livy installed.")
  }

  expect_gt(nrow(livy_installed_versions()), 0)

  livy_conf_dir <- normalizePath(file.path("~", "livy_conf"))
  Sys.setenv(LIVY_CONF_DIR = livy_conf_dir)
  if (!dir.exists(livy_conf_dir)) {
    dir.create(livy_conf_dir)
    writeLines(
      c(
        "log4j.rootCategory=DEBUG, FILE",
        "log4j.appender.FILE=org.apache.log4j.FileAppender",
        "log4j.appender.FILE.File=/tmp/livy-server.log",
        "log4j.appender.FILE.layout=org.apache.log4j.PatternLayout",
        "log4j.appender.FILE.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n",
        "log4j.logger.org.eclipse.jetty=WARN"
      ),
      file.path(livy_conf_dir, "log4j.properties")
    )
    writeLines(
      c(
        "livy.rsc.rpc.server.address = 0.0.0.0",
        "livy.client.http.connection.timeout = 60s",
        "livy.rsc.server.connect.timeout = 60s",
        "livy.rsc.client.connect.timeout = 60s"
      ),
      file.path(livy_conf_dir, "livy-client.conf")
    )
  }

  # generate connection if none yet exists
  connected <- FALSE
  if (exists(".testthat_livy_connection", envir = .GlobalEnv)) {
    sc <- get(".testthat_livy_connection", envir = .GlobalEnv)
    connected <- TRUE
  }

  if (Sys.getenv("INSTALL_WINUTILS") == "true") {
    spark_install_winutils(version)
  }

  livy_service_port <- 8998
  if (!connected) {
    livy_service_start(
      version = livy_version,
      spark_version = version,
      stdout = FALSE,
      stderr = FALSE
    )
    wait_for_svc(
      svc_name = "livy",
      port = livy_service_port,
      timeout_s = 30
    )
    config <- list()
    if (identical(.Platform$OS.type, "windows")) {
      # TODO: investigate why there are Windows-specific timezone portability issues
      config$`spark.sql.session.timeZone` <- "UTC"
    }

    config$`sparklyr.verbose` <- TRUE
    config$`sparklyr.connect.timeout` <- 120
    config$`sparklyr.log.invoke` <- "cat"
    config$`spark.sql.warehouse.dir` <- get_spark_warehouse_dir()
    config$`sparklyr.sdf_collect.persistence_level` <- "NONE"

    sc <- spark_connect(
      master = sprintf("http://localhost:%d", livy_service_port),
      method = "livy",
      config = config,
      version = version,
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

test_requires_latest_spark <- function() {
  test_requires_version(testthat_latest_spark())
}

param_filter_version <- function(args, min_version, params) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) < min_version) {
    args[params] <- NULL
  }
  args
}

param_add_version <- function(args, min_version, ...) {
  sc <- testthat_spark_connection()
  if (spark_version(sc) >= min_version) {
    c(args, list(...))
  } else {
    args
  }
}

output_file <- function(filename) file.path("output", filename)

skip_livy <- function() {
  livy_version <- Sys.getenv("LIVY_VERSION")
  if (nchar(livy_version) > 0 && !identical(livy_version, "NONE")) {
    skip("Test unsupported under Livy.")
  }
}

check_params <- function(test_args, params) {
  purrr::iwalk(
    test_args,
    ~ expect_equal(params[[.y]], .x, info = .y)
  )
}

test_param_setting <- function(sc, fn, test_args, is_ml_pipeline = TRUE) {
  collapse_sublists <- function(x) purrr::map_if(x, rlang::is_bare_list, unlist)

  params1 <- do.call(fn, c(list(x = sc), test_args)) %>%
    ml_params() %>%
    collapse_sublists()

  expected <- collapse_sublists(test_args)
  check_params(expected, params1)

  if (is_ml_pipeline) {
    params2 <- do.call(fn, c(list(x = ml_pipeline(sc)), test_args)) %>%
      ml_stage(1) %>%
      ml_params() %>%
      collapse_sublists()
    check_params(expected, params2)
  }
}

test_default_args <- function(sc, fn) {
  default_args <- rlang::fn_fmls(fn) %>%
    as.list() %>%
    purrr::discard(~ is.symbol(.x) || is.language(.x))

  default_args$Uid <- NULL # rlang::modify is deprecated
  default_args <- purrr::compact(default_args)

  params <- do.call(fn, list(x = sc)) %>%
    ml_params()
  check_params(default_args, params)
}

expect_coef_equal <- function(lhs, rhs) {
  nm <- names(lhs)
  lhs <- lhs[nm]
  rhs <- rhs[nm]

  expect_true(all.equal(lhs, rhs, tolerance = 0.01, scale = 1))
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

skip_on_spark_master <- function() {
  is_on_master <- identical(Sys.getenv("SPARK_VERSION"), "master")
  if (is_on_master) skip("Test skipped on spark master")
}

is_testing_databricks_connect <- function() {
  Sys.getenv("TEST_DATABRICKS_CONNECT") == "true"
}

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

random_table_name <- function(prefix) {
  paste0(prefix, paste0(floor(runif(10, 0, 10)), collapse = ""))
}

get_test_data_path <- function(file_name) {
  if (Sys.getenv("TEST_DATABRICKS_CONNECT") == "true") {
    test_data_path <- paste0(Sys.getenv("DBFS_DATA_PATH"), "/", file_name)
  } else {
    test_data_path <- file.path(normalizePath(getwd()), "data", file_name)
  }

  test_data_path
}

# Helper method to launch a local proxy listening on `proxy_port` and forwarding
# TCP packets to `dest_port`
# This method will return an opaque handle with a finalizer that will stop the
# proxy process once called
local_tcp_proxy <- function(proxy_port, dest_port) {
  pid <- system2(
    "bash",
    args = c(
      "-c",
      paste0(
        "'socat tcp-l:",
        as.integer(proxy_port),
        ",fork,reuseaddr tcp:localhost:",
        as.integer(dest_port),
        " >/dev/null 2>&1 & disown; echo $!'"
      )
    ),
    stdout = TRUE
  )
  wait_for_svc("local_tcp_proxy", proxy_port, timeout_s = 10)

  handle <- structure(
    new.env(parent = emptyenv()),
    class = "local_tcp_proxy_handle"
  )
  reg.finalizer(
    handle,
    function(x) {
      system2("pkill", args = c("-P", pid))
      system2("kill", args = pid)
    },
    onexit = TRUE
  )

  handle
}
