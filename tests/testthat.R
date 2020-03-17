Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

PerformanceReporter <- R6::R6Class("PerformanceReporter",
                                   inherit = Reporter,
                                   public = list(
                                     results = list(
                                       context = character(0),
                                       time = numeric(0)
                                     ),
                                     last_context = NA_character_,
                                     last_test = NA_character_,
                                     last_time = Sys.time(),
                                     last_test_time = 0,
                                     n_ok = 0,
                                     n_skip = 0,
                                     n_warn = 0,
                                     n_fail = 0,

                                     start_context = function(context) {
                                       private$print_last_test()

                                       self$last_context <- context
                                       self$last_time <- Sys.time()
                                       cat(paste0("\nContext: ", context, "\n"))
                                     },

                                     add_result = function(context, test, result) {
                                       elapsed_time <- as.numeric(Sys.time()) - as.numeric(self$last_time)

                                       print_message = TRUE
                                       is_error <- inherits(result, "expectation_failure") ||
                                         inherits(result, "expectation_error")

                                       if (is_error) {
                                         self$n_fail <- self$n_fail + 1
                                       } else if (inherits(result, "expectation_skip")) {
                                         self$n_skip <- self$n_skip + 1
                                       } else if (inherits(result, "expectation_warning")) {
                                         self$n_warn <- self$n_warn + 1
                                       } else {
                                         print_message = FALSE
                                         self$n_ok <- self$n_ok + 1
                                       }

                                       if (print_message) {
                                        cat(
                                          paste0(test, ": ", private$expectation_type(result), ": ", result$message),
                                          "\n"
                                        )
                                         if (is_error) {
                                           cat(
                                             paste(
                                               "  callstack:\n    ",
                                               paste0(utils::limitedLabels(result$call), collapse = "\n    "),
                                               "\n"
                                             )
                                           )
                                         }
                                       }

                                       if (identical(self$last_test, test)) {
                                         elapsed_time <- self$last_test_time + elapsed_time
                                         self$results$time[length(self$results$time)] <- elapsed_time
                                         self$last_test_time <- elapsed_time
                                       }
                                       else {
                                         private$print_last_test()

                                         self$results$context[length(self$results$context) + 1] <- self$last_context
                                         self$results$time[length(self$results$time) + 1] <- elapsed_time
                                         self$last_test_time <- elapsed_time
                                       }

                                       self$last_test <- test
                                       self$last_time <- Sys.time()
                                     },

                                     end_reporter = function() {
                                       private$print_last_test()

                                       cat("\n")
                                       data <- data.frame(
                                          context = self$results$context,
                                          time = self$results$time
                                        )

                                       summary <- data %>%
                                         dplyr::group_by(context) %>%
                                         dplyr::summarise(time = sum(time)) %>%
                                         dplyr::mutate(time = format(time, width = "9", digits = "3", scientific = F))

                                       total <- data %>%
                                         dplyr::summarise(time = sum(time)) %>%
                                         dplyr::mutate(time = format(time, digits = "3", scientific = F)) %>%
                                         dplyr::pull()

                                       cat("\n")
                                       cat("--- Performance Summary  ----\n\n")
                                       print(as.data.frame(summary), row.names = FALSE)

                                       cat(paste0("\nTotal: ", total, "s\n"))

                                       cat("\n")
                                       cat("------- Tests Summary -------\n\n")
                                       self$cat_line("OK:       ", format(self$n_ok, width = 5))
                                       self$cat_line("Failed:   ", format(self$n_fail, width = 5))
                                       self$cat_line("Warnings: ", format(self$n_warn, width = 5))
                                       self$cat_line("Skipped:  ", format(self$n_skip, width = 5))
                                       cat("\n")
                                     }
                                   ),
                                   private = list(
                                     print_last_test = function() {
                                       if (!is.na(self$last_test) &&
                                           length(self$last_test) > 0 &&
                                           length(self$last_test_time) > 0) {
                                         cat(paste0(self$last_test, ": ", self$last_test_time, "\n"))
                                       }

                                       self$last_test <- NA_character_
                                     },
                                     expectation_type = function(exp) {
                                       stopifnot(is.expectation(exp))
                                       gsub("^expectation_", "", class(exp)[[1]])
                                     }
                                   )
)

# create sym-links for test-related dependencies
populate_test_deps_dir_symlinks <- function(dest) {
  test_deps_dir <- "test_deps"
  test_deps <- list.files(test_deps_dir, all.files = TRUE, no.. = TRUE)
  for (dep in test_deps) {
    file.symlink(
      from = normalizePath(file.path(test_deps_dir, dep)),
      to = dest
    )
  }
}

# run the specified list of test cases
run_tests <- function(test_cases_dir, test_cases, log_file = NULL) {
  if (!identical(log_file, NULL)) {
    fd <- file(log_file, open = "w")
    sink(fd, type = c("output", "message"))
    on.exit(close(fd))
  }
  test_dir <- file.path(tempdir(), uuid::UUIDgenerate())
  testthat_dir <- file.path(test_dir, "testthat")
  dir.create(testthat_dir, showWarnings = FALSE, recursive = TRUE)
  populate_test_deps_dir_symlinks(dest = testthat_dir)
  for (test_case in test_cases) {
    file.symlink(
      from = normalizePath(file.path(test_cases_dir, test_case)),
      to = testthat_dir
    )
  }
  setwd(test_dir)
  test_check("sparklyr", filter = test_filter, reporter = "performance")
}

testthat_latest_spark <- function() {
  if (!exists(".testthat_latest_spark", envir = .GlobalEnv))
    assign(".testthat_latest_spark", "2.3.0", envir = .GlobalEnv)
  get(".testthat_latest_spark", envir = .GlobalEnv)
}

testthat_shell_connection <- function(is_worker = FALSE) {
  method <- ifelse(is_worker, "gateway", "shell")
  connection_cache_key <- paste0(".testthat_", method, "_connection")
  version <- Sys.getenv("SPARK_VERSION", unset = testthat_latest_spark())
  master <- ifelse(
    is_worker,
    paste0("sparklyr://localhost:8880/",
           get(".testthat_shell_connection", envir = .GlobalEnv)$sessionId
    ),
    "master"
  )

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
    version <- spark_installed[1,]$spark
  }

  if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
    options(sparkinstall.verbose = TRUE)
    spark_install(version)
  }

  stopifnot(nrow(spark_installed_versions()) > 0)

  # generate connection if none yet exists
  connected <- FALSE
  if (exists(connection_cache_key, envir = .GlobalEnv)) {
    sc <- get(connection_cache_key, envir = .GlobalEnv)
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

    sc <- spark_connect(master = master, method = method, version = version, config = config)
    assign(connection_cache_key, sc, envir = .GlobalEnv)
  }

  # retrieve spark connection
  get(connection_cache_key, envir = .GlobalEnv)
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

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # enforce all configuration settings are described
  options(sparklyr.test.enforce.config = TRUE)

  # TODO:
  test_filter <- c("^barrier$|^serialization$|^invoke$|^copy-to$")

  livy_version <- Sys.getenv("LIVY_VERSION")
  if (nchar(livy_version) > 0) {
    livy_tests <- c(
      "^dplyr$",
      "^dbi$",
      "^copy-to$",
      "^spark-apply$",
      "^ml-clustering-kmeans$"
    )

    test_filter <- paste(livy_tests, collapse = "|")
  }

  arrow_version <- Sys.getenv("ARROW_VERSION")
  is_arrow_devel <- identical(arrow_version, "devel")
  if (is_arrow_devel) {
    arrow_devel_tests <- c(
      "^dplyr$",
      "^dbi$",
      "^copy-to$",
      "^sdf-collect$",
      "^serialization$",
      "^spark-apply.",
      "^ml-clustering-kmeans$"
    )

    test_filter <- paste(arrow_devel_tests, collapse = "|")
  }

  r_arrow <- isTRUE(as.logical(Sys.getenv("ARROW_ENABLED")))
  if (r_arrow) {
    get("library")("arrow")
  }

  on.exit({ spark_disconnect_all() ; livy_service_stop() })

  test_cases_dir <- "test_cases"
  test_cases <- list.files(path = test_cases_dir, pattern = "test-.*\\.R")

  install.packages("uuid")
  library(uuid)
  if (identical(Sys.getenv("RUN_TESTS_IN_PARALLEL"), "true") &&
      livy_version == "" &&
      arrow_version == "" &&
      Sys.getenv("TEST_DATABRICKS_CONNECT") != "true") {
    # run tests in parallel
    num_test_threads <- as.integer(Sys.getenv("NUM_TEST_PROCS", unset = 4))
    gateway_ports <- 8080 + seq(0, num_test_threads - 1) * 10
    install.packages("doParallel")
    library(doParallel)
    doParallel::registerDoParallel(cores = num_test_threads)
    log_file <- function(test_case) {
      file.path(tempdir(), paste("test_", test_case, ".log", sep = ""))
    }
    cwd <- getwd()
    foreach (test_case = test_cases, .export = c(".GlobalEnv")) %dopar% {
      setwd(cwd)
      run_tests(test_cases_dir, c(test_case), log_file = log_file(test_case))
    }
  } else {
    # run test cases serially
    run_tests(test_cases_dir, test_cases)
  }
}
