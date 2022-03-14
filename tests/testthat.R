#Sys.setenv("R_TESTS" = "")

if (identical(Sys.getenv("DBPLYR_API_EDITION"), "1")) {
  options(sparklyr.dbplyr.edition = 1L)
}

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # enforce all configuration settings are described
  options(sparklyr.test.enforce.config = TRUE)

  suppressPackageStartupMessages(library(sparklyr))
  suppressPackageStartupMessages(library(dplyr))
  suppressPackageStartupMessages(library(testthat))

  #Sys.setenv("TESTTHAT_FILTER" = "^dummy$")

  testthat_filter <- Sys.getenv("TESTTHAT_FILTER")
  arrow_enabled <- isTRUE(as.logical(Sys.getenv("ARROW_ENABLED")))
  is_arrow_devel <- identical(Sys.getenv("ARROW_VERSION"), "devel")
  livy_version <- Sys.getenv("LIVY_VERSION")
  is_livy <- nchar(livy_version) > 0 && !identical(livy_version, "NONE")

  if(is_arrow_devel && !arrow_enabled)
    stop("Arrow Devel requested, but tests not enabled, change ARROW_ENABLED to 'true'")

  if(arrow_enabled && is_livy)
    stop("Arrow and Livy are separate test sets. Disable one of them.")

  sm <- NULL

  if(is_livy) {
    sm <- c(sm, paste0("Livy tests enabled, version ", livy_version))
  } else {
    sm <- c(sm, paste0("Spark shell tests only (no Livy)"))
  }

  if(arrow_enabled) {
    if(is_arrow_devel) {
      sm <- c(sm, "Arrow enabled tests with devel version")
    } else {
      sm <- c(sm, "Arrow enabled tests")
    }
    suppressPackageStartupMessages(library(arrow))
  } else {
    if(!is_livy) sm <- c(sm, "Arrow disabled")
  }

  if(testthat_filter != "") {
    sm <- c(sm, paste0("Test filter from environment variable: ", testthat_filter))
  }

  single_sm <- paste0(paste(">> ", sm, collapse = "\n"), "\n\n")
  Sys.setenv("TESTTHAT_MSG" = single_sm)

  if(testthat_filter != "") {
    test_filters <- list(testthat_filter)
  } else if (is_livy) {
    test_filters <- list(
      paste(
        c(
          "^spark-apply$",
          "^spark-apply-bundle$",
          "^spark-apply-ext$",
          "^dbi$",
          "^ml-clustering-kmeans$",
          "^livy-config$",
          "^livy-proxy$",
          "^dplyr$",
          "^dplyr-join$",
          "^dplyr-stats$",
          "^dplyr-sample.*$",
          "^dplyr-weighted-mean$"
        ),
        collapse = "|"
      )
    )
  } else if (is_arrow_devel) {
    test_filters <- list(
      paste(
        c(
          "^binds$",
          "^connect-shell$",
          "^dplyr.*",
          "^dbi$",
          "^copy-to$",
          "^read-write$",
          "^sdf-collect$",
          "^serialization$",
          "^spark-apply.*",
          "^ml-clustering.*kmeans$"
        ),
        collapse = "|"
      )
    )
  } else {
    test_filters <- NULL
  }

  run_tests <- function(test_filter) {
    on.exit({
      spark_disconnect_all(terminate = TRUE)
      tryCatch(livy_service_stop(), error = function(e) {})
      # Sys.sleep(30)
      # remove(".testthat_spark_connection", envir = .GlobalEnv)
      # remove(".testthat_livy_connection", envir = .GlobalEnv)
    })

    if(is_livy) {
      new_reporter <- MultiReporter$new(
        reporters = list(
          SilentReporter$new(),
          PerformanceReporter$new()
        )
      )
    } else {
      new_reporter <- MultiReporter$new(
        reporters = list(
          SummaryReporter$new(),
          PerformanceReporter$new()
        )
      )
    }

    if(is.null(test_filters)) {
      #test_check("sparklyr", reporter = SummaryReporter, perl = TRUE)
    } else {
      test_check("sparklyr", filter = test_filter, perl = TRUE)
    }
  }
  run_tests(test_filters)
}

testthat_test_livy <- function(livy_version = "0.5.0", spark_version = "2.2.0", filter = "^dbi$") {
  Sys.setenv("SPARK_VERSION" = spark_version)
  Sys.setenv("NOT_CRAN" = 'true')
  Sys.setenv("LIVY_VERSION" = livy_version)
  Sys.setenv("ARROW_ENABLED" = 'false')
  Sys.setenv("TESTTHAT_FILTER" = filter)
  source("testthat.R")
}
