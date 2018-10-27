Sys.setenv("R_TESTS" = "")
library(testthat)
library(sparklyr)

PerformanceReporter <- R6::R6Class("PerformanceReporter",
                                   inherit = Reporter,
                                   public = list(
                                     results = list(),
                                     last_context = NA_character_,
                                     last_test = NA_character_,
                                     last_time = Sys.time(),
                                     n_ok = 0,
                                     n_skip = 0,
                                     n_warn = 0,
                                     n_fail = 0,

                                     start_context = function(context) {
                                       self$last_context <- context
                                       self$last_time <- Sys.time()
                                       cat(paste0("\nContext: ", context, "\n"))
                                     },

                                     add_result = function(context, test, result) {
                                       elapsed_time <- as.numeric(Sys.time() - self$last_time)

                                       if (inherits(result, "expectation_failure")) {
                                         self$n_fail <- self$n_fail + 1
                                       } else if (inherits(result, "expectation_skip")) {
                                         self$n_skip <- self$n_skip + 1
                                       } else if (inherits(result, "expectation_warning")) {
                                         self$n_warn <- self$n_warn + 1
                                       } else {
                                         self$n_ok <- self$n_ok + 1
                                       }

                                       if (inherits(result, "expectation_failure")) {
                                         cat("Failure:", result$message, "\n")
                                       }

                                       if (identical(self$last_test, test)) {
                                         total_time <- self$results[[length(self$results)]]$time + elapsed_time
                                         self$results[[length(self$results)]]$time <- total_time
                                       }
                                       else {
                                         if (length(self$results) >= 1) {
                                           previous_result <- self$results[[length(self$results)]]
                                           cat(paste0(previous_result$test, ": ", previous_result$time, "\n"))
                                         }

                                         self$results[[length(self$results) + 1]] <- list(
                                           context = self$last_context,
                                           test = test,
                                           success = inherits(result, "expectation_success"),
                                           time = elapsed_time
                                         )
                                       }

                                       self$last_test <- test
                                       self$last_time <- Sys.time()
                                     },

                                     end_reporter = function() {
                                       cat("\n")
                                       data <- dplyr::bind_rows(self$results)

                                       summary <- dplyr::bind_rows(self$results) %>%
                                         dplyr::group_by(context) %>%
                                         dplyr::summarise(time = sum(time)) %>%
                                         dplyr::mutate(time = format(time, width = "13"))

                                       cat("\n")
                                       cat("--- Performance Summary  ----\n\n")
                                       print(as.data.frame(summary), row.names = FALSE)

                                       cat("\n")
                                       cat("------- Tests Summary -------\n\n")
                                       self$cat_line("OK:       ", format(self$n_ok, width = 5))
                                       self$cat_line("Failed:   ", format(self$n_fail, width = 5))
                                       self$cat_line("Warnings: ", format(self$n_warn, width = 5))
                                       self$cat_line("Skipped:  ", format(self$n_skip, width = 5))
                                       cat("\n")
                                     }
                                   )
)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  # enforce all configuration settings are described
  options(sparklyr.test.enforce.config = TRUE)

  test_filter <- NULL

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

  r_arrow <- isTRUE(as.logical(Sys.getenv("R_ARROW")))
  if (r_arrow) {
    get("library")("arrow")
  }

  on.exit({ spark_disconnect_all() ; livy_service_stop() })

  test_check("sparklyr", filter = test_filter, reporter = "performance")
}
