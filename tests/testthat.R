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

                                     start_context = function(context) {
                                       self$last_context <- context
                                       self$last_time <- Sys.time()
                                       cat(paste0("\nContext: ", context, "\n"))
                                     },

                                     add_result = function(context, test, result) {
                                       elapsed_time <- as.numeric(Sys.time() - self$last_time)

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
                                           result = result,
                                           time = elapsed_time
                                         )
                                       }

                                       self$last_test <- test
                                       self$last_time <- Sys.time()
                                     },

                                     end_reporter = function() {
                                       cat("\n")
                                       data <- do.call("rbind", args = self$results)[, c("context", "test", "time")]
                                       print(as.data.frame(data, row.names = F))
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

  test_filter <- "^config$"
  test_check("sparklyr", filter = test_filter, reporter = "performance")
}
