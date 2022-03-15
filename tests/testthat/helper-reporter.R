sparklyr_reporter <- function() {
  if (using_livy()) {
    MultiReporter$new(
      reporters = list(
        SilentReporter$new(),
        PerformanceReporter$new()
      )
    )
  } else {
    MultiReporter$new(
      reporters = list(
        SummaryReporter$new(),
        PerformanceReporter$new()
      )
    )
  }
}

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
    failures = c(),
    line = function(...) cat(paste0("\n", ...), file = self$out),
    initialize = function(file = getOption("testthat.output_file", stdout())) {
      if (is.character(file)) {
        file <- normalizePath(file, mustWork = FALSE)
      }
      self$out <- file
      if (is.character(self$out) && file.exists(self$out)) {
        # If writing to a file, overwrite it if it exists
        file.remove(self$out)
      }

      # Capture at init so not affected by test settings
      self$width <- cli::console_width()
      self$unicode <- cli::is_utf8_output()
      self$crayon <- crayon::has_color()

      testthat_msg <- Sys.getenv("TESTTHAT_MSG")
      if (testthat_msg != "") self$line(testthat_msg)
    },
    start_context = function(context) {
      self$last_context <- context
      self$last_time <- Sys.time()
    },
    add_result = function(context, test, result) {
      elapsed_time <- as.numeric(Sys.time()) - as.numeric(self$last_time)

      is_error <- inherits(result, "expectation_failure") ||
        inherits(result, "expectation_error")

      if (is_error) {
        self$n_fail <- self$n_fail + 1
        self$failures <- c(self$failures, paste0(test, " (Context: ", context, ")"))
      } else if (inherits(result, "expectation_skip")) {
        self$n_skip <- self$n_skip + 1
      } else if (inherits(result, "expectation_warning")) {
        self$n_warn <- self$n_warn + 1
      } else {
        self$n_ok <- self$n_ok + 1
      }

      if (identical(self$last_test, test)) {
        elapsed_time <- self$last_test_time + elapsed_time
        self$results$time[length(self$results$time)] <- elapsed_time
        self$last_test_time <- elapsed_time
      } else {
        self$results$context[length(self$results$context) + 1] <- self$last_context
        self$results$time[length(self$results$time) + 1] <- elapsed_time
        self$last_test_time <- elapsed_time
      }

      self$last_test <- test
      self$last_time <- Sys.time()
    },
    end_reporter = function() {
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
      if (length(self$failures) > 0) {
        self$cat_line(
          "Failures:  ",
          do.call(paste, as.list(c(self$failures, sep = "\n")))
        )
      }
      cat("\n")
    }
  )
)
