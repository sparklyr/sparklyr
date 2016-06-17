print_newline <- function() {
  cat("", sep = "\n")
}

ml_model_print_call <- function(model) {
  
  formula <- paste(
    paste(model$response, collapse = " + "),
    "~",
    paste(model$features, collapse = " + ")
  )
  
  cat("Call:", sep = "\n")
  cat(formula, sep = "\n")
  invisible(formula)
}

ml_model_print_residuals <- function(model) {
  
  residuals <- model$.model %>%
    spark_invoke("summary") %>%
    spark_invoke("residuals")
  
  # randomly sample residuals and produce quantiles based on
  # sample to avoid slowness in Spark's 'percentile_approx()'
  # implementation
  count <- spark_invoke(residuals, "count")
  limit <- 1E5
  isApproximate <- count > limit
  
  values <- if (isApproximate) {
    fraction <- limit / count
    residuals %>%
      spark_invoke("sample", FALSE, fraction) %>%
      spark_dataframe_read_column("residuals") %>%
      quantile()
  } else {
    residuals %>%
      spark_dataframe_read_column("residuals") %>%
      quantile()
  }
  names(values) <- c("Min", "1Q", "Median", "3Q", "Max")
  
  header <- if (isApproximate)
    "Residuals (approximate):"
  else
    "Residuals:"
  
  cat(header, sep = "\n")
  print(values, digits = max(3L, getOption("digits") - 3L))
  invisible(values)
}

ml_model_print_coefficients <- function(model) {
  
  coef <- coefficients(model)
  
  cat("Coefficients:", sep = "\n")
  print(coef)
  invisible(coef)
}