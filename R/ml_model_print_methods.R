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
  
  summary <- sdf_summarize(residuals, "residuals")$residuals
  fns <- as.numeric(summary[c("Minimum", "Q1", "Median", "Q3", "Maximum")])
  names(fns) <- c("Min", "1Q", "Median", "3Q", "Max")
  
  cat("Residuals:", sep = "\n")
  print(fns)
  invisible(fns)
}

ml_model_print_coefficients <- function(model) {
  
  coef <- coefficients(model)
  
  cat("Coefficients:", sep = "\n")
  print(coef)
  invisible(coef)
}