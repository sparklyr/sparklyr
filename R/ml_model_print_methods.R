print_newline <- function() {
  cat("", sep = "\n")
}

ml_model_print_call <- function(model) {
  
  formula <- paste(
    paste(model$response, collapse = " + "),
    "~",
    paste(model$features, collapse = " + "),
    if (identical(model$intercept, FALSE)) "- 1"
  )
  
  cat("Call:", sep = "\n")
  cat(formula, sep = "\n")
  invisible(formula)
}

ml_model_print_residuals <- function(model,
                                     residuals.header = "Residuals") {
  
  residuals <- model$.model %>%
    invoke("summary") %>%
    invoke("residuals")
  
  # randomly sample residuals and produce quantiles based on
  # sample to avoid slowness in Spark's 'percentile_approx()'
  # implementation
  count <- invoke(residuals, "count")
  limit <- 1E5
  isApproximate <- count > limit
  column <- invoke(residuals, "columns")[[1]]
  
  values <- if (isApproximate) {
    fraction <- limit / count
    residuals %>%
      invoke("sample", FALSE, fraction) %>%
      sdf_read_column(column) %>%
      quantile()
  } else {
    residuals %>%
      sdf_read_column(column) %>%
      quantile()
  }
  names(values) <- c("Min", "1Q", "Median", "3Q", "Max")
  
  header <- if (isApproximate)
    paste(residuals.header, "(approximate):")
  else
    paste(residuals.header, ":", sep = "") 
  
  cat(header, sep = "\n")
  print(values, digits = max(3L, getOption("digits") - 3L))
  invisible(values)
}

#' @importFrom stats coefficients quantile
ml_model_print_coefficients <- function(model) {
  
  coef <- coefficients(model)
  
  cat("Coefficients:", sep = "\n")
  print(coef)
  invisible(coef)
}

ml_model_print_centers <- function(model) {
  
  centers <- model$centers
  if (is.null(centers))
    return()
  
  cat("Cluster centers:", sep = "\n")
  print(model$centers)
  
}
